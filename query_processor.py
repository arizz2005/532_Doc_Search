import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, Normalizer, BucketedRandomProjectionLSH
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, expr, row_number
from pyspark.sql.window import Window
from collections import OrderedDict

class SchedulingStrategy(Enum):
    FIFO = "fifo"
    PRIORITY = "priority"

class LRUCache:
    def __init__(self, capacity=100):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

class QueryProcessor:
    def __init__(self, documents, scheduling_strategy=SchedulingStrategy.FIFO, max_workers=4, use_spark=False, use_cache=False, cache_size=100):
        self.documents = documents
        self.use_spark = use_spark
        if use_spark:
            self.spark = (
                SparkSession.builder.master("local[*]")
                .appName("DocumentSearch")
                .config("spark.driver.memory", "8g")
                .config("spark.executor.memory", "8g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate()
            )
            self.tokenizer = Tokenizer(inputCol="text", outputCol="words")
            self.hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
            self.doc_embeddings, self.idfModel = self._compute_embeddings_spark()
        else:
            self.vectorizer = TfidfVectorizer()
            self.doc_embeddings = self.vectorizer.fit_transform(documents)
        self.scheduling_strategy = scheduling_strategy
        self.query_queue = queue.Queue() if scheduling_strategy == SchedulingStrategy.FIFO else queue.PriorityQueue()
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.results = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self._process_queries)
        self.worker_thread.daemon = True
        self.worker_thread.start()
        self.use_cache = use_cache
        self.cache = LRUCache(cache_size) if use_cache else None

    def _compute_embeddings_spark(self):
        df = self.spark.createDataFrame([(i, doc) for i, doc in enumerate(self.documents)], ["id", "text"])
        wordsData = self.tokenizer.transform(df)
        featurizedData = self.hashingTF.transform(wordsData)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
        normalizer = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)
        normalizedDocs = normalizer.transform(rescaledData).select("id", "features", "norm_features")
        self.lshModel = BucketedRandomProjectionLSH(
            inputCol="norm_features",
            outputCol="hashes",
            numHashTables=4,
            bucketLength=1.0
        ).fit(normalizedDocs)
        doc_embeddings = self.lshModel.transform(normalizedDocs).select("id", "features", "norm_features")
        doc_embeddings = doc_embeddings.persist(StorageLevel.MEMORY_AND_DISK)
        return doc_embeddings, idfModel

    def _process_queries(self):
        while not self.stop_event.is_set() or not self.query_queue.empty():
            try:
                item = self.query_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if self.scheduling_strategy == SchedulingStrategy.FIFO:
                query_items = [(item[0], item[1])]
            else:
                # PriorityQueue stores tuples as (priority, query_id, query)
                query_items = [(item[1], item[2])]

            if self.use_spark:
                while True:
                    try:
                        next_item = self.query_queue.get_nowait()
                    except queue.Empty:
                        break
                    if self.scheduling_strategy == SchedulingStrategy.FIFO:
                        query_items.append((next_item[0], next_item[1]))
                    else:
                        query_items.append((next_item[1], next_item[2]))

                batch_results = self._search_spark_batch(query_items)
                for query_id, result in batch_results.items():
                    with self.lock:
                        self.results[query_id] = result
                for _ in query_items:
                    self.query_queue.task_done()
            else:
                query_id, query = query_items[0]
                result = self._search(query)
                with self.lock:
                    self.results[query_id] = result
                self.query_queue.task_done()

    def _search(self, query):
        if self.use_cache:
            cached_result = self.cache.get(query)
            if cached_result is not None:
                return cached_result

        query_vec = self.vectorizer.transform([query])
        similarities = cosine_similarity(query_vec, self.doc_embeddings)[0]
        top_indices = np.argsort(similarities)[-5:][::-1]  # Top 5
        result = [(self.documents[i], similarities[i]) for i in top_indices]

        if self.use_cache:
            self.cache.put(query, result)

        return result

    def _search_spark_batch(self, query_items):
        if not query_items:
            return {}

        batch_results = {}
        queries_to_process = []
        if self.use_cache:
            for query_id, query in query_items:
                cached_result = self.cache.get(query)
                if cached_result is not None:
                    batch_results[query_id] = cached_result
                else:
                    queries_to_process.append((query_id, query))
        else:
            queries_to_process = query_items

        if not queries_to_process:
            return batch_results

        query_rows = [(query_id, query) for query_id, query in queries_to_process]
        query_df = self.spark.createDataFrame(query_rows, ["query_id", "text"])
        wordsQuery = self.tokenizer.transform(query_df)
        featurizedQuery = self.hashingTF.transform(wordsQuery)
        rescaledQuery = self.idfModel.transform(featurizedQuery)
        normalizer = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)
        query_vectors = normalizer.transform(rescaledQuery).select("query_id", "features", "norm_features")

        similarity_join = self.lshModel.approxSimilarityJoin(
            query_vectors,
            self.doc_embeddings,
            2.0,
            distCol="dist"
        )

        top_candidates = similarity_join.select(
            col("datasetA.query_id").alias("query_id"),
            col("datasetB.id").alias("doc_id"),
            col("dist")
        ).withColumn(
            "similarity",
            expr("1 - dist * dist / 2")
        )

        window_spec = Window.partitionBy("query_id").orderBy(col("similarity").desc())
        top_df = top_candidates.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 5)

        top_results = top_df.select("query_id", "doc_id", "similarity").collect()
        for row in top_results:
            batch_results.setdefault(row["query_id"], []).append((self.documents[row["doc_id"]], row["similarity"]))

        if self.use_cache:
            for query_id, query in queries_to_process:
                if query_id in batch_results:
                    self.cache.put(query, batch_results[query_id])

        return batch_results

    def _search_spark(self, query):
        return self._search_spark_batch([(None, query)]).get(None, [])

    def submit_query(self, query_id, query, priority=0):
        if self.scheduling_strategy == SchedulingStrategy.FIFO:
            self.query_queue.put((query_id, query))
        else:
            self.query_queue.put((priority, query_id, query))

    def get_result(self, query_id):
        with self.lock:
            return self.results.get(query_id, None)

    def shutdown(self):
        self.stop_event.set()
        self.query_queue.join()
        self.worker_thread.join(timeout=5)
        self.executor.shutdown(wait=True)
        if self.use_spark and self.spark is not None:
            self.spark.stop()

def handle_multiple_queries(processor, queries, priorities=None):
    query_ids = []
    for i, query in enumerate(queries):
        priority = priorities[i] if priorities is not None else 0
        processor.submit_query(i, query, priority)
        query_ids.append(i)
    return query_ids

def benchmark_queries(processor, queries, priorities=None, num_runs=5):
    latencies = []
    throughputs = []
    for _ in range(num_runs):
        start_time = time.time()
        query_ids = handle_multiple_queries(processor, queries, priorities)
        while any(processor.get_result(qid) is None for qid in query_ids):
            time.sleep(0.01)
        end_time = time.time()
        total_time = end_time - start_time
        latencies.append(total_time / len(queries))
        throughputs.append(len(queries) / total_time)
    return {
        'avg_latency': np.mean(latencies),
        'avg_throughput': np.mean(throughputs)
    }