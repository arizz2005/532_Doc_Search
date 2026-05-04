import time
import numpy as np
import pandas as pd
from query_processor import QueryProcessor, SchedulingStrategy, benchmark_queries, handle_multiple_queries

df = pd.read_csv("1M.csv")

documents = df["text"].to_list()

"""
# test data/doc TODO: need to pull from twitter db
documents = [
    "Breaking news: Major earthquake hits California",
    "Weather update: Heavy rain expected tomorrow",
    "Sports: Team wins championship game",
    "Technology: New AI breakthrough announced",
    "Politics: Election results declared",4
    "Economy: Stock market reaches new high",
    "Health: New vaccine approved",
    "Entertainment: Movie breaks box office records",
    "Science: Mars mission successful",
    "Travel: New airline routes announced"
] * 100  # testing
"""

# Sample queries tuned for the tweet/news dataset
queries = [
    "Croatia Schengen euro",
    "UK enters 2023 live",
    "predatory rent inflation housing",
    "NewYearsDay London Dublin Lisbon",
    "pop up camper ATV UTV",
    "Cristiano Ronaldo Happy New Year",
    "Emory University researcher fraud",
    "stock market crash recession",
    "global crash news",
    "industry news latest PR"
] * 10  # Multiple queries

def run_experiments():
    print("Running experiments for multi-query processing...")

    # Experiment 1: FIFO vs Priority Scheduling (without cache)
    print("\n=== Experiment 1: Scheduling Strategies (No Cache) ===")
    
    # FIFO
    processor_fifo = QueryProcessor(documents, SchedulingStrategy.FIFO, max_workers=4, use_spark=False, use_cache=False)
    results_fifo = benchmark_queries(processor_fifo, queries)
    print(f"FIFO - Average Latency: {results_fifo['avg_latency']:.4f} seconds per query")
    print(f"FIFO - Average Throughput: {results_fifo['avg_throughput']:.2f} queries per second")
    processor_fifo.shutdown()

    # Priority (random priorities)
    priorities = np.random.randint(1, 10, len(queries))
    processor_priority = QueryProcessor(documents, SchedulingStrategy.PRIORITY, max_workers=4, use_spark=False, use_cache=False)
    results_priority = benchmark_queries(processor_priority, queries, priorities)
    print(f"Priority - Average Latency: {results_priority['avg_latency']:.4f} seconds per query")
    print(f"Priority - Average Throughput: {results_priority['avg_throughput']:.2f} queries per second")
    processor_priority.shutdown()

    # Experiment 2: With and Without Caching
    print("\n=== Experiment 2: Impact of Caching ===")
    
    # Without cache
    processor_no_cache = QueryProcessor(documents, SchedulingStrategy.FIFO, max_workers=4, use_spark=False, use_cache=False)
    results_no_cache = benchmark_queries(processor_no_cache, queries)
    print(f"No Cache - Average Latency: {results_no_cache['avg_latency']:.4f} seconds per query")
    print(f"No Cache - Average Throughput: {results_no_cache['avg_throughput']:.2f} queries per second")
    processor_no_cache.shutdown()

    # With cache
    processor_cache = QueryProcessor(documents, SchedulingStrategy.FIFO, max_workers=4, use_spark=False, use_cache=True, cache_size=50)
    results_cache = benchmark_queries(processor_cache, queries)
    print(f"With Cache - Average Latency: {results_cache['avg_latency']:.4f} seconds per query")
    print(f"With Cache - Average Throughput: {results_cache['avg_throughput']:.2f} queries per second")
    processor_cache.shutdown()

    # Experiment 3: Single-node vs Distributed (Spark)
    print("\n=== Experiment 3: Single-node vs Distributed Processing ===")
    
    # Single-node
    processor_single = QueryProcessor(documents, SchedulingStrategy.FIFO, max_workers=4, use_spark=False, use_cache=False)
    results_single = benchmark_queries(processor_single, queries)
    print(f"Single-node - Average Latency: {results_single['avg_latency']:.4f} seconds per query")
    print(f"Single-node - Average Throughput: {results_single['avg_throughput']:.2f} queries per second")
    processor_single.shutdown()

    # Distributed (Spark)
    processor_spark = QueryProcessor(documents, SchedulingStrategy.FIFO, max_workers=4, use_spark=True, use_cache=False)
    results_spark = benchmark_queries(processor_spark, queries)
    print(f"Distributed (Spark) - Average Latency: {results_spark['avg_latency']:.4f} seconds per query")
    print(f"Distributed (Spark) - Average Throughput: {results_spark['avg_throughput']:.2f} queries per second")
    processor_spark.shutdown()

    print("\nExperiments completed.")

if __name__ == "__main__":
    run_experiments()