# Benchmark Results

## Overview
This document summarizes the benchmark results for the multi-query processing experiments run on both the `10k.csv` and `100k.csv` datasets.

## Dataset: 10k.csv

### Experiment 1: Scheduling Strategies (No Cache)
- FIFO - Average Latency: `0.0006` seconds per query
- FIFO - Average Throughput: `226,195.77` queries per second
- Priority - Average Latency: `0.0006` seconds per query
- Priority - Average Throughput: `184,934.15` queries per second

### Experiment 2: Impact of Caching
- No Cache - Average Latency: `0.0006` seconds per query
- No Cache - Average Throughput: `228,899.25` queries per second
- With Cache - Average Latency: `0.0001` seconds per query
- With Cache - Average Throughput: `671,272.24` queries per second

### Experiment 3: Single-node vs Distributed Processing
- Single-node - Average Latency: `0.0006` seconds per query
- Single-node - Average Throughput: `227,046.53` queries per second
- Distributed (Spark) - Average Latency: `0.0119` seconds per query
- Distributed (Spark) - Average Throughput: `209,614.32` queries per second

---

## Dataset: 100k.csv

### Experiment 1: Scheduling Strategies (No Cache)
- FIFO - Average Latency: `0.0066` seconds per query
- FIFO - Average Throughput: `212,887.57` queries per second
- Priority - Average Latency: `0.0061` seconds per query
- Priority - Average Throughput: `168,608.16` queries per second

### Experiment 2: Impact of Caching
- No Cache - Average Latency: `0.0062` seconds per query
- No Cache - Average Throughput: `211,637.98` queries per second
- With Cache - Average Latency: `0.0007` seconds per query
- With Cache - Average Throughput: `213,317.29` queries per second

### Experiment 3: Single-node vs Distributed Processing
- Single-node - Average Latency: `0.0064` seconds per query
- Single-node - Average Throughput: `214,146.44` queries per second
- Distributed (Spark) - Average Latency: `0.0620` seconds per query
- Distributed (Spark) - Average Throughput: `190,662.36` queries per second

---

## Notes
- Caching dramatically reduces latency, especially in single-node execution, and substantially increases throughput when query repetition is present.
- On both datasets, the distributed Spark path remains slower in latency than single-node processing for this benchmark, though throughput remains comparable.
- The benchmark uses CSV tweet/news data and measures end-to-end query processing including query scheduling, embedding, and similarity search.
