# 532_Doc_Search

Scalable Distributed Document Search System with Caching and Parallel Query Processing

## Overview

This project implements a scalable distributed document search system using PySpark for distributed data processing, vector embeddings for semantic search, and systems-level optimizations such as concurrency, caching, and scheduling.

## Components

- `query_processor.py`: Implements the multi-query processing system with threading, scheduling strategies (FIFO and priority-based), caching (LRU), and support for both single-node and distributed (PySpark) execution.

- `benchmark.py`: Runs experiments to evaluate system performance under different configurations, measuring latency and throughput.

## Installation

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```


## Usage

### Running Benchmarks

Execute the benchmark script to run experiments:

```bash
python benchmark.py
```

This will run three experiments:
1. **Scheduling Strategies**: Compare FIFO vs Priority-based scheduling.
2. **Caching Impact**: Compare performance with and without LRU caching.
3. **Distributed Processing**: Compare single-node vs PySpark distributed execution.

