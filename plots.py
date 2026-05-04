import matplotlib.pyplot as plt
import numpy as np

datasets = ['10k', '100k', '1M']
x = np.arange(len(datasets))  
width = 0.35                  

fifo_latency = [0.0006, 0.0066, 0.1154]
priority_latency = [0.0006, 0.0061, 0.1150]
fifo_throughput = [226195.77, 212887.57, 135752.77]
priority_throughput = [184934.15, 168608.16, 134936.95]

nocache_latency = [0.0006, 0.0062, 0.1150]
cache_latency = [0.0001, 0.0007, 0.0116]
nocache_throughput = [228899.25, 211637.98, 152828.60]
cache_throughput = [671272.24, 213317.29, 207890.88]

single_latency = [0.0006, 0.0064, 0.1162]
distributed_latency = [0.0119, 0.0620, 0.0175]
single_throughput = [227046.53, 214146.44, 139584.56]
distributed_throughput = [209614.32, 190662.36, 211058.67]

def create_plot(title, labels, data1, data2, ylabel, filename, log_scale=False):
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.bar(x - width/2, data1, width, label=labels[0], color='skyblue')
    ax.bar(x + width/2, data2, width, label=labels[1], color='orange')

    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(datasets)
    ax.legend()
    
    if log_scale:
        ax.set_yscale('log')
    
    fig.tight_layout()
    plt.savefig(filename)
    plt.show()

create_plot('Exp 1: Scheduling - Latency', ['FIFO', 'Priority'], 
            fifo_latency, priority_latency, 'Latency (seconds)', 'exp1_latency.png', log_scale=True)
create_plot('Exp 1: Scheduling - Throughput', ['FIFO', 'Priority'], 
            fifo_throughput, priority_throughput, 'Throughput (queries/s)', 'exp1_throughput.png')

create_plot('Exp 2: Caching - Latency', ['No Cache', 'With Cache'], 
            nocache_latency, cache_latency, 'Latency (seconds)', 'exp2_latency.png', log_scale=True)
create_plot('Exp 2: Caching - Throughput', ['No Cache', 'With Cache'], 
            nocache_throughput, cache_throughput, 'Throughput (queries/s)', 'exp2_throughput.png')

create_plot('Exp 3: Single-node vs Distributed - Latency', ['Single-node', 'Distributed'], 
            single_latency, distributed_latency, 'Latency (seconds)', 'exp3_latency.png', log_scale=True)
create_plot('Exp 3: Single-node vs Distributed - Throughput', ['Single-node', 'Distributed'], 
            single_throughput, distributed_throughput, 'Throughput (queries/s)', 'exp3_throughput.png')