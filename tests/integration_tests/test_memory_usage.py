import psutil
import os
import time
import gc
import numpy as np
import pandas as pd
from job_pool import JobPool

"""
Checks that memory is released as soon as the job is consumed.
"""


def get_memory_usage():
    """Return memory usage in MB"""
    process = psutil.Process(os.getpid())  # Get current process
    return process.memory_info().rss / (1024 * 1024)  # Convert to MB


def worker(data):
    """Worker function that processes data"""
    time.sleep(10)  # Simulate work
    return len(data["data"])  # Return size of processed data


if __name__ == "__main__":
    print(f"🔹 Initial memory usage: {get_memory_usage():.2f} MB")
    large_data = []
    for i in range(10):
        large_data.append(
            {
                "data": pd.DataFrame(
                    np.random.randn(10000, 100),  # Random floats (normal distribution)
                    columns=[f"col_{i}" for i in range(100)],
                )
            }
        )  # 10 MB string

    print(f"🔹 Memory before apply_async: {get_memory_usage():.2f} MB")

    # Create a pool with 2 workers
    pool = JobPool(processes=2)
    results = []
    for i in range(10):
        # Large object to test memory impact
        large_data2 = {
            "data": large_data[i]["data"].iloc[:-i, :],
            "data2": large_data[i]["data"].iloc[i:, :],
        }
        pool.applyAsync(worker, (large_data2,))  # Send large object

        print(f"📝 Called apply_async. Memory now: {get_memory_usage():.2f} MB")

        time.sleep(5)

    print(f"✅ Task in progress. Memory now: {get_memory_usage():.2f} MB")

    # Wait for the task to complete
    pool.checkPool(printProgressEvery=1)

    print(f"✅ Task completed. Memory now: {get_memory_usage():.2f} MB")

    # Pool exits here, workers are terminated

    print(f"🔹 Memory after closing Pool: {get_memory_usage():.2f} MB")

    # Run garbage collection
    gc.collect()
    print(f"🧹 Memory after GC: {get_memory_usage():.2f} MB")
