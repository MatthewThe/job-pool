import pandas as pd
import pyarrow.feather as feather

from job_pool import JobPool


def store_dataframe(df, variable_name):
    """Stores a Pandas DataFrame in shared memory using Feather format."""
    feather.write_feather(df, f"/dev/shm/{variable_name}.feather", compression=None)


def worker(variable_name):
    """Worker process retrieves the DataFrame from shared memory."""
    df = feather.read_feather(
        f"/dev/shm/{variable_name}.feather", memory_map=True
    )  # Zero-copy access
    print(f"[Worker] Received DataFrame:\n{df}")


if __name__ == "__main__":
    # Create a sample DataFrame
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10.5, 20.3, 30.1],
            "name": ["Alice", "Bob", "Charlie"],  # String column
        }
    )

    # Store in shared memory
    variable_name = "my_df"
    store_dataframe(df, variable_name)

    # Start a pool
    pool = JobPool(processes=2)
    for i in range(10):
        pool.applyAsync(worker, (variable_name,))

    pool.checkPool()
