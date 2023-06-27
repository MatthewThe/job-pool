#!/usr/bin/python

from job_pool import JobPool


def allocate_memory(mega_bytes):
    print(f"Allocating {mega_bytes} megabytes")
    size = mega_bytes * 1024 * 1024
    data = bytearray(size)
    print(f"Allocated {mega_bytes} megabytes")
    return data


if __name__ == "__main__":
    pool = JobPool(4, timeout=1000)
    for value in [10, 20, 30, 40, 50, 60, 100]:
        pool.applyAsync(allocate_memory, [value])

    # with pytest.raises(SystemExit) as pytest_wrapped_e:
    _ = pool.checkPool()
