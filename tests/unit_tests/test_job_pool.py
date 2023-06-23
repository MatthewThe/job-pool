from job_pool.job_pool import JobPool


def add_one(i):
    return i + 1


def test_add_one():
    pool = JobPool(4)
    for i in range(20):
        pool.applyAsync(add_one, [i])
    results = pool.checkPool()
    assert results == list(range(1,21))