from job_pool import JobPool, AbnormalPoolTerminationError
import sys
import time
import pytest


def add_one(i):
    return i + 1


def test_add_one():
    """Tests that the results are in the correct order"""
    pool = JobPool(4)
    for i in range(20):
        pool.applyAsync(add_one, [i])
    results = pool.checkPool()
    assert results == list(range(1, 21))


def exit_if_one(value):
    if value:
        sys.exit(123)
    return value


def test_exited_process():
    pool = JobPool(4, timeout=10)
    for value in [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]:
        pool.applyAsync(exit_if_one, [value])

    with pytest.raises(AbnormalPoolTerminationError):
        _ = pool.checkPool()


def test_no_exited_process():
    pool = JobPool(4, timeout=10)
    for value in [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]:
        pool.applyAsync(exit_if_one, [value])

    results = pool.checkPool()

    assert results == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def sleep_and_return(value):
    time.sleep(value)
    return value


def test_timeout():
    """Tests that TimeoutError is triggered if one of the jobs exceeds timeout"""
    pool = JobPool(4, timeout=2)
    for value in [0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0]:
        pool.applyAsync(sleep_and_return, [value])

    with pytest.raises(AbnormalPoolTerminationError):
        _ = pool.checkPool()


def test_no_timeout():
    """Tests that each job finishes within timeout, but total time is allowed to exceed timeout"""
    pool = JobPool(4, timeout=2)
    for value in [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]:
        pool.applyAsync(sleep_and_return, [value])

    results = pool.checkPool()

    assert results == [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]


def test_maxtasksperchild():
    """Tests that each job finishes within timeout, but total time is allowed to exceed timeout"""
    pool = JobPool(4, maxtasksperchild=2)
    for value in [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]:
        pool.applyAsync(add_one, [value])

    results = pool.checkPool()

    assert results == [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]


def test_maxtasksperchild_with_exited_process():
    """Tests that each job finishes within timeout, but total time is allowed to exceed timeout"""
    pool = JobPool(4, maxtasksperchild=2)
    for value in [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]:
        pool.applyAsync(exit_if_one, [value])

    with pytest.raises(AbnormalPoolTerminationError):
        _ = pool.checkPool()


if __name__ == "__main__":
    test_maxtasksperchild_with_exited_process()
