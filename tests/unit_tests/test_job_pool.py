import sys
import time
import pytest

from job_pool import JobPool, AbnormalPoolTerminationError


def add_one(i):
    return i + 1


def exit_if_one(value):
    if value:
        sys.exit(123)
    return value


def test_add_one():
    """Tests that the results are in the correct order"""
    pool = JobPool(4)
    for i in range(20):
        pool.applyAsync(add_one, [i])
    results = pool.checkPool()
    assert results == list(range(1, 21))


def test_add_one_with_callback():
    """Tests that a callback can be passed to applyAsync"""
    a = 0

    def my_callback(x):
        nonlocal a
        a += x

    pool = JobPool(4)
    for i in range(20):
        pool.applyAsync(add_one, [i], callback=my_callback)
    results = pool.checkPool()
    assert results == list(range(1, 21))
    assert a == 210


def test_exited_process():
    pool = JobPool(4, timeout=10)
    for value in [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]:
        pool.applyAsync(exit_if_one, [value])

    with pytest.raises(AbnormalPoolTerminationError):
        _ = pool.checkPool()


def test_custom_error_callback():
    def my_error_callback(x):
        print(x)  # doesn't seem to do anything but at least it runs through...
        return x

    pool = JobPool(4, timeout=10)
    for value in [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]:
        pool.applyAsync(exit_if_one, [value], error_callback=my_error_callback)

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


def test_write_progress_to_logger(mocker):
    """Tests that each job finishes within timeout, but total time is allowed to exceed timeout"""
    mock_logger = mocker.patch("job_pool.job_pool.logger")
    input_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    pool = JobPool(
        4,
        maxtasksperchild=2,
        write_progress_to_logger=True,
        print_progress_every=1,
        total_jobs=len(input_list),
    )
    for value in input_list:
        time.sleep(0.1)
        pool.applyAsync(add_one, [value])

    results = pool.checkPool()

    # 1. [-1]: final call
    # 2. [0]: first of (args, kwargs) tuple
    # 3. [1]: second argument (first argument is the log level)
    assert mock_logger.log.call_args_list[-1][0][1].startswith("100%")

    assert results == [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]


def add_one_with_pool(x):
    pool = JobPool(4, maxtasksperchild=2)
    for value in [1, 1, 1, 1, 1, 1, 1, 1]:
        pool.applyAsync(add_one, [x + value])

    results = pool.checkPool()

    return sum(results)


def test_pool_is_nestable():
    """Tests that each job finishes within timeout, but total time is allowed to exceed timeout"""
    pool = JobPool(4, maxtasksperchild=2)
    for value in [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]:
        pool.applyAsync(add_one_with_pool, [value])

    results = pool.checkPool()

    assert results == [16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16]


if __name__ == "__main__":
    test_write_progress_to_logger()
