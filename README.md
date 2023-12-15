# job-pool

[![PyPI version](https://img.shields.io/pypi/v/job_pool.svg?logo=pypi&logoColor=FFE873)](https://pypi.org/project/job_pool/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/job_pool.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/job_pool/)
[![PyPI downloads](https://img.shields.io/pypi/dm/job_pool.svg)](https://pypistats.org/packages/job_pool)
[![Build](https://github.com/matthewthe/job-pool/workflows/Publish%20release/badge.svg)](https://github.com/matthewthe/job-pool/actions?workflow=release)
[![Tests](https://github.com/matthewthe/job-pool/workflows/Unit%20tests/badge.svg)](https://github.com/matthewthe/job-pool/actions?workflow=tests)
[![Codecov](https://codecov.io/gh/matthewthe/job-pool/branch/main/graph/badge.svg)](https://codecov.io/gh/matthewthe/job-pool)


Enhanced Job Pool for Python Multiprocessing

## Usage

```python
from job_pool import JobPool

def add_one(i):
    return i + 1

def multiprocessed_add_one():
    pool = JobPool(4)
    for i in range(20):
        pool.applyAsync(add_one, [i])
    results = pool.checkPool(printProgressEvery=5)
    assert results == list(range(1,21))
```

## Installation

job-pool is available on PyPI and can be installed with `pip`:

```shell
pip install job-pool
```

Alternatively, you can install job-pool after cloning from this repository:

```shell
git clone https://github.com/matthewthe/job-pool.git
pip install .
```
