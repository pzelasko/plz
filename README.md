# plz
Helpers for running native Python functions as qsub jobs on the CLSP grid.

plz uses [Dask](https://dask.org/) and [Dask-Jobqueue](https://jobqueue.dask.org/en/latest/) to distribute Python functions as SGE-managed jobs.
No need to wrap python scripts into bash scripts anymore!

## Installation

    pip install git+https://github.com/pzelasko/plz
    
## Examples

CPU jobs:

```python
def task(x):
    return x ** 2

import plz
plz.map(task, range(1000), jobs=10)
```
    
GPU jobs:

```python
def task(x):
    import torch
    return torch.tensor([x] * 100, device='cuda').sum()

import plz
plz.map(task, range(1000), jobs=1, gpus=1)
```
    
Using logs:

```python
def task(x):
    import logging
    logging.info(f'Running job with input {x}')
    return x ** 2

import plz
plz.map(task, range(1000), jobs=10, log_dir='/path/to/logs')
```
    
Single task:

```python
def task(x):
    return x ** 2

import plz
plz.run(task, 1)
```
   
## Technical details

Under the hood, for each `run` or `map` call, it creates your own "mini-cluster" of Python worker "services" where the jobs are being distributed. 
This cluster has its own scheduling, load balancing etc. It automatically shuts down as soon as all the inputs are processed.
