[![PyPI version](https://badge.fury.io/py/dedup-me.svg)](https://badge.fury.io/py/dedup-me)

# dedup_me

dedup_me is a simple library for concurrent in-flight deduplication of functions.
This can be useful for e.g. API calls or DB access. Instead of querying the same data multiple times,
this library allows you to query once and share the result.

Note: This library does not cache results. After the result is returned, new consumers will call the function(API, DB, ...) again.

# Installation
```shell
pip install dedup-me
```

# Usage

## AsyncIO

```python
import asyncio
import random

from dedup_me import async_dedup

# @async_dedup('static-key')  # if the arguments don't matter, you can use a static key
@async_dedup(key = lambda x: f'dynamic-key-{x}')  # or pass in a key function that accepts all arguments
async def expensive_function(x: int) -> int:
    print('expensive function called')
    await asyncio.sleep(x)
    return random.randint(0, 10)


async def main() -> None:
    # call the function 10 times
    # this should only print 'expensive function called' once
    results = await asyncio.gather(
        *(
            expensive_function(1)
            for _ in range(10)
        )
    )
    print(results)
    # all results should be the same
    results_list = list(results)
    assert results_list == [results_list[0]] * 10
```

Alternatively, without the decorator:
```python
import asyncio
import random

from dedup_me import AsyncDedup


async def expensive_function() -> int:
    print('expensive function called')
    await asyncio.sleep(1)
    return random.randint(0, 10)


async def main() -> None:
    dedup = AsyncDedup()
    await asyncio.gather(
        *(
            # functions are grouped by the key, choose something that represents the function and its arguments
            # the second argument must be a function without arguments that returns an awaitable
            dedup.run('some-key', lambda: expensive_function())
            for _ in range(10)
        )
    )
```

## Threads

For threading just use the `threading_dedup` decorator or `ThreadingDedup`.
```python
from time import sleep
from dedup_me import threading_dedup, ThreadingDedup


@threading_dedup('key')
def expensive_function() -> None:
    sleep(1)

expensive_function()

# or
dedup = ThreadingDedup()
dedup.run('key', lambda: sleep(1))
```

## Forcing a new execution
You can enforce a new execution by passing `force_new = True` to the `run` function.
When using the decorator, you can add a callable that will receive all arguments and returns a boolean.
