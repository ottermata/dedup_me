# dedup_me

dedup_me is a simple library for concurrent in-flight deduplication of functions.
This can be useful for e.g. API calls or DB access. Instead of querying the same data multiple times,
this library allows you to query once and share the result.

Note: This library does not cache results. After the result is returned, new consumers will call the function(API, DB, ...) again.

# Usage

## AsyncIO

```python
import asyncio
import random

from dedup_me import AsyncioDedup


async def expensive_function():
    print('expensive function called')
    await asyncio.sleep(1)
    return random.randint(0, 10)


async def main():
    dedup = AsyncioDedup()

    # call the function 10 times
    # this should only print 'expensive function called' once
    results = await asyncio.gather(
        *(
            # functions are grouped by the key, choose something that represents the function and its arguments
            # the second argument must be a function without arguments that returns an awaitable
            dedup.run('some-key', lambda: expensive_function())
            for _ in range(10)
        )
    )
    print(results)
    # all results should be the same
    results_list = list(results)
    assert results_list == [results_list[0]] * 10


if __name__ == '__main__':
    asyncio.run(main())
```