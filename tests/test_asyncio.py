import asyncio
from asyncio import Event

import pytest

from dedup_me import AsyncDedup, async_dedup


@pytest.mark.asyncio
async def test_decorator_static() -> None:
    event = Event()
    count = 0

    @async_dedup('static-key')
    async def wait_for_event() -> int:
        nonlocal count
        count += 1
        await event.wait()
        return count

    task = asyncio.create_task(wait_for_event())
    await asyncio.sleep(0.01)
    consumer = asyncio.create_task(wait_for_event())
    await asyncio.sleep(0.01)

    event.set()

    assert await consumer == 1
    assert await task == 1

    assert count == 1


@pytest.mark.asyncio
async def test_decorator_dynamic() -> None:
    event = Event()
    count = 0

    @async_dedup(key = lambda n: f'key-{n}')
    async def wait_for_event(n: int) -> int:
        nonlocal count
        count += 1
        await event.wait()
        return n

    task = asyncio.create_task(wait_for_event(1))
    await asyncio.sleep(0.01)
    consumer = asyncio.create_task(wait_for_event(1))
    await asyncio.sleep(0.01)

    assert count == 1

    task2 = asyncio.create_task(wait_for_event(2))
    await asyncio.sleep(0.01)

    assert count == 2

    event.set()

    assert await task2 == 2

    assert await consumer == 1
    assert await task == 1

    assert count == 2


# noinspection PyProtectedMember
@pytest.mark.asyncio
async def test_decorator_instance() -> None:
    event = Event()
    dedup = AsyncDedup()

    @async_dedup('static-key', dedup = dedup)
    async def wait_for_event() -> int:
        await event.wait()
        return 1

    task = asyncio.create_task(wait_for_event())
    await asyncio.sleep(0.01)

    assert len(dedup._running) == 1
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 0
    internal_key = dedup._running['static-key'][1]
    assert dedup._counts[internal_key] == 0

    event.set()

    assert await task == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 0
    assert len(dedup._results) == 0


# noinspection PyProtectedMember
@pytest.mark.asyncio
async def test_single() -> None:
    dedup = AsyncDedup()
    event = Event()

    async def wait_for_event() -> int:
        await event.wait()
        return 1

    task = asyncio.create_task(dedup.run('test', lambda: wait_for_event()))
    await asyncio.sleep(0.01)

    assert len(dedup._running) == 1
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 0
    internal_key = dedup._running['test'][1]
    assert dedup._counts[internal_key] == 0

    event.set()
    assert await task == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 0
    assert len(dedup._results) == 0


# noinspection PyProtectedMember
@pytest.mark.asyncio
async def test_many_consumers() -> None:
    dedup = AsyncDedup()
    event = Event()
    event2 = Event()
    count = 0

    async def wait_for_event() -> int:
        nonlocal count
        count += 1
        await event.wait()
        return 1

    async def wait_for_event_2() -> int:
        nonlocal count
        count += 1
        await event2.wait()
        return 2

    task = asyncio.create_task(dedup.run('test', lambda: wait_for_event()))
    await asyncio.sleep(0.01)

    assert len(dedup._running) == 1
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 0
    internal_key = dedup._running['test'][1]
    assert dedup._counts[internal_key] == 0

    consumer_1 = asyncio.create_task(dedup.run('test', lambda: wait_for_event()))
    await asyncio.sleep(0.01)
    assert dedup._counts[internal_key] == 1

    # same key, so they will share the same result
    consumer_2 = asyncio.create_task(dedup.run('test', lambda: wait_for_event_2()))
    await asyncio.sleep(0.01)
    assert dedup._counts[internal_key] == 2

    assert len(dedup._running) == 1
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 0

    event.set()

    assert await consumer_1 == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 1

    assert await consumer_2 == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 1

    assert await task == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 0
    assert len(dedup._results) == 0

    assert count == 1


# noinspection PyProtectedMember
@pytest.mark.asyncio
async def test_many() -> None:
    dedup = AsyncDedup()
    event1 = Event()
    event2 = Event()
    event3 = Event()

    async def wait_for_event(event: Event, result: int) -> int:
        await event.wait()
        return result

    task1 = asyncio.create_task(dedup.run('1', lambda: wait_for_event(event1, 1)))

    task2 = asyncio.create_task(dedup.run('2', lambda: wait_for_event(event2, 2)))
    task2_consumer = asyncio.create_task(dedup.run('2', lambda: wait_for_event(event2, 2)))

    task3 = asyncio.create_task(dedup.run('3', lambda: wait_for_event(event3, 3)))
    task3_consumer_1 = asyncio.create_task(dedup.run('3', lambda: wait_for_event(event3, 3)))
    task3_consumer_2 = asyncio.create_task(dedup.run('3', lambda: wait_for_event(event3, 3)))

    await asyncio.sleep(0.1)

    assert len(dedup._running) == 3
    assert len(dedup._counts) == 3
    assert len(dedup._results) == 0

    event3.set()

    t3_result = await task3
    assert t3_result == 3
    assert t3_result == await task3_consumer_1
    assert t3_result == await task3_consumer_2

    assert len(dedup._running) == 2
    assert len(dedup._counts) == 2
    assert len(dedup._results) == 0

    event1.set()
    event2.set()

    assert await task1 == 1
    assert await task2_consumer == 2
    assert await task2 == 2

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 0
    assert len(dedup._results) == 0
