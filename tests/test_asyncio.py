import asyncio
from asyncio import Event

from dedup_me import AsyncioDedup


# noinspection PyProtectedMember
async def _test_single() -> None:
    dedup = AsyncioDedup()
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
    result = await task
    assert result == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 0
    assert len(dedup._results) == 0


def test_single() -> None:
    asyncio.run(_test_single())


# noinspection PyProtectedMember
async def _test_many_consumers() -> None:
    dedup = AsyncioDedup()
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

    consumer_1_result = await consumer_1
    assert consumer_1_result == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 1

    consumer_2_result = await consumer_2
    assert consumer_2_result == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 1
    assert len(dedup._results) == 1

    result = await task
    assert result == 1

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 0
    assert len(dedup._results) == 0


def test_many_consumers() -> None:
    asyncio.run(_test_many_consumers())


# noinspection PyProtectedMember
async def _test_many() -> None:
    dedup = AsyncioDedup()
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
    t3c1_result = await task3_consumer_1
    t3c2_result = await task3_consumer_2
    t3_result = await task3

    assert t3_result == 3
    assert t3_result == t3c1_result
    assert t3_result == t3c2_result

    assert len(dedup._running) == 2
    assert len(dedup._counts) == 2
    assert len(dedup._results) == 0

    event1.set()
    event2.set()

    t1_result = await task1
    t2c_result = await task2_consumer
    t2_result = await task2

    assert t1_result == 1
    assert t2_result == 2
    assert t2c_result == 2

    assert len(dedup._running) == 0
    assert len(dedup._counts) == 0
    assert len(dedup._results) == 0


def test_many() -> None:
    asyncio.run(_test_many())
