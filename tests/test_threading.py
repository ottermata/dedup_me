import random
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import sleep

from dedup_me import ThreadingDedup, threading_dedup


def test_decorator_static() -> None:
    event = Event()

    @threading_dedup('static-key')
    def wait_for_event() -> int:
        event.wait()
        return random.randint(0, 10)

    with ThreadPoolExecutor() as executor:
        thread = executor.submit(wait_for_event)
        sleep(0.1)
        consumer = executor.submit(wait_for_event)
        sleep(0.1)

        event.set()
        assert thread.result() == consumer.result()


def test_decorator_dynamic() -> None:
    event = Event()

    @threading_dedup(key = lambda n: f'key-{n}')
    def wait_for_event(n: int) -> int:
        event.wait()
        return n

    with ThreadPoolExecutor() as executor:
        thread = executor.submit(wait_for_event, 1)
        sleep(0.1)
        consumer = executor.submit(wait_for_event, 1)
        sleep(0.1)

        thread2 = executor.submit(wait_for_event, 2)
        sleep(0.1)

        event.set()

        assert thread2.result() == 2

        assert thread.result() == 1
        assert consumer.result() == 1


# noinspection PyProtectedMember
def test_decorator_instance() -> None:
    event = Event()
    dedup = ThreadingDedup()

    @threading_dedup('static-key', dedup = dedup)
    def wait_for_event() -> int:
        event.wait()
        return 1

    with ThreadPoolExecutor() as executor:
        thread = executor.submit(wait_for_event)
        sleep(0.1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 1
            assert len(dedup._counts) == 1
            assert len(dedup._results) == 0

            internal_key = dedup._running['static-key'][1]
            assert dedup._counts[internal_key] == 0

        event.set()
        assert thread.result() == 1

    with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
        assert len(dedup._running) == 0
        assert len(dedup._counts) == 0
        assert len(dedup._results) == 0


# noinspection PyProtectedMember
def test_decorator_force() -> None:
    dedup = ThreadingDedup()

    @threading_dedup('static-key', dedup = dedup, force_new = lambda _event: True)
    def wait_for_event(event: Event) -> int:
        event.wait()
        return 1

    with ThreadPoolExecutor() as executor:
        event1 = Event()
        thread1 = executor.submit(wait_for_event, event1)
        sleep(0.1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 1
            assert len(dedup._counts) == 1
            assert len(dedup._results) == 0

        event2 = Event()
        thread2 = executor.submit(wait_for_event, event2)
        sleep(0.1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 1
            assert len(dedup._counts) == 2
            assert len(dedup._results) == 0

        event2.set()
        assert thread2.result() == 1

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 0
            assert len(dedup._counts) == 1
            assert len(dedup._results) == 0

        event1.set()
        assert thread1.result() == 1

    with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
        assert len(dedup._running) == 0
        assert len(dedup._counts) == 0
        assert len(dedup._results) == 0


# noinspection PyProtectedMember
def test_force() -> None:
    dedup = ThreadingDedup()

    def wait_for_event(event: Event) -> int:
        event.wait()
        return 1

    with ThreadPoolExecutor() as executor:
        event1 = Event()
        thread1 = executor.submit(dedup.run, 'test', lambda: wait_for_event(event1))
        sleep(0.1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 1
            assert len(dedup._counts) == 1
            assert len(dedup._results) == 0

        event2 = Event()
        thread2 = executor.submit(dedup.run, 'test', lambda: wait_for_event(event2), True)
        sleep(0.1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 1
            assert len(dedup._counts) == 2
            assert len(dedup._results) == 0

        event2.set()
        assert thread2.result() == 1

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 0
            assert len(dedup._counts) == 1
            assert len(dedup._results) == 0

        event1.set()
        assert thread1.result() == 1

    with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
        assert len(dedup._running) == 0
        assert len(dedup._counts) == 0
        assert len(dedup._results) == 0


# noinspection PyProtectedMember
def test_single() -> None:
    dedup = ThreadingDedup()
    event = Event()

    def wait_for_event() -> int:
        event.wait()
        return 1

    with ThreadPoolExecutor() as executor:
        thread = executor.submit(dedup.run, 'test', lambda: wait_for_event())
        sleep(0.1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 1
            assert len(dedup._counts) == 1
            assert len(dedup._results) == 0

            internal_key = dedup._running['test'][1]
            assert dedup._counts[internal_key] == 0

        event.set()
        assert thread.result() == 1

    with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
        assert len(dedup._running) == 0
        assert len(dedup._counts) == 0
        assert len(dedup._results) == 0


# noinspection PyProtectedMember
def test_many_consumers() -> None:
    dedup = ThreadingDedup()
    event = Event()
    event2 = Event()

    def wait_for_event() -> int:
        event.wait()
        return 1

    def wait_for_event_2() -> int:
        event2.wait()
        return 2

    with ThreadPoolExecutor() as executor:
        main = executor.submit(dedup.run, 'test', lambda: wait_for_event())
        sleep(0.1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 1
            assert len(dedup._counts) == 1
            assert len(dedup._results) == 0

            internal_key = dedup._running['test'][1]
            assert dedup._counts[internal_key] == 0

        consumer_1 = executor.submit(dedup.run, 'test', lambda: wait_for_event())
        sleep(0.1)
        with dedup._counts_lock:
            assert dedup._counts[internal_key] == 1

        # same key, so they will share the same result
        consumer_2 = executor.submit(dedup.run, 'test', lambda: wait_for_event_2())
        sleep(0.1)
        with dedup._counts_lock:
            assert dedup._counts[internal_key] == 2

        event.set()

        assert consumer_1.result() == 1
        assert consumer_2.result() == 1
        assert main.result() == 1

    with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
        assert len(dedup._running) == 0
        assert len(dedup._counts) == 0
        assert len(dedup._results) == 0


# noinspection PyProtectedMember
def test_many() -> None:
    dedup = ThreadingDedup()
    event1 = Event()
    event2 = Event()
    event3 = Event()

    def wait_for_event(event: Event, result: int) -> int:
        event.wait()
        return result

    with ThreadPoolExecutor() as executor:
        task1 = executor.submit(dedup.run, '1', lambda: wait_for_event(event1, 1))

        task2 = executor.submit(dedup.run, '2', lambda: wait_for_event(event2, 2))
        task2_consumer = executor.submit(dedup.run, '2', lambda: wait_for_event(event2, 2))

        task3 = executor.submit(dedup.run, '3', lambda: wait_for_event(event3, 3))
        task3_consumer_1 = executor.submit(dedup.run, '3', lambda: wait_for_event(event3, 33))
        task3_consumer_2 = executor.submit(dedup.run, '3', lambda: wait_for_event(event3, 333))

        sleep(1)

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 3
            assert len(dedup._counts) == 3
            assert len(dedup._results) == 0

        event3.set()
        assert task3_consumer_1.result() == 3
        assert task3_consumer_2.result() == 3
        assert task3.result() == 3

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 2
            assert len(dedup._counts) == 2
            assert len(dedup._results) == 0

        event1.set()
        event2.set()

        assert task1.result() == 1

        assert task2_consumer.result() == 2
        assert task2.result() == 2

        with dedup._running_lock, dedup._results_lock, dedup._counts_lock:
            assert len(dedup._running) == 0
            assert len(dedup._counts) == 0
            assert len(dedup._results) == 0
