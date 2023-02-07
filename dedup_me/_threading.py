import uuid
from threading import Event, Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, TypeVar

    T = TypeVar('T')


class ThreadingDedup:
    __slots__ = '_running', '_running_lock', '_results', '_results_lock', '_counts', '_counts_lock'

    def __init__(self) -> None:
        """
        Creates a locking deduplicator.
        """
        self._running: dict[str, tuple[Event, int]] = {}  # public key -> results event, unique internal key
        self._running_lock = Lock()
        self._results: dict[int, tuple[Event, object]] = {}  # internal key -> clear event, result
        self._results_lock = Lock()
        self._counts: dict[int, int] = {}  # internal key -> consumer count
        self._counts_lock = Lock()

    def _handle_result(self, public_key: str, result: 'T') -> 'T':
        """
        Handles the propagation of the result to the consumers, if there are any.

        :param public_key: Public facing key
        :param result: Result
        :return: Result
        """
        # remove this execution from the running queue to stop incoming requests from being added to this one
        with self._running_lock:
            event, internal_key = self._running.pop(public_key)

        # check if there are consumers waiting for this result
        with self._counts_lock:
            has_consumers = bool(self._counts[internal_key])
            if not has_consumers:
                del self._counts[internal_key]

        if has_consumers:
            # save the result along with a new event that will notify us once every consumer has gotten the result
            clear_event = Event()
            with self._results_lock:
                self._results[internal_key] = clear_event, result

            # set the event, to notify consumers of the result
            event.set()

            # wait until all consumers have read the result, then delete it
            clear_event.wait()
            with self._results_lock:
                del self._results[internal_key]
            with self._counts_lock:
                del self._counts[internal_key]

        return result

    def _wait_for_result(self, event: Event, internal_key: int) -> object:
        """
        Wait for the result to be available.

        :param event: Event to wait on
        :param internal_key: Internal execution key
        :return: Result
        """
        event.wait()

        with self._results_lock:
            count_event, result = self._results[internal_key]

        with self._counts_lock:
            self._counts[internal_key] -= 1
            if self._counts[internal_key] <= 0:
                count_event.set()

        return result

    def run(self, key: str, function: 'Callable[[], T]') -> 'T':
        """
        Start a deduplicated execution.
        Executions with the same `key` will be grouped together. The first execution to be started will propagate the
        result to others, if they registered themselves before the execution completed.

        :param key: Key for the execution
        :param function: Function that should be deduplicated
        :return: Execution result
        """
        with self._running_lock:
            event_and_internal_key = self._running.get(key)
            if event_and_internal_key:
                event, internal_key = event_and_internal_key
                with self._counts_lock:
                    self._counts[internal_key] += 1
            else:
                event = Event()
                internal_key = uuid.uuid4().int
                self._running[key] = event, internal_key
                with self._counts_lock:
                    self._counts[internal_key] = 0

        if event_and_internal_key:
            return self._wait_for_result(event, internal_key)  # type: ignore[return-value]
        else:
            return self._handle_result(key, function())
