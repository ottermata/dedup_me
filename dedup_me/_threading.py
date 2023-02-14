import uuid
from functools import update_wrapper
from threading import Event, Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, ParamSpec, TypeVar

    T = TypeVar('T')
    P = ParamSpec('P')


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

    def _handle_result(
            self,
            public_key: str,
            internal_key: int,
            event: Event,
            result: 'T',
    ) -> 'T':
        """
        Handles the propagation of the result to the consumers, if there are any.

        :param public_key: Public facing key
        :param internal_key: Internal key
        :param event: Event showing the result is ready
        :param result: Result
        :return: Result
        """
        # remove this execution from the running queue to stop incoming requests from being added to this one
        with self._running_lock:
            if event_and_key := self._running.get(public_key):
                _, current_internal_key = event_and_key
                if internal_key == current_internal_key:
                    self._running.pop(public_key)

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

    def run(
            self,
            key: str,
            function: 'Callable[[], T]',
            force_new: bool = False,
    ) -> 'T':
        """
        Start a deduplicated execution.
        Executions with the same `key` will be grouped together. The first execution to be started will propagate the
        result to others, if they registered themselves before the execution completed.

        :param key: Key for the execution
        :param function: Function that should be deduplicated
        :param force_new: Enforce a new execution, already running executions won't be affected.
                          New consumers will use the newly created execution.
        :return: Execution result
        """
        with self._running_lock:
            if force_new or (key not in self._running):
                owned = True
                event = Event()
                internal_key = uuid.uuid4().int
                self._running[key] = event, internal_key  # this may overwrite an older execution
                with self._counts_lock:
                    self._counts[internal_key] = 0

            else:
                owned = False
                event, internal_key = self._running[key]
                with self._counts_lock:
                    self._counts[internal_key] += 1

        if owned:
            return self._handle_result(key, internal_key, event, function())
        else:
            return self._wait_for_result(event, internal_key)  # type: ignore[return-value]


def threading_dedup(
        static_key: str | None = None,
        key: 'Callable[P, str] | None' = None,
        force_new: 'Callable[P, bool] | None' = None,
        dedup: ThreadingDedup | None = None,
) -> 'Callable[[Callable[P, T]], Callable[P, T]]':
    """
    Decorator for de-duplicating a function.

    :param static_key: Static key, if the function arguments don't matter
    :param key: Key function, generate a key based on the function arguments
    :param force_new: Force a new
    :param dedup: ThreadingDedup instance to use, will create a new one if not set
    :return: Wrapped function
    """
    if (static_key is None) == (key is None):
        raise ValueError('Exactly one of `static_key` or `key` must be set')
    if static_key is not None and not isinstance(static_key, str):
        raise ValueError('`static_key` must be a string, use `key` for generating keys based on the arguments')
    if key is not None and not callable(key):
        raise ValueError('`key` must be a callable')
    if force_new is not None and not callable(force_new):
        raise ValueError('`force_new` must be a callable')

    if dedup is None:
        dedup = ThreadingDedup()
    elif not isinstance(dedup, ThreadingDedup):
        raise ValueError('`dedup` must be an instance of ThreadingDedup')

    def wrapper(f: 'Callable[P, T]') -> 'Callable[P, T]':
        function_source = f'''
def inner(*args, **kwargs):
    return dedup.run(
        key = {'static_key' if static_key else 'key(*args, **kwargs)'},
        function = lambda: f(*args, **kwargs),
        force_new = {'force_new(*args, **kwargs)' if force_new else 'False'}
    )
'''
        exec_globals = dict(
            f = f,
            dedup = dedup,
            static_key = static_key,
            key = key,
            force_new = force_new,
        )
        exec(function_source, exec_globals)
        return update_wrapper(exec_globals['inner'], f)

    return wrapper
