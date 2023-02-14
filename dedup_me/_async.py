import uuid
from asyncio import Event
from functools import update_wrapper
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Awaitable, Callable, ParamSpec, TypeVar

    T = TypeVar('T')
    P = ParamSpec('P')


class AsyncDedup:
    __slots__ = '_running', '_results', '_counts'

    def __init__(self) -> None:
        self._running: dict[str, tuple[Event, int]] = {}  # public key -> results event, unique internal key
        self._results: dict[int, tuple[Event, object]] = {}  # internal key -> clear event, result
        self._counts: dict[int, int] = {}  # internal key -> consumer count

    async def _handle_result(
            self,
            public_key: str,
            internal_key: int,
            event: Event,
            coro: 'Awaitable[T]',
    ) -> 'T':
        """
        Awaits the coroutine and handles the propagation of the result to the consumers, if there are any.

        :param public_key: Public facing key
        :param internal_key: Internal key
        :param event: Event showing the result is ready
        :param coro: Coroutine
        :return: Coroutine result
        """
        # run the coroutine to get the result
        result = await coro

        # remove this execution from the running queue to stop incoming requests from being added to this one
        if event_and_key := self._running.get(public_key):
            _, current_internal_key = event_and_key
            if internal_key == current_internal_key:
                self._running.pop(public_key)

        # check if there are consumers waiting for this result
        if self._counts[internal_key]:
            # save the result along with a new event that will notify us once every consumer has gotten the result
            clear_event = Event()
            self._results[internal_key] = clear_event, result

            # set the event, to notify consumers of the result
            event.set()

            # wait until all consumers have read the result, then delete it
            await clear_event.wait()
            del self._results[internal_key]

        del self._counts[internal_key]
        return result

    async def _wait_for_result(self, event: Event, internal_key: int) -> object:
        """
        Wait for the result to be available.

        :param event: Event to wait on
        :param internal_key: Internal execution key
        :return: Result
        """
        self._counts[internal_key] += 1

        await event.wait()

        count_event, result = self._results[internal_key]
        self._counts[internal_key] -= 1
        if self._counts[internal_key] <= 0:
            count_event.set()

        return result

    async def run(
            self,
            key: str,
            getter: 'Callable[[], Awaitable[T]]',
            force_new: bool = False,
    ) -> 'T':
        """
        Start a deduplicated execution.
        Executions with the same `key` will be grouped together. The first execution to be started will propagate the
        result to others, if they registered themselves before the execution completed.

        :param key: Key for the execution
        :param getter: Function that returns the awaitable execution
        :param force_new: Enforce a new execution, already running executions won't be affected.
                          New consumers will use the newly created execution.
        :return: Execution result
        """
        if force_new or (key not in self._running):
            event = Event()
            internal_key = uuid.uuid4().int
            self._running[key] = event, internal_key  # this may overwrite an older execution
            self._counts[internal_key] = 0

            return await self._handle_result(key, internal_key, event, getter())
        else:
            event, internal_key = self._running[key]
            return await self._wait_for_result(event, internal_key)  # type: ignore[return-value]


def async_dedup(
        static_key: str | None = None,
        key: 'Callable[P, str] | None' = None,
        force_new: 'Callable[P, bool] | None' = None,
        dedup: AsyncDedup | None = None,
) -> 'Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]':
    """
    Decorator for de-duplicating an async function.

    :param static_key: Static key, if the function arguments don't matter
    :param key: Key function, generate a key based on the function arguments
    :param force_new: Force a new
    :param dedup: AsyncioDedup instance to use, will create a new one if not set
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
        dedup = AsyncDedup()
    elif not isinstance(dedup, AsyncDedup):
        raise ValueError('`dedup` must be an instance of AsyncioDedup')

    def wrapper(f: 'Callable[P, Awaitable[T]]') -> 'Callable[P, Awaitable[T]]':
        function_source = f'''
async def inner(*args, **kwargs):
    return await dedup.run(
        key = {'static_key' if static_key else 'key(*args, **kwargs)'},
        getter = lambda: f(*args, **kwargs),
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
