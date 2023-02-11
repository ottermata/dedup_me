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

    async def _handle_result(self, public_key: str, coro: 'Awaitable[T]') -> 'T':
        """
        Awaits the coroutine and handles the propagation of the result to the consumers, if there are any.

        :param public_key: Public facing key
        :param coro: Coroutine
        :return: Coroutine result
        """
        # run the coroutine to get the result
        result = await coro

        # remove this execution from the running queue to stop incoming requests from being added to this one
        event, internal_key = self._running.pop(public_key)

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

    async def run(self, key: str, getter: 'Callable[[], Awaitable[T]]') -> 'T':
        """
        Start a deduplicated execution.
        Executions with the same `key` will be grouped together. The first execution to be started will propagate the
        result to others, if they registered themselves before the execution completed.

        :param key: Key for the execution
        :param getter: Function that returns the awaitable execution
        :return: Execution result
        """
        event_and_internal_key = self._running.get(key)
        if event_and_internal_key:
            event, internal_key = event_and_internal_key
            return await self._wait_for_result(event, internal_key)  # type: ignore[return-value]

        else:
            event = Event()
            internal_key = uuid.uuid4().int
            self._running[key] = event, internal_key
            self._counts[internal_key] = 0

            return await self._handle_result(key, getter())


def async_dedup(
        static_key: str | None = None,
        key: 'Callable[P, str] | None' = None,
        dedup: AsyncDedup | None = None,
) -> 'Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]':
    """
    Decorator for de-duplicating an async function.

    :param static_key: Static key, if the function arguments don't matter
    :param key: Key function, generate a key based on the function arguments
    :param dedup: AsyncioDedup instance to use, will create a new one if not set
    :return: Wrapped function
    """
    if (static_key is None) == (key is None):
        raise ValueError('Exactly one of `static_key` or `key` must be set')
    if static_key is not None and not isinstance(static_key, str):
        raise ValueError('`static_key` must be a string, use `key` for generating keys based on the arguments')
    if key is not None and not callable(key):
        raise ValueError('`key` must be a callable')

    if dedup is None:
        dedup = AsyncDedup()
    elif not isinstance(dedup, AsyncDedup):
        raise ValueError('`dedup` must be an instance of AsyncioDedup')

    def wrapper(f: 'Callable[P, Awaitable[T]]') -> 'Callable[P, Awaitable[T]]':
        if static_key:
            async def inner(*args: 'P.args', **kwargs: 'P.kwargs') -> 'T':
                return await dedup.run(static_key, lambda: f(*args, **kwargs))  # type: ignore[union-attr,arg-type]

        else:
            async def inner(*args: 'P.args', **kwargs: 'P.kwargs') -> 'T':
                k = key(*args, **kwargs)  # type: ignore[misc]
                return await dedup.run(k, lambda: f(*args, **kwargs))  # type: ignore[union-attr]

        return update_wrapper(inner, f)

    return wrapper
