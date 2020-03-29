"""
Module containing future related methods and classes
"""

from __future__ import absolute_import
from __future__ import print_function
import kiwipy
import asyncio
import inspect
import plumpy
import concurrent

__all__ = ['Future', 'gather', 'chain', 'copy_future', 'CancelledError', 'create_task']

CancelledError = kiwipy.CancelledError


class InvalidStateError(Exception):
    """Exception for when a future or action is in an invalid state"""
    pass


copy_future = kiwipy.copy_future  # pylint: disable=invalid-name
chain = kiwipy.chain  # pylint: disable=invalid-name
gather = lambda *args: gen.multi(args)  # pylint: disable=invalid-name


class Future(concurrent.futures.Future):
    """Just a concurrent Future that can be awaited in an event loop"""

    def __await__(self):
        return asyncio.wrap_future(self).__await__()


class CancellableAction(Future):
    """
    An action that can be launched and potentially cancelled
    """

    def __init__(self, action, cookie=None):
        super(CancellableAction, self).__init__()
        self._action = action
        self._cookie = cookie

    @property
    def cookie(self):
        """ A cookie that can be used to correlate the actions with something """
        return self._cookie

    def run(self, *args, **kwargs):
        """
        Runt he action
        :param args: the positional arguments to the action
        :param kwargs: the keyword arguments to the action
        """
        if self.done():
            raise InvalidStateError('Action has already been ran')

        try:
            with kiwipy.capture_exceptions(self):
                self.set_result(self._action(*args, **kwargs))
        finally:
            self._action = None


def create_task(coro, loop=None):
    """
    Schedule a call to a coroutine in the event loop and wrap the outcome
    in a future.

    :param coro: the coroutine to schedule
    :param loop: the event loop to schedule it in
    :return: the future representing the outcome of the coroutine
    :rtype: :class:`concurrent.futures.Future`
    """
    future = plumpy.Future()

    print('create_task')
    print((id(asyncio.get_event_loop())))
    print((id(loop)))

    async def run_task():
        print('inside run_task')
        print((id(asyncio.get_event_loop())))
        with kiwipy.capture_exceptions(future):
            res = coro()
            if inspect.isawaitable(res):
                future.set_result(await res)
            else:
                future.set_result(res)

    # function create_task is called in kiwipy's thread
    # but we want it awaited in event loop of plumpy's LoopCommunicator
    asyncio.run_coroutine_threadsafe(run_task(), loop)
    return future


def unwrap_kiwi_future(future):
    """
    Create a kiwi future that represents the final results of a nested series of futures,
    meaning that if the futures provided itself resolves to a future the returned
    future will not resolve to a value until the final chain of futures is not a future
    but a concrete value.  If at any point in the chain a future resolves to an exception
    then the returned future will also resolve to that exception.

    :param future: the future to unwrap
    :type future: :class:`kiwipy.Future`
    :return: the unwrapping future
    :rtype: :class:`kiwipy.Future`
    """
    unwrapping = kiwipy.Future()

    def unwrap(fut):
        if fut.cancelled():
            unwrapping.cancel()
        else:
            with kiwipy.capture_exceptions(unwrapping):
                result = fut.result()
                if isinstance(result, kiwipy.Future):
                    result.add_done_callback(unwrap)
                else:
                    unwrapping.set_result(result)

    future.add_done_callback(unwrap)
    return unwrapping
