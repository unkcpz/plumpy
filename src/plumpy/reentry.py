import abc
from concurrent.futures import ThreadPoolExecutor
import contextvars
import asyncio
import inspect
import queue
import sys
import threading
from weakref import WeakSet
from greenlet import greenlet

def _close_loop(loop):
    if loop is not None:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            try:
                shutdown_default_executor = loop.shutdown_default_executor
            except AttributeError:
                pass
            else:
                loop.run_until_complete(shutdown_default_executor())
        finally:
            loop.close()

class _Genlet(greenlet):
    """
    Generator-like object based on ``greenlets``. It allows nested :class:`_Genlet`
    to make their parent yield on their behalf, as if callees could decide to
    be annotated ``yield from`` without modifying the caller.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Forward the context variables to the greenlet, which will not happen
        # by default:
        # https://greenlet.readthedocs.io/en/latest/contextvars.html
        self.gr_context = contextvars.copy_context()

    @classmethod
    def from_coro(cls, coro):
        """
        Create a :class:`_Genlet` from a given coroutine, treating it as a
        generator.
        """
        f = lambda value: self.consume_coro(coro, value)
        self = cls(f)
        return self

    def consume_coro(self, coro, value):
        """
        Send ``value`` to ``coro`` then consume the coroutine, passing all its
        yielded actions to the enclosing :class:`_Genlet`. This allows crossing
        blocking calls layers as if they were async calls with `await`.
        """
        excep = None
        while True:
            try:
                if excep is None:
                    future = coro.send(value)
                else:
                    future = coro.throw(excep)

            except StopIteration as e:
                return e.value
            else:
                parent = self.parent
                # Switch back to the consumer that returns the values via
                # send()
                try:
                    value = parent.switch(future)
                except BaseException as e:
                    excep = e
                    value = None
                else:
                    excep = None

    @classmethod
    def get_enclosing(cls):
        """
        Get the immediately enclosing :class:`_Genlet` in the callstack or
        ``None``.
        """
        g = greenlet.getcurrent()
        while not (isinstance(g, cls) or g is None):
            g = g.parent
        return g

    def _send_throw(self, value, excep):
        self.parent = greenlet.getcurrent()

        # Switch back to the function yielding values
        if excep is None:
            result = self.switch(value)
        else:
            result = self.throw(excep)

        if self:
            return result
        else:
            raise StopIteration(result)

    def gen_send(self, x):
        """
        Similar to generators' ``send`` method.
        """
        return self._send_throw(x, None)

    def gen_throw(self, x):
        """
        Similar to generators' ``throw`` method.
        """
        return self._send_throw(None, x)


class _AwaitableGenlet:
    """
    Wrap a coroutine with a :class:`_Genlet` and wrap that to be awaitable.
    """

    @classmethod
    def wrap_coro(cls, coro):
        async def coro_f():
            # Make sure every new task will be instrumented since a task cannot
            # yield futures on behalf of another task. If that were to happen,
            # the task B trying to do a nested yield would switch back to task
            # A, asking to yield on its behalf. Since the event loop would be
            # currently handling task B, nothing would handle task A trying to
            # yield on behalf of B, leading to a deadlock.
            loop = asyncio.get_running_loop()
            _install_task_factory(loop)

            # Create a top-level _AwaitableGenlet that all nested runs will use
            # to yield their futures
            _coro = cls(coro)

            return await _coro

        return coro_f()

    def __init__(self, coro):
        self._coro = coro

    def __await__(self):
        coro = self._coro
        is_started = inspect.iscoroutine(coro) and coro.cr_running

        def genf():
            gen = _Genlet.from_coro(coro)
            value = None
            excep = None

            # The coroutine is already started, so we need to dispatch the
            # value from the upcoming send() to the gen without running
            # gen first.
            if is_started:
                try:
                    value = yield
                except BaseException as e:
                    excep = e

            while True:
                try:
                    if excep is None:
                        future = gen.gen_send(value)
                    else:
                        future = gen.gen_throw(excep)
                except StopIteration as e:
                    return e.value
                finally:
                    _set_current_context(gen.gr_context)

                try:
                    value = yield future
                except BaseException as e:
                    excep = e
                    value = None
                else:
                    excep = None

        gen = genf()
        if is_started:
            # Start the generator so it waits at the first yield point
            gen.gen_send(None)

        return gen


def _allow_nested_run(coro):
    if _Genlet.get_enclosing() is None:
        return _AwaitableGenlet.wrap_coro(coro)
    else:
        return coro


def allow_nested_run(coro):
    """
    Wrap the coroutine ``coro`` such that nested calls to :func:`run` will be
    allowed.
    .. warning:: The coroutine needs to be consumed in the same OS thread it
        was created in.
    """
    return _allow_nested_run(coro)


# This thread runs coroutines that cannot be ran on the event loop in the
# current thread. Instead, they are scheduled in a separate thread where
# another event loop has been setup, so we can wrap coroutines before
# dispatching them there.
_CORO_THREAD_EXECUTOR = ThreadPoolExecutor(
    # Allow for a ridiculously large number so that we will never end up
    # queuing one job after another. This is critical as we could otherwise end
    # up in deadlock, if a job triggers another job and waits for it.
    max_workers=2**64,
)


def _check_executor_alive(executor):
    try:
        executor.submit(lambda: None)
    except RuntimeError:
        return False
    else:
        return True


_PATCHED_LOOP_LOCK = threading.Lock()
_PATCHED_LOOP = WeakSet()
def _install_task_factory(loop):
    """
    Install a task factory on the given event ``loop`` so that top-level
    coroutines are wrapped using :func:`allow_nested_run`. This ensures that
    the nested :func:`run` infrastructure will be available.
    """
    def install(loop):
        if sys.version_info >= (3, 11):
            def default_factory(loop, coro, context=None):
                return asyncio.Task(coro, loop=loop, context=context)
        else:
            def default_factory(loop, coro, context=None):
                return asyncio.Task(coro, loop=loop)

        make_task = loop.get_task_factory() or default_factory
        def factory(loop, coro, context=None):
            # Make sure each Task will be able to yield on behalf of its nested
            # await beneath blocking layers
            coro = _AwaitableGenlet.wrap_coro(coro)
            return make_task(loop, coro, context=context)

        loop.set_task_factory(factory)

    with _PATCHED_LOOP_LOCK:
        if loop in _PATCHED_LOOP:
            return
        else:
            install(loop)
            _PATCHED_LOOP.add(loop)


def _set_current_context(ctx):
    """
    Get all the variable from the passed ``ctx`` and set them in the current
    context.
    """
    for var, val in ctx.items():
        var.set(val)


class _CoroRunner(abc.ABC):
    """
    ABC for an object that can execute multiple coroutines in a given
    environment.
    This allows running coroutines for which it might be an assumption, such as
    the awaitables yielded by an async generator that are all attached to a
    single event loop.
    """
    @abc.abstractmethod
    def _run(self, coro):
        pass

    def run(self, coro):
        # Ensure we have a fresh coroutine. inspect.getcoroutinestate() does not
        # work on all objects that asyncio creates on some version of Python, such
        # as iterable_coroutine
        assert not (inspect.iscoroutine(coro) and coro.cr_running)
        return self._run(coro)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        pass


class _ThreadCoroRunner(_CoroRunner):
    """
    Run the coroutines on a thread picked from a
    :class:`concurrent.futures.ThreadPoolExecutor`.
    Critically, this allows running multiple coroutines out of the same thread,
    which will be reserved until the runner ``__exit__`` method is called.
    """
    def __init__(self, future, jobq, resq):
        self._future = future
        self._jobq = jobq
        self._resq = resq

    @staticmethod
    def _thread_f(jobq, resq):
        def handle_jobs(runner):
            while True:
                job = jobq.get()
                if job is None:
                    return
                else:
                    ctx, coro = job
                    try:
                        value = ctx.run(runner.run, coro)
                    except BaseException as e:
                        value = None
                        excep = e
                    else:
                        excep = None

                    resq.put((ctx, excep, value))

        with _LoopCoroRunner(None) as runner:
            handle_jobs(runner)

    @classmethod
    def from_executor(cls, executor):
        jobq = queue.SimpleQueue()
        resq = queue.SimpleQueue()

        try:
            future = executor.submit(cls._thread_f, jobq, resq)
        except RuntimeError as e:
            if _check_executor_alive(executor):
                raise e
            else:
                raise RuntimeError('Devlib relies on nested asyncio implementation requiring threads. These threads are not available while shutting down the interpreter.')

        return cls(
            jobq=jobq,
            resq=resq,
            future=future,
        )

    def _run(self, coro):
        ctx = contextvars.copy_context()
        self._jobq.put((ctx, coro))
        ctx, excep, value = self._resq.get()

        _set_current_context(ctx)

        if excep is None:
            return value
        else:
            raise excep

    def __exit__(self, *args, **kwargs):
        self._jobq.put(None)
        self._future.result()


class _LoopCoroRunner(_CoroRunner):
    """
    Run a coroutine on the given event loop.
    The passed event loop is assumed to not be running. If ``None`` is passed,
    a new event loop will be created in ``__enter__`` and closed in
    ``__exit__``.
    """
    def __init__(self, loop):
        self.loop = loop
        self._owned = False

    def _run(self, coro):
        loop = self.loop

        # Back-propagate the contextvars that could have been modified by the
        # coroutine. This could be handled by asyncio.Runner().run(...,
        # context=...) or loop.create_task(..., context=...) but these APIs are
        # only available since Python 3.11
        ctx = None
        async def capture_ctx():
            nonlocal ctx
            try:
                return await _allow_nested_run(coro)
            finally:
                ctx = contextvars.copy_context()

        try:
            return loop.run_until_complete(capture_ctx())
        finally:
            _set_current_context(ctx)

    def __enter__(self):
        loop = self.loop
        if loop is None:
            owned = True
            loop = asyncio.new_event_loop()
        else:
            owned = False

        asyncio.set_event_loop(loop)

        self.loop = loop
        self._owned = owned
        return self

    def __exit__(self, *args, **kwargs):
        if self._owned:
            asyncio.set_event_loop(None)
            _close_loop(self.loop)


class _GenletCoroRunner(_CoroRunner):
    """
    Run a coroutine assuming one of the parent coroutines was wrapped with
    :func:`allow_nested_run`.
    """
    def __init__(self, g):
        self._g = g

    def _run(self, coro):
        return self._g.consume_coro(coro, None)


def _get_runner():
    executor = _CORO_THREAD_EXECUTOR
    g = _Genlet.get_enclosing()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    # We have an coroutine wrapped with allow_nested_run() higher in the
    # callstack, that we will be able to use as a conduit to yield the
    # futures.
    if g is not None:
        return _GenletCoroRunner(g)
    # No event loop setup, so we can just make our own
    elif loop is None:
        return _LoopCoroRunner(None)
    # There is an event loop setup, but it is not currently running so we
    # can just re-use it.
    #
    # TODO: for now, this path is dead since asyncio.get_running_loop() will
    # always raise a RuntimeError if the loop is not running, even if
    # asyncio.set_event_loop() was used.
    elif not loop.is_running():
        return _LoopCoroRunner(loop)
    # There is an event loop currently running in our thread, so we cannot
    # just create another event loop and install it since asyncio forbids
    # that. The only choice is doing this in a separate thread that we
    # fully control.
    else:
        return _ThreadCoroRunner.from_executor(executor)
