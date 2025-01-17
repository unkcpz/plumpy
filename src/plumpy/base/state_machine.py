# -*- coding: utf-8 -*-
"""The state machine for processes"""

from __future__ import annotations

import enum
import functools
import inspect
import logging
import os
import sys
from types import TracebackType
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Hashable,
    Iterable,
    List,
    Optional,
    Protocol,
    Sequence,
    Type,
    Union,
    runtime_checkable,
)

from plumpy.futures import Future

from .utils import call_with_super_check, super_check

__all__ = ['StateMachine', 'StateMachineMeta', 'TransitionFailed', 'event']

_LOGGER = logging.getLogger(__name__)

EVENT_CALLBACK_TYPE = Callable[['StateMachine', Hashable, Optional['State']], None]


class StateMachineError(Exception):
    """Base class for state machine errors"""


class StateEntryFailed(Exception):  # noqa: N818
    """
    Failed to enter a state, can provide the next state to go to via this exception
    """

    def __init__(self, state: State, *args: Any, **kwargs: Any) -> None:
        super().__init__('failed to enter state')
        self.state = state
        self.args = args
        self.kwargs = kwargs


class InvalidStateError(Exception):
    """The operation is not allowed in this state."""


class EventError(StateMachineError):
    def __init__(self, evt: str, msg: str):
        super().__init__(msg)
        self.event = evt


class TransitionFailed(Exception):  # noqa: N818
    """A state transition failed"""

    def __init__(
        self, initial_state: 'State', final_state: Optional['State'] = None, traceback_str: Optional[str] = None
    ) -> None:
        self.initial_state = initial_state
        self.final_state = final_state
        self.traceback_str = traceback_str
        super().__init__(self._format_msg())

    def _format_msg(self) -> str:
        msg = [f'{self.initial_state} -> {self.final_state}']
        if self.traceback_str is not None:
            msg.append(self.traceback_str)
        return '\n'.join(msg)


def event(
    from_states: Union[str, Type['State'], Iterable[Type['State']]] = '*',
    to_states: Union[str, Type['State'], Iterable[Type['State']]] = '*',
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """A decorator to check for correct transitions, raising ``EventError`` on invalid transitions."""
    if from_states != '*':
        if inspect.isclass(from_states):
            from_states = (from_states,)
        if not all(isinstance(state, State) for state in from_states):  # type: ignore
            raise TypeError(f'from_states: {from_states}')
    if to_states != '*':
        if inspect.isclass(to_states):
            to_states = (to_states,)
        if not all(isinstance(state, State) for state in to_states):  # type: ignore
            raise TypeError(f'to_states: {to_states}')

    def wrapper(wrapped: Callable[..., Any]) -> Callable[..., Any]:
        evt_label = wrapped.__name__

        @functools.wraps(wrapped)
        def transition(self: Any, *a: Any, **kw: Any) -> Any:
            initial = self._state

            if from_states != '*' and not any(isinstance(self._state, state) for state in from_states):  # type: ignore
                raise EventError(evt_label, f'Event {evt_label} invalid in state {initial.LABEL}')

            result = wrapped(self, *a, **kw)
            if not (result is False or isinstance(result, Future)):
                if to_states != '*' and not any(isinstance(self._state, state) for state in to_states):  # type: ignore
                    if self._state == initial:
                        raise EventError(evt_label, 'Machine did not transition')

                    raise EventError(
                        evt_label,
                        'Event produced invalid state transition from ' f'{initial.LABEL} to {self._state.LABEL}',
                    )

            return result

        return transition

    if inspect.isfunction(from_states):
        return wrapper(from_states)

    return wrapper


@runtime_checkable
class State(Protocol):
    LABEL: ClassVar[Any]
    ALLOWED: ClassVar[set[Any]]
    is_terminal: ClassVar[bool]

    def __init__(self, *args: Any, **kwargs: Any): ...

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@runtime_checkable
class Interruptable(Protocol):
    def interrupt(self, reason: Exception) -> None: ...


@runtime_checkable
class Proceedable(Protocol):
    def execute(self) -> State | None:
        """
        Execute the state, performing the actions that this state is responsible for.
        :returns: a state to transition to or None if finished.
        """
        ...


def create_state(st: StateMachine, state_label: Hashable, *args: Any, **kwargs: Any) -> State:
    if state_label not in st.get_states_map():
        raise ValueError(f'{state_label} is not a valid state')

    state_cls = st.get_states_map()[state_label]
    return state_cls(*args, **kwargs)


class StateEventHook(enum.Enum):
    """
    Hooks that can be used to register callback at various points in the state transition
    procedure.  The callback will be passed a state instance whose meaning will differ depending
    on the hook as commented below.
    """

    ENTERING_STATE: int = 0  # State passed will be the state that is being entered
    ENTERED_STATE: int = 1  # State passed will be the last state that we entered from
    EXITING_STATE: int = 2  # State passed will be the next state that will be entered (or None for terminal)


class StateMachineMeta(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> 'StateMachine':
        """
        Create the state machine and enter the initial state.

        :param args: Any positional arguments to be passed to the constructor
        :param kwargs: Any keyword arguments to be passed to the constructor
        :return: An instance of the state machine
        """
        inst: StateMachine = super().__call__(*args, **kwargs)
        inst.transition_to(inst.create_initial_state())
        call_with_super_check(inst.init)
        return inst


class StateMachine(metaclass=StateMachineMeta):
    STATES: Optional[Sequence[Type[State]]] = None
    _STATES_MAP: Optional[Dict[Hashable, Type[State]]] = None

    _transitioning = False
    _transition_failing = False

    @classmethod
    def get_states_map(cls) -> Dict[Hashable, Type[State]]:
        cls.__ensure_built()
        assert cls._STATES_MAP is not None  # required for type checking
        return cls._STATES_MAP

    @classmethod
    def get_states(cls) -> Sequence[Type[State]]:
        if cls.STATES is not None:
            return cls.STATES

        raise RuntimeError('States not defined')

    @classmethod
    def initial_state_label(cls) -> Any:
        cls.__ensure_built()
        assert cls.STATES is not None
        return cls.STATES[0].LABEL

    @classmethod
    def get_state_class(cls, label: Any) -> Type[State]:
        cls.__ensure_built()
        assert cls._STATES_MAP is not None
        return cls._STATES_MAP[label]

    @classmethod
    def __ensure_built(cls) -> None:
        try:
            # Check if it's already been built (and therefore sealed)
            if cls.__getattribute__(cls, 'sealed'):
                return
        except AttributeError:
            pass

        cls.STATES = cls.get_states()
        assert isinstance(cls.STATES, Iterable)

        # Build the states map
        cls._STATES_MAP = {}
        for state_cls in cls.STATES:
            assert isinstance(state_cls, State)
            label = state_cls.LABEL
            assert label not in cls._STATES_MAP, f"Duplicate label '{label}'"
            cls._STATES_MAP[label] = state_cls

        # should class initialise sealed = False?
        cls.sealed = True  # type: ignore

    def __init__(self) -> None:
        super().__init__()
        self.__ensure_built()
        self._state: Optional[State] = None
        self._exception_handler = None  # Note this appears to never be used
        self.set_debug((not sys.flags.ignore_environment and bool(os.environ.get('PYTHONSMDEBUG'))))
        self._transitioning = False
        self._event_callbacks: Dict[Hashable, List[EVENT_CALLBACK_TYPE]] = {}

    @super_check
    def init(self) -> None:
        """Called after entering initial state in `__call__` method of `StateMachineMeta`"""

    def __str__(self) -> str:
        return f'<{self.__class__.__name__}> ({self.state})'

    def create_initial_state(self, *args: Any, **kwargs: Any) -> State:
        return self.get_state_class(self.initial_state_label())(self, *args, **kwargs)

    @property
    def state(self) -> Any:
        if self._state is None:
            return None
        return self._state.LABEL

    def add_state_event_callback(self, hook: Hashable, callback: EVENT_CALLBACK_TYPE) -> None:
        """
        Add a callback to be called on a particular state event hook.
        The callback should have form fn(state_machine, hook, state)

        :param hook: The state event hook
        :param callback: The callback function
        """
        self._event_callbacks.setdefault(hook, []).append(callback)

    def remove_state_event_callback(self, hook: Hashable, callback: EVENT_CALLBACK_TYPE) -> None:
        if getattr(self, '_closed', False):
            # if the process is closed, then all callbacks have already been removed
            return None
        try:
            self._event_callbacks[hook].remove(callback)
        except (KeyError, ValueError):
            raise ValueError(f"Callback not set for hook '{hook}'")

    def _fire_state_event(self, hook: Hashable, state: Optional[State]) -> None:
        for callback in self._event_callbacks.get(hook, []):
            callback(self, hook, state)

    @super_check
    def on_terminated(self) -> None:
        """Called when a terminal state is entered"""

    def transition_to(self, new_state: State | None, **kwargs: Any) -> None:
        """Transite to the new state.

        The new target state will be create lazily when the state is not yet instantiated,
        which will happened for states not in the expect path such as pause and kill.
        The arguments are passed to the state class to create state instance.
        (process arg does not need to pass since it will always call with 'self' as process)
        """
        print(f'try: {self._state} -> {new_state}')
        assert not self._transitioning, 'Cannot call transition_to when already transitioning state'

        if new_state is None:
            # early return if the new state is `None`
            # it can happened when transit from terminal state
            return None

        initial_state_label = self._state.LABEL if self._state is not None else None
        label = None
        try:
            self._transitioning = True
            label = new_state.LABEL

            # If the previous transition failed, do not try to exit it but go straight to next state
            if not self._transition_failing:
                self._exit_current_state(new_state)

            try:
                self._enter_next_state(new_state)
            except StateEntryFailed as exception:
                new_state = exception.state
                label = new_state.LABEL
                self._exit_current_state(new_state)
                self._enter_next_state(new_state)

            if self._state is not None and self._state.is_terminal:
                call_with_super_check(self.on_terminated)
        except Exception:
            self._transitioning = False
            if self._transition_failing:
                raise
            self._transition_failing = True
            self.transition_failed(initial_state_label, label, *sys.exc_info()[1:])
        finally:
            self._transition_failing = False
            self._transitioning = False

    def transition_failed(
        self,
        initial_state: Hashable,
        final_state: Hashable,
        exception: Exception,
        trace: TracebackType,
    ) -> None:
        """Called when a state transitions fails.

        This method can be overwritten to change the default behaviour which is to raise the exception.

        :param exception: The transition failed exception.
        """
        raise exception.with_traceback(trace)

    def get_debug(self) -> bool:
        return self._debug

    def set_debug(self, enabled: bool) -> None:
        self._debug: bool = enabled

    def _exit_current_state(self, next_state: State) -> None:
        """Exit the given state"""

        # If we're just being constructed we may not have a state yet to exit,
        # in which case check the new state is the initial state
        if self._state is None:
            if next_state.LABEL != self.initial_state_label():
                raise RuntimeError(f"Cannot enter state '{next_state}' as the initial state")
            return  # Nothing to exit

        if next_state.LABEL not in self._state.ALLOWED:
            raise RuntimeError(f'Cannot transition from {self._state.LABEL} to {next_state.LABEL}')
        self._fire_state_event(StateEventHook.EXITING_STATE, next_state)
        self._state.exit()

    def _enter_next_state(self, next_state: State) -> None:
        last_state = self._state
        self._fire_state_event(StateEventHook.ENTERING_STATE, next_state)
        # Enter the new state
        next_state.enter()
        self._state = next_state
        self._fire_state_event(StateEventHook.ENTERED_STATE, last_state)
