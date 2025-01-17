# -*- coding: utf-8 -*-
# mypy: disable-error-code=name-defined
__version__ = '0.24.0'

import logging

# interfaces
from .controller import *
from .coordinator import *
from .events import *
from .exceptions import *
from .futures import *
from .loaders import *
from .message import *
from .mixins import *
from .persistence import *
from .ports import *
from .process_listener import *
from .process_states import *
from .processes import *
from .utils import *
from .workchains import *

__all__ = (
    # controller
    'ProcessController',
    # coordinator
    'Coordinator',
    # event
    'PlumpyEventLoopPolicy',
    'get_event_loop',
    'new_event_loop',
    'reset_event_loop_policy',
    'run_until_complete',
    'set_event_loop',
    'set_event_loop_policy',
    # exceptions
    'ClosedError',
    'CoordinatorConnectionError',
    'CoordinatorTimeoutError',
    'InvalidStateError',
    'KilledError',
    'PersistenceError',
    'UnsuccessfulResult',
    # futures
    'CancellableAction',
    'Future',
    'capture_exceptions',
    'create_task',
    'create_task',
    # loaders
    'DefaultObjectLoader',
    'ObjectLoader',
    'get_object_loader',
    'set_object_loader',
    # message
    'MessageBuilder',
    'ProcessLauncher',
    'create_continue_body',
    'create_launch_body',
    # mixins
    'ContextMixin',
    # persistence
    'Bundle',
    'InMemoryPersister',
    'LoadSaveContext',
    'PersistedCheckpoint',
    'Persister',
    'PicklePersister',
    'Savable',
    'SavableFuture',
    'auto_persist',
    # ports
    'UNSPECIFIED',
    'InputPort',
    'OutputPort',
    'Port',
    'PortNamespace',
    'PortValidationError',
    # process_listener
    'ProcessListener',
    # process_states/States
    'Continue',
    'Created',
    'Excepted',
    'Finished',
    'Interruption',
    # process_states/Commands
    'Kill',
    'KillInterruption',
    'Killed',
    'PauseInterruption',
    'ProcessState',
    'Running',
    'Stop',
    'Wait',
    'Waiting',
    # processes
    'BundleKeys',
    'Process',
    'ProcessSpec',
    'TransitionFailed',
    # utils
    'AttributesDict',
    # workchain
    'ToContext',
    'WorkChain',
    'WorkChainSpec',
    'if_',
    'return_',
    'while_',
)


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        pass


logging.getLogger('plumpy').addHandler(NullHandler())
