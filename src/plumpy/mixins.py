# -*- coding: utf-8 -*-
from typing import Any, Optional

from . import persistence
from .utils import SAVED_STATE_TYPE, AttributesDict


class ContextMixin(persistence.Savable):
    """
    Add a context to a Process.  The contents of the context will be saved
    in the instance state unlike standard instance variables.
    """

    CONTEXT: str = '_context'

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._context: Optional[AttributesDict] = AttributesDict()

    @property
    def ctx(self) -> Optional[AttributesDict]:
        return self._context

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        try:
            self._context = AttributesDict(**saved_state[self.CONTEXT])
        except KeyError:
            pass
