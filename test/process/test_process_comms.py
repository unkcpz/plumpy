# -*- coding: utf-8 -*-
import asyncio
import unittest

from kiwipy import rmq

import pytest

import plumpy
from plumpy import process_comms

from .. import utils


class Process(plumpy.Process):

    def run(self):
        pass


class CustomObjectLoader(plumpy.DefaultObjectLoader):

    def load_object(self, identifier):
        if identifier == 'jimmy':
            return Process
        else:
            return super().load_object(identifier)

    def identify_object(self, obj):
        if isinstance(obj, Process) or issubclass(obj, Process):
            return 'jimmy'
        else:
            return super().identify_object(obj)


class TestProcessLauncher(unittest.TestCase):

    @pytest.mark.asyncio
    async def test_continue(self):
        persister = plumpy.InMemoryPersister()
        load_context = plumpy.LoadSaveContext()
        launcher = plumpy.ProcessLauncher(persister=persister, load_context=load_context)

        process = utils.DummyProcess()
        pid = process.pid
        persister.save_checkpoint(process)
        del process
        process = None

        result = await launcher._continue(None, **plumpy.create_continue_body(pid)[process_comms.TASK_ARGS])
        self.assertEqual(utils.DummyProcess.EXPECTED_OUTPUTS, result)

    @pytest.mark.asyncio
    async def test_loader_is_used(self):
        """ Make sure that the provided class loader is used by the process launcher """
        loader = CustomObjectLoader()
        proc = Process()
        persister = plumpy.InMemoryPersister(loader=loader)
        persister.save_checkpoint(proc)
        launcher = plumpy.ProcessLauncher(persister=persister, loader=loader)

        continue_task = plumpy.create_continue_body(proc.pid)
        await launcher._continue(None, **continue_task[process_comms.TASK_ARGS])
