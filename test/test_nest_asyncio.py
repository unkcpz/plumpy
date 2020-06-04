# -*- coding: utf-8 -*-
"""Process tests"""
import unittest
import pytest
import asyncio
import enum
import kiwipy

import plumpy
from plumpy import Process, ProcessState, BundleKeys
from plumpy.utils import AttributesFrozendict
from test import utils

import nest_asyncio
nest_asyncio.apply()

@pytest.mark.asyncio
async def test_process_scope():

    print()

    class ProcessTaskInterleave(plumpy.Process):

        async def task(self, steps: list):
            steps.append('[{}] started'.format(self.pid))
            assert plumpy.Process.current() is self
            steps.append('[{}] sleeping'.format(self.pid))
            await asyncio.sleep(0.1)
            assert plumpy.Process.current() is self
            steps.append('[{}] finishing'.format(self.pid))

    p1 = ProcessTaskInterleave()
    p2 = ProcessTaskInterleave()

    p1steps = []
    p2steps = []
    p1task = asyncio.ensure_future(p1._run_task(p1.task, p1steps))
    p2task = asyncio.ensure_future(p2._run_task(p2.task, p2steps))
    await p1task, p2task


class TestProcess_00(unittest.TestCase):


    def test_execute(self):
        proc = utils.DummyProcessWithOutput()
        print('before exe: ', id(asyncio.get_event_loop()))
        proc.execute()
        print('after exe: ', id(asyncio.get_event_loop()))

class TestProcess_01(unittest.TestCase):
    """
    Working fine when python>=3.7, but not for py3.6 and py3.5
    """

    def test_exception(self):
        proc = utils.ExceptionProcess()
        with self.assertRaises(RuntimeError):
            proc.execute()
        self.assertEqual(proc.state, ProcessState.EXCEPTED)

    def test_pause_in_process(self):
        """ Test that we can pause and cancel that by playing within the process """
        test_case = self

        class TestPausePlay(plumpy.Process):

            def run(self):
                fut = self.pause()
                test_case.assertIsInstance(fut, plumpy.Future)

        loop = asyncio.get_event_loop()

        listener = plumpy.ProcessListener()
        listener.on_process_paused = lambda _proc: loop.stop()

        proc = TestPausePlay(loop=loop)
        proc.add_process_listener(listener)

        loop.create_task(proc.step_until_terminated())
        loop.run_forever()

        self.assertTrue(proc.paused)
        self.assertEqual(plumpy.ProcessState.FINISHED, proc.state)

class TestProcess_02(unittest.TestCase):
    """
    Working fine when python>=3.7, but not for py3.6 and py3.5
    """
    def test_execute(self):
        proc = utils.DummyProcessWithOutput()
        proc.execute()

    def test_pause_in_process(self):
        """ Test that we can pause and cancel that by playing within the process """
        from asyncio import events
        events._set_running_loop(None)
        test_case = self

        class TestPausePlay(plumpy.Process):

            def run(self):
                fut = self.pause()
                test_case.assertIsInstance(fut, plumpy.Future)

        loop = asyncio.get_event_loop()

        listener = plumpy.ProcessListener()
        listener.on_process_paused = lambda _proc: loop.stop()

        proc = TestPausePlay(loop=loop)
        proc.add_process_listener(listener)

        loop.create_task(proc.step_until_terminated())
        loop.run_forever()

        self.assertTrue(proc.paused)
        self.assertEqual(plumpy.ProcessState.FINISHED, proc.state)


class TestProcess_03(unittest.TestCase):
    """
    For py3.5 and py3.5 have to running nested process in the same loop
    to keep the contextvar working as expected.
    """

    def test_process_nested(self):
        """
        >=py3.7
        Run multiple and nested processes to make sure the process stack is always correct
        """

        print()

        class StackTest(plumpy.Process):

            def run(self):
                pass

        class ParentProcess(plumpy.Process):

            def run(self):
                StackTest().execute()

        ParentProcess().execute()

    def test_process_nested_single_loop(self):
        """
        <py3.7
        Run multiple and nested processes to make sure the process stack is always correct
        """

        print()
        loop = asyncio.get_event_loop()

        class StackTest(plumpy.Process):

            def run(self):
                pass

        class ParentProcess(plumpy.Process):

            def run(self):
                StackTest(loop=loop).execute()

        ParentProcess(loop=loop).execute()


@pytest.mark.asyncio
async def test_kill_when_paused_always_passed():
    """
    pytest mark fixture must not be run in the unittest.TestCase
    """
    assert 1 == 0

class TestPytestAsyncUnittest(unittest.TestCase):


    @pytest.mark.asyncio
    async def test_kill_when_paused_always_passed(self):
        """
        This test will always unexpectedly passed.
        """
        assert 1 == 0
