"""Process tests"""
import unittest
import pytest
import asyncio

import plumpy
from plumpy import Process, ProcessState, BundleKeys
from test import test_utils

class ForgetToCallParent(plumpy.Process):

    def __init__(self, forget_on):
        super(ForgetToCallParent, self).__init__()
        self.forget_on = forget_on

    def on_create(self):
        if self.forget_on != 'create':
            super(ForgetToCallParent, self).on_create()

    def on_run(self):
        if self.forget_on != 'run':
            super(ForgetToCallParent, self).on_run()

    def on_except(self, exception):
        if self.forget_on != 'except':
            super(ForgetToCallParent, self).on_except(exception)

    def on_finish(self, result, successful):
        if self.forget_on != 'finish':
            super(ForgetToCallParent, self).on_finish(result, successful)

    def on_kill(self, msg):
        if self.forget_on != 'kill':
            super(ForgetToCallParent, self).on_kill(msg)



class TestProcess(unittest.TestCase):

    def test_spec(self):
        """
        Check that the references to specs are doing the right thing...
        """
        proc = test_utils.DummyProcess()
        self.assertIsNot(test_utils.DummyProcess.spec(), Process.spec())
        self.assertIs(proc.spec(), test_utils.DummyProcess.spec())

        class Proc(test_utils.DummyProcess):
            pass

        self.assertIsNot(Proc.spec(), Process.spec())
        self.assertIsNot(Proc.spec(), test_utils.DummyProcess.spec())
        p = Proc()
        self.assertIs(p.spec(), Proc.spec())

    def test_dynamic_inputs(self):

        class NoDynamic(Process):
            pass

        class WithDynamic(Process):

            @classmethod
            def define(cls, spec):
                super(WithDynamic, cls).define(spec)
                spec.inputs.dynamic = True

        with self.assertRaises(ValueError):
            NoDynamic(inputs={'a': 5}).execute()

        proc = WithDynamic(inputs={'a': 5})
        proc.execute()

    def test_inputs(self):

        class Proc(Process):

            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('a')

        p = Proc({'a': 5})

        # Check that we can access the inputs after creating
        self.assertEqual(p.raw_inputs.a, 5)
        with self.assertRaises(AttributeError):
            p.raw_inputs.b

    def test_inputs_default(self):

        class Proc(test_utils.DummyProcess):

            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('input', default=5, required=False)

        # Supply a value
        p = Proc(inputs={'input': 2})
        self.assertEqual(p.inputs['input'], 2)

        # Don't supply, use default
        p = Proc()
        self.assertEqual(p.inputs['input'], 5)

    def test_inputs_default_that_evaluate_to_false(self):
        for def_val in (True, False, 0, 1):

            class Proc(test_utils.DummyProcess):

                @classmethod
                def define(cls, spec):
                    super(Proc, cls).define(spec)
                    spec.input('input', default=def_val)

            # Don't supply, use default
            p = Proc()
            self.assertIn('input', p.inputs)
            self.assertEqual(p.inputs['input'], def_val)

    def test_nested_namespace_defaults(self):
        """Process with a default in a nested namespace should be created, even if top level namespace not supplied."""

        class SomeProcess(Process):

            @classmethod
            def define(cls, spec):
                super(SomeProcess, cls).define(spec)
                spec.input_namespace('namespace', required=False)
                spec.input('namespace.sub', default=True)

        process = SomeProcess()
        self.assertIn('sub', process.inputs.namespace)
        self.assertEqual(process.inputs.namespace.sub, True)

    def test_raise_in_define(self):
        """Process which raises in its 'define' method. Check that the spec is not set."""

        class BrokenProcess(Process):
            @classmethod
            def define(cls, spec):
                super(BrokenProcess, cls).define(spec)
                raise ValueError

        with self.assertRaises(ValueError):
            BrokenProcess.spec()
        # Check that the error is still raised when calling .spec()
        # a second time.
        with self.assertRaises(ValueError):
            BrokenProcess.spec()

    def test_execute(self):
        proc = test_utils.DummyProcessWithOutput()
        proc.execute()

        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)
        self.assertEqual(proc.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method
        proc = test_utils.DummyProcessWithOutput()
        proc.execute()
        results = proc.outputs
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        for event in ('create', 'run', 'finish'):
            with self.assertRaises(AssertionError):
                proc = ForgetToCallParent(event)
                proc.execute()

    def test_forget_to_call_parent_kill(self):
        with self.assertRaises(AssertionError):
            proc = ForgetToCallParent('kill')
            proc.kill()
            proc.execute()

    def test_pid(self):
        # Test auto generation of pid
        process = test_utils.DummyProcessWithOutput()
        self.assertIsNotNone(process.pid)

        # Test using integer as pid
        process = test_utils.DummyProcessWithOutput(pid=5)
        self.assertEqual(process.pid, 5)

        # Test using string as pid
        process = test_utils.DummyProcessWithOutput(pid='a')
        self.assertEqual(process.pid, 'a')

    def test_exception(self):
        proc = test_utils.ExceptionProcess()
        with self.assertRaises(RuntimeError):
            proc.execute()
        self.assertEqual(proc.state, ProcessState.EXCEPTED)

    def test_get_description(self):

        class ProcWithoutSpec(Process):
            pass

        class ProcWithSpec(Process):
            """ Process with a spec and a docstring """

            @classmethod
            def define(cls, spec):
                super(ProcWithSpec, cls).define(spec)
                spec.input('a', default=1)

        for proc_class in test_utils.TEST_PROCESSES:
            desc = proc_class.get_description()
            self.assertIsInstance(desc, dict)

        desc_with_spec = ProcWithSpec.get_description()
        desc_without_spec = ProcWithoutSpec.get_description()

        self.assertIsInstance(desc_without_spec, dict)
        self.assertTrue('spec' in desc_without_spec)
        self.assertTrue('description' not in desc_without_spec)
        self.assertIsInstance(desc_with_spec['spec'], dict)

        self.assertIsInstance(desc_with_spec, dict)
        self.assertTrue('spec' in desc_with_spec)
        self.assertTrue('description' in desc_with_spec)
        self.assertIsInstance(desc_with_spec['spec'], dict)
        self.assertIsInstance(desc_with_spec['description'], str)

    def test_logging(self):

        class LoggerTester(Process):

            def run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        proc = LoggerTester()
        proc.execute()

    def test_kill(self):
        proc = test_utils.DummyProcess()

        proc.kill('Farewell!')
        self.assertTrue(proc.killed())
        self.assertEqual(proc.killed_msg(), 'Farewell!')
        self.assertEqual(proc.state, ProcessState.KILLED)

    @pytest.mark.asyncio
    async def test_wait_continue(self):
        proc = test_utils.WaitForSignalProcess()
        # Wait - Execute the process and wait until it is waiting

        listener = plumpy.ProcessListener()
        listener.on_process_waiting = lambda proc: proc.resume()

        proc.add_process_listener(listener)
        await proc.step_until_terminated()

        # Check it's done
        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)

    def test_exc_info(self):
        proc = test_utils.ExceptionProcess()
        try:
            proc.execute()
        except RuntimeError as e:
            self.assertEqual(proc.exception(), e)

    def test_run_done(self):
        proc = test_utils.DummyProcess()
        proc.execute()
        self.assertTrue(proc.done())

    @pytest.mark.asyncio
    async def test_wait_pause_play_resume(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = test_utils.WaitForSignalProcess()
        asyncio.ensure_future(proc.step_until_terminated())

        await test_utils.run_until_waiting(proc)
        self.assertEqual(proc.state, ProcessState.WAITING)

        result = await proc.pause()
        self.assertTrue(result)
        self.assertTrue(proc.paused)

        result = proc.play()
        self.assertTrue(result)
        self.assertFalse(proc.paused)

        proc.resume()
        # Wait until the process is terminated
        await proc.future()

        # Check it's done
        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)

    @pytest.mark.asyncio
    async def test_pause_play_status_messaging(self):
        """
        Test the setting of a processes' status through pause and play works correctly.

        Any process can have its status set to a given message. When pausing, a pause message can be set for the
        status, which should store the current status, which should be restored, once the process is played again.
        """
        PLAY_STATUS = 'process was played by Hans Klok'
        PAUSE_STATUS = 'process was paused by Evel Knievel'

        proc = test_utils.WaitForSignalProcess()
        proc.set_status(PLAY_STATUS)
        asyncio.ensure_future(proc.step_until_terminated())

        await test_utils.run_until_waiting(proc)
        self.assertEqual(proc.state, ProcessState.WAITING)

        result = await proc.pause(PAUSE_STATUS)
        self.assertTrue(result)
        self.assertTrue(proc.paused)
        self.assertEqual(proc.status, PAUSE_STATUS)

        result = proc.play()
        self.assertEqual(proc.status, PLAY_STATUS)
        self.assertIsNone(proc._pre_paused_status)

        proc.resume()
        # Wait until the process is terminated
        await proc.future()

        # Check it's done
        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)

    def test_kill_in_run(self):

        class KillProcess(Process):
            after_kill = False

            def run(self, **kwargs):
                self.kill()
                # The following line should be executed because kill will not
                # interrupt execution of a method call in the RUNNING state
                self.after_kill = True

        proc = KillProcess()
        with self.assertRaises(plumpy.KilledError):
            proc.execute()

        self.assertTrue(proc.after_kill)
        self.assertEqual(proc.state, ProcessState.KILLED)

    def test_kill_when_paused_in_run(self):

        class PauseProcess(Process):

            def run(self, **kwargs):
                self.pause()
                self.kill()

        proc = PauseProcess()
        with self.assertRaises(plumpy.KilledError):
            proc.execute()

        self.assertEqual(proc.state, ProcessState.KILLED)

    @pytest.mark.asyncio
    async def test_kill_when_paused(self):
        proc = test_utils.WaitForSignalProcess()

        asyncio.ensure_future(proc.step_until_terminated())
        await test_utils.run_until_waiting(proc)

        saved_state = plumpy.Bundle(proc)

        result = await proc.pause()
        self.assertTrue(result)
        self.assertTrue(proc.paused)

        # Kill the process
        proc.kill()

        with self.assertRaises(plumpy.KilledError):
            result = await proc.future()

        self.assertEqual(proc.state, ProcessState.KILLED)

    @pytest.mark.asyncio
    async def test_run_multiple(self):
        # Create and play some processes
        loop = asyncio.get_event_loop()

        procs = []
        for proc_class in test_utils.TEST_PROCESSES:
            proc = proc_class(loop=loop)
            procs.append(proc)

        l = await asyncio.gather(*[p.step_until_terminated() for p in procs])
        futures = await asyncio.gather(*[p.future() for p in procs])

        for future, proc_class in zip(futures, test_utils.TEST_PROCESSES):
            self.assertDictEqual(proc_class.EXPECTED_OUTPUTS, future)

    def test_invalid_output(self):

        class InvalidOutput(plumpy.Process):

            def run(self):
                self.out("invalid", 5)

        proc = InvalidOutput()
        with self.assertRaises(ValueError):
            proc.execute()

    def test_missing_output(self):
        proc = test_utils.MissingOutputProcess()

        with self.assertRaises(plumpy.InvalidStateError):
            proc.successful()

        proc.execute()

        self.assertFalse(proc.successful())

    def test_unsuccessful_result(self):
        ERROR_CODE = 256

        class Proc(Process):

            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)

            def run(self):
                return plumpy.UnsuccessfulResult(ERROR_CODE)

        proc = Proc()
        proc.execute()

        self.assertEqual(proc.result(), ERROR_CODE)

    def test_pause_in_process(self):
        """ Test that we can pause and cancel that by playing within the process """

        test_case = self
        ioloop = asyncio.get_event_loop()

        class TestPausePlay(plumpy.Process):

            def run(self):
                fut = self.pause()
                test_case.assertIsInstance(fut, plumpy.Future)

        listener = plumpy.ProcessListener()
        listener.on_process_paused = lambda _proc: ioloop.stop()

        proc = TestPausePlay()
        proc.add_process_listener(listener)

        asyncio.ensure_future(proc.step_until_terminated())
        ioloop.run_forever()
        self.assertTrue(proc.paused)
        self.assertEqual(plumpy.ProcessState.FINISHED, proc.state)

    @pytest.mark.asyncio
    async def test_pause_play_in_process(self):
        """ Test that we can pause and play that by playing within the process """

        test_case = self

        class TestPausePlay(plumpy.Process):

            def run(self):
                fut = self.pause()
                test_case.assertIsInstance(fut, plumpy.Future)
                result = self.play()
                test_case.assertTrue(result)

        proc = TestPausePlay()

        # asyncio.ensure_future(proc.step_until_terminated())
        await proc.step_until_terminated()
        self.assertFalse(proc.paused)
        self.assertEqual(plumpy.ProcessState.FINISHED, proc.state)

    def test_process_stack(self):
        test_case = self

        class StackTest(plumpy.Process):

            def run(self):
                test_case.assertIs(self, Process.current())

        proc = StackTest()
        proc.execute()

    @pytest.mark.asyncio
    async def test_process_stack_multiple(self):
        """
        Run multiple and nested processes to make sure the process stack is always correct
        """
        test_case = self

        def test_nested(process):
            test_case.assertIs(process, Process.current())

        class StackTest(plumpy.Process):

            def run(self):
                test_case.assertIs(self, Process.current())
                test_nested(self)

        class ParentProcess(plumpy.Process):

            def run(self):
                test_case.assertIs(self, Process.current())
                StackTest().execute()

        to_run = []
        for _ in range(100):
            to_run.append(ParentProcess().step_until_terminated())

        await asyncio.gather(*to_run)

    def test_call_soon(self):

        class CallSoon(plumpy.Process):

            def run(self):
                self.call_soon(self.do_except)

            def do_except(self):
                raise RuntimeError("Breaking yo!")

        CallSoon().execute()
        
    def test_execute_twice(self):
        """Test a process that is executed once finished raises a ClosedError"""
        proc = test_utils.DummyProcess()
        proc.execute()
        with self.assertRaises(plumpy.ClosedError):
            proc.execute()
