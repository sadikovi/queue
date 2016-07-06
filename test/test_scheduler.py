#!/usr/bin/env python

import multiprocessing
import Queue as threadqueue
import unittest
import mock
import src.scheduler as scheduler

class MessageSuite(unittest.TestCase):
    def test_init(self):
        msg = scheduler.Message("status", a=1, b="a")
        self.assertEqual(msg.status, "status")
        self.assertEqual(msg.arguments, {"a": 1, "b": "a"})

        msg = scheduler.Message("status")
        self.assertEqual(msg.status, "status")
        self.assertEqual(msg.arguments, {})

    def test_pretty_name(self):
        msg = scheduler.Message("status", a=1, b="a")
        pretty_name = u"Message[status]{'a': 1, 'b': 'a'}"
        self.assertEqual("%s" % msg, pretty_name)
        self.assertEqual(msg.__unicode__(), pretty_name)
        self.assertEqual(msg.__repr__(), pretty_name)

class TerminationExceptionSuite(unittest.TestCase):
    def test_init(self):
        try:
            raise scheduler.TerminationException()
        except scheduler.TerminationException as err:
            self.assertEqual("%s" % err, "Requested termination")

        try:
            raise scheduler.TerminationException(message="Test")
        except scheduler.TerminationException as err:
            self.assertEqual("%s" % err, "Test")

class TaskSuite(unittest.TestCase):
    def setUp(self):
        class Test(scheduler.Task):
            pass
        self.task = Test()

    def test_uid(self):
        with self.assertRaises(NotImplementedError):
            self.task.uid

    def test_exit_code(self):
        with self.assertRaises(NotImplementedError):
            self.task.exit_code

    def test_status(self):
        with self.assertRaises(NotImplementedError):
            self.task.status()

    def test_status_const(self):
        self.assertEqual(self.task.BLOCKED, "BLOCKED")
        self.assertEqual(self.task.PENDING, "PENDING")
        self.assertEqual(self.task.RUNNING, "RUNNING")
        self.assertEqual(self.task.FINISHED, "FINISHED")

    def test_async_launch(self):
        with self.assertRaises(NotImplementedError):
            self.task.async_launch()

    def test_async_cancel(self):
        with self.assertRaises(NotImplementedError):
            self.task.cancel()

class ExecutorSuite(unittest.TestCase):
    def setUp(self):
        self.queue = mock.create_autospec(multiprocessing).Queue()
        # main_conn and conn are obtained through Pipe() creation
        self.main_conn = mock.Mock()
        self.conn = mock.Mock()
        self.mock_task = mock.create_autospec(scheduler).Task()

    def test_init_1(self):
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=True)
        self.assertEqual(ex.name, "name")
        self.assertEqual(ex.conn, self.conn)
        self.assertEqual(ex.task_queue, self.queue)
        self.assertEqual(ex.timeout, 1)
        self.assertEqual(ex.logger, True)

    def test_init_2(self):
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=10, logger=None)
        self.assertEqual(ex.name, "name")
        self.assertEqual(ex.conn, self.conn)
        self.assertEqual(ex.task_queue, self.queue)
        self.assertEqual(ex.timeout, 10)
        self.assertNotEqual(ex.logger, None)

    def test_process_message(self):
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        # should process None message
        ex._process_message(None)
        ex._process_message("SHUTDOWN")
        with self.assertRaises(scheduler.TerminationException):
            ex._process_message(scheduler.Message(scheduler.MESSAGE_SHUTDOWN))

    def test_process_task_1(self):
        self.queue.get.return_value = self.mock_task
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        # test when there is no active task
        ex.active_task = None
        self.assertEqual(ex._process_task(), None)
        self.queue.get.assert_called_once_with(block=False)
        self.assertEqual(ex.active_task, self.mock_task)

    def test_process_task_2(self):
        self.queue.get.side_effect = threadqueue.Empty()
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        # test when there is no active task
        ex.active_task = None
        self.assertEqual(ex._process_task(), None)
        self.assertEqual(ex.active_task, None)

    def test_process_task_blocked(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.BLOCKED
        # test when active task is blocked
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        ex.active_task = self.mock_task
        self.assertEqual(ex._process_task(), None)
        self.assertNotEqual(ex.active_task, None)

    def test_process_task_pending(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.PENDING
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        ex.active_task = self.mock_task
        self.assertEqual(ex._process_task(), None)
        self.assertNotEqual(ex.active_task, None)
        self.mock_task.async_launch.assert_called_once_with()
        self.assertEqual(len(self.conn.send.call_args_list), 1)
        conn_msg = "call(Message[TASK_STARTED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_process_task_running(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.RUNNING
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        ex.active_task = self.mock_task
        self.assertEqual(ex._process_task(), None)

    def test_process_task_finished(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.FINISHED
        self.mock_task.exit_code = 1
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        ex.active_task = self.mock_task
        self.assertEqual(ex._process_task(), 1)
        self.assertEqual(ex.active_task, None)
        conn_msg = "call(Message[TASK_FINISHED]{'exit_code': 1, 'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_cancel_task(self):
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=1, logger=mock.Mock())
        # test when no active task
        self.assertEqual(ex.active_task, None)
        ex._cancel_task()
        self.assertEqual(ex.active_task, None)
        # test when active task exists
        self.mock_task.uid = "123"
        ex.active_task = self.mock_task
        ex._cancel_task()
        self.assertEqual(ex.active_task, None)
        self.mock_task.cancel.assert_called_once_with()
        conn_msg = "call(Message[TASK_KILLED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_iteration_executor_terminated(self):
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=0.5, logger=mock.Mock())
        ex._terminated = True
        self.assertEqual(ex.iteration(), False)

    def test_iteration_valid_task_poll(self):
        self.conn.poll.return_value = True
        self.conn.recv.return_value = 123
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=0.5, logger=mock.Mock())
        ex._process_message = mock.create_autospec(ex._process_message)
        ex._process_task = mock.create_autospec(ex._process_task)
        self.assertEqual(ex.iteration(), True)
        ex._process_message.assert_called_once_with(123)
        ex._process_task.assert_called_once_with()

    def test_iteration_valid_task_no_poll(self):
        self.conn.poll.return_value = False
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=0.5, logger=mock.Mock())
        ex._process_message = mock.create_autospec(ex._process_message)
        ex._process_task = mock.create_autospec(ex._process_task)
        self.assertEqual(ex.iteration(), True)
        self.assertEqual(ex._process_message.call_count, 0)
        ex._process_task.assert_called_once_with()

    def test_iteration_terminate(self):
        self.conn.poll.return_value = True
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=0.5, logger=mock.Mock())
        ex._process_message = mock.create_autospec(ex._process_message)
        ex._process_message.side_effect = scheduler.TerminationException()
        ex._process_task = mock.create_autospec(ex._process_task)
        ex._cancel_task = mock.create_autospec(ex._cancel_task)
        self.assertEqual(ex.iteration(), False)
        self.assertEqual(ex._terminated, True)
        self.assertEqual(ex._process_task.call_count, 0)
        ex._cancel_task.assert_called_once_with()

    def test_iteration_terminate_global(self):
        self.conn.poll.return_value = True
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=0.5, logger=mock.Mock())
        ex._process_message = mock.create_autospec(ex._process_message)
        ex._process_message.side_effect = StandardError()
        self.assertEqual(ex.iteration(), False)

    @mock.patch("src.scheduler.time")
    def test_run(self, mock_time):
        # test time.sleep, if iteration returns True, `time.sleep` is called
        ex = scheduler.Executor("name", self.conn, self.queue, timeout=0.5, logger=mock.Mock())
        ex.iteration = mock.create_autospec(ex.iteration, return_value=False)
        self.assertEqual(ex.run(), False)
        self.assertEqual(mock_time.sleep.call_count, 0)

# Load test suites
def suites():
    return [
        MessageSuite,
        TerminationExceptionSuite,
        TaskSuite,
        ExecutorSuite
    ]
