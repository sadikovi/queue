#!/usr/bin/env python

import multiprocessing
import Queue as threadqueue
import threading
import unittest
import mock
import src.const as const
import src.scheduler as scheduler

# Test of miscellaneous methods and constants
class SchedulerModuleSuite(unittest.TestCase):
    def test_task_statuses(self):
        self.assertEqual(scheduler.TASK_PENDING, "PENDING")
        self.assertEqual(scheduler.TASK_STARTED, "STARTED")
        self.assertEqual(scheduler.TASK_CANCELLED, "CANCELLED")
        self.assertEqual(scheduler.TASK_SUCCEEDED, "SUCCEEDED")
        self.assertEqual(scheduler.TASK_FAILED, "FAILED")

    def test_message_statuses(self):
        self.assertEqual(scheduler.EXECUTOR_SHUTDOWN, "EXECUTOR_SHUTDOWN")
        self.assertEqual(scheduler.EXECUTOR_CANCEL_TASK, "EXECUTOR_CANCEL_TASK")
        self.assertEqual(scheduler.EXECUTOR_TASK_STARTED, "EXECUTOR_TASK_STARTED")
        self.assertEqual(scheduler.EXECUTOR_TASK_SUCCEEDED, "EXECUTOR_TASK_SUCCEEDED")
        self.assertEqual(scheduler.EXECUTOR_TASK_FAILED, "EXECUTOR_TASK_FAILED")
        self.assertEqual(scheduler.EXECUTOR_TASK_CANCELLED, "EXECUTOR_TASK_CANCELLED")

class TaskSuite(unittest.TestCase):
    def setUp(self):
        self.task = scheduler.Task()

    def test_uid(self):
        with self.assertRaises(NotImplementedError):
            assert self.task.uid

    def test_priority(self):
        with self.assertRaises(NotImplementedError):
            assert self.task.priority

    def test_run(self):
        with self.assertRaises(NotImplementedError):
            self.task.run()

    def test_cancel(self):
        with self.assertRaises(NotImplementedError):
            self.task.cancel()

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

# pylint: disable=W0212,protected-access
class WorkerThreadSuite(unittest.TestCase):
    def test_init_1(self):
        thread = scheduler.WorkerThread(None, None)
        self.assertTrue(isinstance(thread, threading.Thread))
        self.assertEqual(thread._task, None)
        self.assertEqual(thread._msg_queue, None)
        self.assertEqual(thread.daemon, False)

    def test_init_2(self):
        task = mock.create_autospec(scheduler.Task)
        msg_queue = mock.Mock()
        thread = scheduler.WorkerThread(task, msg_queue)
        self.assertTrue(isinstance(thread, threading.Thread))
        self.assertEqual(thread._task, task)
        self.assertEqual(thread._msg_queue, msg_queue)
        self.assertEqual(thread.daemon, False)

    def test_run_ok(self):
        task = mock.create_autospec(scheduler.Task)
        thread = scheduler.WorkerThread(task, mock.Mock())
        thread.run()
        task.run.assert_called_once_with()

    def test_run_error(self):
        task = mock.create_autospec(scheduler.Task)
        expected_error = StandardError("Test")
        task.run.side_effect = expected_error
        msg_queue = mock.Mock()
        # create thread and check that exception was thrown
        thread = scheduler.WorkerThread(task, msg_queue)
        thread.run()
        task.run.assert_called_once_with()
        msg_queue.put_nowait.assert_called_once_with(expected_error)

    def test_cancel(self):
        task = mock.create_autospec(scheduler.Task)
        thread = scheduler.WorkerThread(task, mock.Mock())
        thread.cancel()
        task.cancel.assert_called_once_with()
# pylint: enable=W0212,protected-access

# pylint: disable=W0212,protected-access
class TaskThreadSuite(unittest.TestCase):
    def setUp(self):
        self.task = mock.create_autospec(scheduler.Task, uid=123)
        self.logger = mock.Mock()

    def test_init_ok(self):
        thread = scheduler.TaskThread(self.task, self.logger)
        self.assertEqual(thread.daemon, True)
        self.assertEqual(thread.refresh_timeout, 0.5)
        self.assertEqual(thread.uid, 123)
        self.assertEqual(thread.status, scheduler.TASK_PENDING)
        self.assertEqual(thread.name, "Task[123]")
        self.assertEqual(thread.on_task_started, None)
        self.assertEqual(thread.on_task_cancelled, None)
        self.assertEqual(thread.on_task_succeeded, None)
        self.assertEqual(thread.on_task_failed, None)

    def test_init_error(self):
        with self.assertRaises(AttributeError):
            scheduler.TaskThread(mock.Mock(), mock.Mock())
        with self.assertRaises(AttributeError):
            scheduler.TaskThread(None, mock.Mock())

    def test_set_metric(self):
        thread = scheduler.TaskThread(self.task, self.logger)
        thread._set_metric("a", 1)
        thread._set_metric("b", None)
        self.assertEqual(thread._get_metric("a"), 1)
        self.assertEqual(thread._get_metric("b"), None)
        # reset value for name
        thread._set_metric("a", 2)
        self.assertEqual(thread._get_metric("a"), 2)

    def test_get_metric(self):
        thread = scheduler.TaskThread(self.task, self.logger)
        thread._set_metric("a", 1)
        self.assertEqual(thread._get_metric("a"), 1)
        self.assertEqual(thread._get_metric("b"), None)

    def test_cancel(self):
        thread = scheduler.TaskThread(self.task, self.logger)
        self.assertEqual(thread.is_cancelled, False)
        thread.cancel()
        self.assertEqual(thread.is_cancelled, True)

    def test_safe_exec(self):
        thread = scheduler.TaskThread(self.task, self.logger)
        thread._safe_exec(None)
        self.assertEqual(self.logger.debug.call_count, 0)
        # function that does not raise error
        ok_func = mock.Mock()
        thread._safe_exec(ok_func, a=1, b=2)
        ok_func.assert_called_once_with(a=1, b=2)
        # function that raises error
        err_func = mock.Mock()
        err_func.side_effect = StandardError("Test")
        thread._safe_exec(err_func, a=1, b=2)
        err_func.assert_called_once_with(a=1, b=2)

    @mock.patch("src.scheduler.time")
    def test_run_cancel(self, mock_time):
        mock_time.sleep.return_value = None
        thread = scheduler.TaskThread(self.task, self.logger)
        thread.cancel()
        thread.run()
        self.assertEqual(thread.status, scheduler.TASK_CANCELLED)
        self.assertNotEqual(thread._get_metric("starttime"), None)
        self.assertNotEqual(thread._get_metric("endtime"), None)
        self.assertNotEqual(thread._get_metric("duration"), None)

    @mock.patch("src.scheduler.time")
    def test_run_success(self, mock_time):
        mock_time.sleep.return_value = None
        thread = scheduler.TaskThread(self.task, self.logger)
        thread.run()
        self.assertEqual(thread.status, scheduler.TASK_SUCCEEDED)
        self.assertNotEqual(thread._get_metric("starttime"), None)
        self.assertNotEqual(thread._get_metric("endtime"), None)
        self.assertNotEqual(thread._get_metric("duration"), None)

    @mock.patch("src.scheduler.time")
    def test_run_failure(self, mock_time):
        mock_time.sleep.return_value = None
        self.task.run.side_effect = StandardError("Test")
        thread = scheduler.TaskThread(self.task, self.logger)
        thread.run()
        self.assertEqual(thread.status, scheduler.TASK_FAILED)
        self.assertNotEqual(thread._get_metric("starttime"), None)
        self.assertNotEqual(thread._get_metric("endtime"), None)
        self.assertNotEqual(thread._get_metric("duration"), None)
# pylint: enable=W0212,protected-access

# pylint: disable=W0212,protected-access
class ExecutorSuite(unittest.TestCase):
    def setUp(self):
        self.queue_map = {
            const.PRIORITY_0: mock.create_autospec(multiprocessing).Queue(),
            const.PRIORITY_1: mock.create_autospec(multiprocessing).Queue(),
            const.PRIORITY_2: mock.create_autospec(multiprocessing).Queue()
        }
        # main_conn and conn are obtained through Pipe() creation
        self.main_conn = mock.Mock()
        self.conn = mock.Mock()
        self.mock_task = mock.create_autospec(scheduler.Task)
        self.logger = mock.Mock()

    def test_init_1(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=self.logger)
        self.assertEqual(exc.name, "Executor[name]")
        self.assertEqual(exc.conn, self.conn)
        self.assertEqual(exc.task_queue_map, self.queue_map)
        self.assertEqual(exc.timeout, 1)
        self.assertEqual(exc._cancel_task_ids, set())
        self.assertNotEqual(exc.logger, None)

    def test_init_2(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=10, logger=None)
        self.assertEqual(exc.name, "Executor[name]")
        self.assertEqual(exc.conn, self.conn)
        self.assertEqual(exc.task_queue_map, self.queue_map)
        self.assertEqual(exc.timeout, 10)
        self.assertEqual(exc._cancel_task_ids, set())
        self.assertNotEqual(exc.logger, None)

    def test_process_message_invalid(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._process_message(None)
        exc.logger.info.assert_called_once_with("Invalid message %s is ignored", None)

    def test_process_message_shutdown(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        msg_mock = mock.create_autospec(scheduler.Message, status=scheduler.EXECUTOR_SHUTDOWN)
        with self.assertRaises(scheduler.ExecutorInterruptedException):
            exc._process_message(msg_mock)

    def test_process_message_cancel_task(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        msg_mock = mock.create_autospec(scheduler.Message, status=scheduler.EXECUTOR_CANCEL_TASK,
                                        arguments={"task_id": "123"})
        exc._process_message(msg_mock)
        self.assertTrue("123" in exc._cancel_task_ids)
        exc.logger.debug.assert_called_with("Registered cancelled task %s", "123")

    def test_get_new_task_empty_map(self):
        exc = scheduler.Executor("a", self.conn, {}, timeout=1, logger=self.logger)
        self.assertEqual(exc._get_new_task(), None)

    def test_get_new_task_empty_queue(self):
        # prepare mock queues
        for queue in self.queue_map.values():
            queue.get.side_effect = threadqueue.Empty()
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        self.assertEqual(exc._get_new_task(), None)

    def test_get_new_task_priority2_queue(self):
        # prepare mock queues
        self.queue_map[const.PRIORITY_0].get.side_effect = threadqueue.Empty()
        self.queue_map[const.PRIORITY_1].get.side_effect = threadqueue.Empty()
        self.queue_map[const.PRIORITY_2].get.return_value = self.mock_task
        # should select priority 2
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        thread = exc._get_new_task()
        self.assertTrue(isinstance(thread, scheduler.TaskThread))

    def test_process_task_empty(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        mock_task_thread = mock.Mock()
        exc._get_new_task = mock.Mock()
        exc._get_new_task.return_value = mock_task_thread
        exc._process_task()
        self.assertEqual(exc._active_task, mock_task_thread)

    def test_process_task_cancel(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        mock_task_thread = mock.Mock()
        mock_task_thread.uid = "123"
        exc._get_new_task = mock.Mock()
        exc._get_new_task.return_value = mock_task_thread
        exc._cancel_active_task = mock.Mock()
        exc._cancel_task_ids.add("123")
        exc._process_task()
        self.assertFalse("123" in exc._cancel_task_ids)
        exc._cancel_active_task.assert_called_once_with()

    def test_process_task_pending(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._active_task = mock.create_autospec(scheduler.TaskThread, uid="123",
                                                status=scheduler.TASK_PENDING)
        exc._process_task()
        exc._active_task.start.assert_called_once_with()
        self.assertEqual(len(self.conn.send.call_args_list), 1)
        conn_msg = "call(Message[EXECUTOR_TASK_STARTED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_process_task_pending_not_available(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._active_task = mock.create_autospec(scheduler.TaskThread, uid="123",
                                                status=scheduler.TASK_PENDING)
        exc.external_system_available = mock.Mock()
        exc.external_system_available.return_value = False
        exc._process_task()
        self.assertEqual(exc._active_task.start.call_count, 0)
        self.assertEqual(self.conn.send.call_count, 0)

    def test_process_task_started(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._active_task = mock.create_autospec(scheduler.TaskThread, uid="123", is_alive=True,
                                                status=scheduler.TASK_STARTED)
        exc._process_task()
        exc.logger.debug.assert_called_with("Ping task %s is alive", "123")

    def test_process_task_succeeded(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._active_task = mock.create_autospec(scheduler.TaskThread, uid="123",
                                                status=scheduler.TASK_SUCCEEDED)
        exc._process_task()
        self.assertEqual(len(self.conn.send.call_args_list), 1)
        conn_msg = "call(Message[EXECUTOR_TASK_SUCCEEDED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)
        self.assertEqual(exc._active_task, None)

    def test_process_task_failed(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._active_task = mock.create_autospec(scheduler.TaskThread, uid="123",
                                                status=scheduler.TASK_FAILED)
        exc._process_task()
        self.assertEqual(len(self.conn.send.call_args_list), 1)
        conn_msg = "call(Message[EXECUTOR_TASK_FAILED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)
        self.assertEqual(exc._active_task, None)

    def test_process_task_cancelled(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._active_task = mock.create_autospec(scheduler.TaskThread, uid="123",
                                                status=scheduler.TASK_CANCELLED)
        exc._process_task()
        self.assertEqual(exc._active_task, None)

    def test_process_task_no_tasks(self):
        exc = scheduler.Executor("a", self.conn, {}, timeout=1, logger=self.logger)
        exc._active_task = None
        exc._process_task()
        exc.logger.debug.assert_called_with("No active task registered")

    def test_cancel_active_task_1(self):
        exc = scheduler.Executor("a", self.conn, {}, timeout=1, logger=self.logger)
        exc._active_task = None
        exc._cancel_active_task()
        exc.logger.info.assert_called_once_with("No active task to cancel")

    def test_cancel_active_task_2(self):
        exc = scheduler.Executor("a", self.conn, {}, timeout=1, logger=self.logger)
        mock_task = mock.create_autospec(scheduler.TaskThread, uid="123",
                                         status=scheduler.TASK_STARTED)
        exc._active_task = mock_task
        exc._cancel_active_task()
        self.assertEqual(exc._active_task, None)
        mock_task.cancel.assert_called_once_with()
        self.assertEqual(len(self.conn.send.call_args_list), 1)
        conn_msg = "call(Message[EXECUTOR_TASK_CANCELLED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_iteration_executor_terminated(self):
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._terminated = True
        self.assertEqual(exc.iteration(), False)

    def test_iteration_valid_task_poll(self):
        self.conn.poll.return_value = True
        self.conn.recv.return_value = 123
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_task = mock.create_autospec(exc._process_task)
        self.assertEqual(exc.iteration(), True)
        exc._process_message.assert_called_once_with(123)
        exc._process_task.assert_called_once_with()

    def test_iteration_valid_task_no_poll(self):
        self.conn.poll.return_value = False
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_task = mock.create_autospec(exc._process_task)
        self.assertEqual(exc.iteration(), True)
        self.assertEqual(exc._process_message.call_count, 0)
        exc._process_task.assert_called_once_with()

    def test_iteration_terminate(self):
        self.conn.poll.return_value = True
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_message.side_effect = scheduler.ExecutorInterruptedException()
        exc._process_task = mock.create_autospec(exc._process_task)
        exc._cancel_active_task = mock.create_autospec(exc._cancel_active_task)
        self.assertEqual(exc.iteration(), False)
        self.assertEqual(exc._terminated, True)
        self.assertEqual(exc._process_task.call_count, 0)
        exc._cancel_active_task.assert_called_once_with()

    def test_iteration_terminate_global(self):
        self.conn.poll.return_value = True
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_message.side_effect = StandardError()
        self.assertEqual(exc.iteration(), False)

    @mock.patch("src.scheduler.time")
    def test_run(self, mock_time):
        # test time.sleep, if iteration returns True, `time.sleep` is called
        exc = scheduler.Executor("a", self.conn, self.queue_map, timeout=1, logger=self.logger)
        exc.iteration = mock.create_autospec(exc.iteration, return_value=False)
        self.assertEqual(exc.run(), False)
        self.assertEqual(mock_time.sleep.call_count, 0)
        # test when iteration returns True, and then False
        exc.iteration = mock.Mock()
        exc.iteration.side_effect = [True, True, False]
        self.assertEqual(exc.run(), False)
        self.assertEqual(mock_time.sleep.call_count, 2)
# pylint: enable=W0212,protected-access

# pylint: disable=W0212,protected-access
class SchedulerSuite(unittest.TestCase):
    def setUp(self):
        self.logger = mock.Mock()
        self.task = mock.create_autospec(scheduler.Task, uid="123")

    def test_init_1(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        self.assertEqual(sched.num_executors, 3)
        self.assertEqual(sched.timeout, 0.5)
        self.assertEqual(sched.on_task_started, None)
        self.assertEqual(sched.on_task_cancelled, None)
        self.assertEqual(sched.on_task_succeeded, None)
        self.assertEqual(sched.on_task_failed, None)

    def test_init_2(self):
        with self.assertRaises(ValueError):
            scheduler.Scheduler("abc", 0.5, self.logger)

    def test_get_metric(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        self.assertEqual(sched._get_metric("a"), None)

    def test_set_metric(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        sched._set_metric("a", 1)
        self.assertEqual(sched._get_metric("a"), 1)
        sched._set_metric("a", 2)
        self.assertEqual(sched._get_metric("a"), 2)

    def test_increment_metric_nonexistent(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        sched._increment_metric("a")
        self.assertEqual(sched._get_metric("a"), None)

    def test_increment_metric_convert_error(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        sched._set_metric("a", "b")
        sched._increment_metric("a")
        self.assertEqual(sched._get_metric("a"), 0)

    def test_increment_metric_correct(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        sched._set_metric("a", "1")
        sched._increment_metric("a")
        self.assertEqual(sched._get_metric("a"), 2)
        sched._increment_metric("a")
        self.assertEqual(sched._get_metric("a"), 3)

    def test_get_metrics(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        sched._set_metric("a", "1")
        sched._set_metric("b", 1)
        self.assertEqual(sched.get_metrics(), {"a": "1", "b": 1})

    def test_prepare_executor(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        # prepare mock executor
        exc = sched._prepare_executor("test")
        # assertions
        self.assertTrue(isinstance(exc, scheduler.Executor))
        self.assertEqual(exc.daemon, True)
        self.assertEqual(exc.name, "Executor[test]")
        self.assertEqual(sched.executors, [exc])
        self.assertTrue(exc.name in sched.pipe)

    def test_prepare_executor_wrong_type(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        sched.executor_class = mock.Mock()
        # prepare mock executor
        with self.assertRaises(TypeError):
            sched._prepare_executor("test")

    def test_start(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        mock_exc = mock.Mock()
        sched._prepare_executor = mock.Mock()
        sched._prepare_executor.return_value = mock_exc
        # launch executor
        sched.start()
        # assertions, check that prepare_executor was called number of executors times
        self.assertEqual(sched._prepare_executor.call_count, 3)
        self.assertEqual(mock_exc.start.call_count, 3)

    @mock.patch("src.scheduler.time")
    def test_stop(self, mock_time):
        mock_time.sleep.return_value = None
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        mock_conn = mock.Mock()
        sched.pipe = {"executor_name": mock_conn}
        mock_exc_1 = mock.Mock()
        mock_exc_2 = mock.Mock()
        mock_exc_2.is_alive.return_value = False
        sched.executors = [mock_exc_1, mock_exc_2]
        # stop scheduler
        sched.stop()
        # assertions
        self.assertEqual(sched.pipe, None)
        self.assertEqual(sched.executors, None)
        self.assertEqual(sched.task_queue_map, None)
        self.assertEqual(mock_conn.send.call_count, 1)
        # verify termination of executors
        mock_exc_1.is_alive.assert_called_once_with()
        mock_exc_1.terminate.assert_called_once_with()
        mock_exc_1.join.assert_called_once_with()
        mock_exc_2.is_alive.assert_called_once_with()
        self.assertEqual(mock_exc_2.terminate.call_count, 0)
        mock_exc_2.join.assert_called_once_with()

    def test_submit_wrong_type(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        with self.assertRaises(TypeError):
            sched.submit(mock.Mock())

    def test_submit_wrong_priority(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        self.task.priority = "abc"
        with self.assertRaises(KeyError):
            sched.submit(self.task)

    def test_submit_correct(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        self.task.priority = const.PRIORITY_0
        sched.task_queue_map = {
            const.PRIORITY_0: mock.Mock(),
            const.PRIORITY_1: mock.Mock(),
            const.PRIORITY_2: mock.Mock()
        }
        sched.submit(self.task)
        # assertions
        sched.task_queue_map[const.PRIORITY_0].put_nowait.assert_called_once_with(self.task)
        self.assertEqual(sched.task_queue_map[const.PRIORITY_1].put_nowait.call_count, 0)
        self.assertEqual(sched.task_queue_map[const.PRIORITY_2].put_nowait.call_count, 0)

    def test_cancel(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        # no task_id provided -> no-op
        sched.pipe = mock.Mock()
        sched.cancel(None)
        self.assertEqual(sched.pipe.values.call_count, 0)
        # task_id provided, and connections are available
        mock_conn = mock.Mock()
        sched.pipe.values = mock.Mock()
        sched.pipe.values.return_value = [mock_conn]
        sched.cancel("123")
        sched.pipe.values.assert_called_once_with()
        conn_msg = "call(Message[EXECUTOR_CANCEL_TASK]{'task_id': '123'})"
        self.assertEqual(str(mock_conn.send.call_args_list[0]), conn_msg)

    def test_process_callback_1(self):
        sched = scheduler.Scheduler(2, 0.5, self.logger)
        sched.on_task_started = mock.Mock()
        sched.on_task_cancelled = mock.Mock()
        sched.on_task_succeeded = mock.Mock()
        sched.on_task_failed = mock.Mock()
        # mock pipe + connection receiver
        msg1 = mock.create_autospec(scheduler.Message, status=scheduler.EXECUTOR_TASK_STARTED)
        msg2 = mock.create_autospec(scheduler.Message, status=scheduler.EXECUTOR_TASK_FAILED)
        msg3 = mock.create_autospec(scheduler.Message, status=scheduler.EXECUTOR_TASK_SUCCEEDED)
        msg4 = mock.create_autospec(scheduler.Message, status=scheduler.EXECUTOR_TASK_CANCELLED)
        conn1 = mock.Mock()
        conn1.poll.side_effect = [True, True, True, True, True, False]
        conn1.recv.side_effect = [msg1, msg1, msg2, msg3, msg4]
        sched.pipe = {"a": conn1}
        # process callback
        sched._process_callback()
        # check function calls
        sched.on_task_started.assert_called_once_with([msg1, msg1])
        sched.on_task_failed.assert_called_once_with([msg2])
        sched.on_task_succeeded.assert_called_once_with([msg3])
        sched.on_task_cancelled.assert_called_once_with([msg4])

    def test_process_callback_2(self):
        sched = scheduler.Scheduler(2, 0.5, self.logger)
        sched.on_task_started = mock.Mock()
        sched.on_task_cancelled = mock.Mock()
        sched.on_task_succeeded = mock.Mock()
        sched.on_task_failed = mock.Mock()
        # mock pipe + connection receiver
        msg = mock.Mock()
        conn1 = mock.Mock()
        conn1.poll.side_effect = [True, True, True, True, True, False]
        conn1.recv.side_effect = [msg, msg, msg, msg, msg]
        sched.pipe = {"a": conn1}
        # process callback
        sched._process_callback()
        # check function calls
        self.assertEqual(sched.on_task_started.call_count, 0)
        self.assertEqual(sched.on_task_failed.call_count, 0)
        self.assertEqual(sched.on_task_succeeded.call_count, 0)
        self.assertEqual(sched.on_task_cancelled.call_count, 0)

    def test_process_callback_3(self):
        sched = scheduler.Scheduler(2, 0.5, self.logger)
        sched.on_task_started = mock.Mock()
        sched.on_task_cancelled = mock.Mock()
        sched.on_task_succeeded = mock.Mock()
        sched.on_task_failed = mock.Mock()
        # mock pipe + connection receiver
        sched.pipe = None
        # process callback
        sched._process_callback()
        # check function calls
        self.assertEqual(sched.on_task_started.call_count, 0)
        self.assertEqual(sched.on_task_failed.call_count, 0)
        self.assertEqual(sched.on_task_succeeded.call_count, 0)
        self.assertEqual(sched.on_task_cancelled.call_count, 0)

    def test_prepare_polling_thread(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        # check polling thread without consumer
        thread = sched._prepare_polling_thread("test")
        self.assertNotEqual(thread, None)
        self.assertEqual(thread.daemon, True)
        self.assertEqual(thread.name, "test")

    def test_start_maintenance(self):
        sched = scheduler.Scheduler(3, 0.5, self.logger)
        sched._prepare_polling_thread = mock.Mock()
        mock_thread = mock.Mock()
        sched._prepare_polling_thread.return_value = mock_thread
        # launch maintenance should not result in any error
        sched.start_maintenance()
        mock_thread.start.assert_called_once_with()
        # launch maintenance when thread is None
        sched._prepare_polling_thread.return_value = None
        sched.start_maintenance()
# pylint: enable=W0212,protected-access

# Load test suites
def suites():
    return [
        SchedulerModuleSuite,
        TaskSuite,
        MessageSuite,
        WorkerThreadSuite,
        TaskThreadSuite,
        ExecutorSuite,
        SchedulerSuite
    ]
