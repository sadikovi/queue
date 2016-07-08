#!/usr/bin/env python

import multiprocessing
import Queue as threadqueue
import unittest
import mock
import src.const as const
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

# pylint: disable=W0223,W0231
class TestTask(scheduler.Task):
    pass
# pylint: enable=W0223,W0231

class TaskSuite(unittest.TestCase):
    def setUp(self):
        self.task = TestTask()

    def test_uid(self):
        with self.assertRaises(NotImplementedError):
            uid = self.task.uid
            raise ValueError("Not raise exception for %s" % uid)

    def test_exit_code(self):
        with self.assertRaises(NotImplementedError):
            exit_code = self.task.exit_code
            raise ValueError("Not raise exception for %s" % exit_code)

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
        self.mock_task = mock.create_autospec(scheduler).Task()

    def test_init_1(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=True)
        self.assertEqual(exc.name, "name")
        self.assertEqual(exc.conn, self.conn)
        self.assertEqual(exc.task_queue_map, self.queue_map)
        self.assertEqual(exc.timeout, 1)
        self.assertEqual(exc.logger, True)
        self.assertEqual(exc.cancel_task_ids, set())

    def test_init_2(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=10, logger=None)
        self.assertEqual(exc.name, "name")
        self.assertEqual(exc.conn, self.conn)
        self.assertEqual(exc.task_queue_map, self.queue_map)
        self.assertEqual(exc.timeout, 10)
        self.assertNotEqual(exc.logger, None)
        self.assertEqual(exc.cancel_task_ids, set())

    def test_process_message_invalid(self):
        mock_logger = mock.Mock()
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock_logger)
        # should process None message, but it should be no-op
        exc._process_message(None)
        mock_logger.info.assert_called_with("Invalid message %s is ignored", None)
        # should process arbitrary string message, but it should be no-op
        exc._process_message("SHUTDOWN")
        mock_logger.info.assert_called_with("Invalid message %s is ignored", "SHUTDOWN")

    def test_process_message_shutdown(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        with self.assertRaises(scheduler.TerminationException):
            exc._process_message(scheduler.Message(scheduler.MESSAGE_SHUTDOWN))

    def test_process_message_cancel(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        # cancel task without providing task id, should be no-op
        exc._process_message(scheduler.Message(scheduler.MESSAGE_TASK_CANCEL))
        self.assertEqual(exc.cancel_task_ids, set())
        # cancel task providing specific id
        msg = scheduler.Message(scheduler.MESSAGE_TASK_CANCEL, task_id="123")
        exc._process_message(msg)
        self.assertEqual(msg.status, scheduler.MESSAGE_TASK_CANCEL)
        self.assertEqual(msg.arguments["task_id"], "123")
        self.assertEqual(exc.cancel_task_ids, set(["123"]))

    def test_get_new_task_empty_map(self):
        exc = scheduler.Executor("name", self.conn, {}, timeout=1, logger=mock.Mock())
        self.assertEqual(exc._get_new_task(), None)

    def test_get_new_task_empty_queue(self):
        # prepare mock queues
        for queue in self.queue_map.values():
            queue.get.side_effect = threadqueue.Empty()
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        self.assertEqual(exc._get_new_task(), None)

    def test_get_new_task_priority2_queue(self):
        # prepare mock queues
        self.queue_map[const.PRIORITY_0].get.side_effect = threadqueue.Empty()
        self.queue_map[const.PRIORITY_1].get.side_effect = threadqueue.Empty()
        self.queue_map[const.PRIORITY_2].get.return_value = self.mock_task
        # should select priority 2
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        self.assertEqual(exc._get_new_task(), self.mock_task)

    def test_process_task_unresolved_task(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc.active_task = None
        exc._get_new_task = mock.Mock()
        exc._get_new_task.return_value = self.mock_task
        # assertions
        self.assertEqual(exc._process_task(), None)
        self.assertEqual(exc.active_task, self.mock_task)
        exc._get_new_task.assert_called_once_with()

    def test_process_task_empty_queue_map(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc._get_new_task = mock.Mock()
        exc._get_new_task.return_value = None
        # test when there is no active task
        exc.active_task = None
        # assertions
        self.assertEqual(exc._process_task(), None)
        self.assertEqual(exc.active_task, None)

    def test_process_task_fail(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc._get_new_task = mock.Mock()
        exc._get_new_task.side_effect = StandardError()
        with self.assertRaises(StandardError):
            self.assertEqual(exc._process_task(), None)

    def test_process_task_blocked(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.BLOCKED
        # test when active task is blocked
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc.active_task = self.mock_task
        self.assertEqual(exc._process_task(), None)
        self.assertNotEqual(exc.active_task, None)

    def test_process_task_pending(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.PENDING
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc.active_task = self.mock_task
        self.assertEqual(exc._process_task(), None)
        self.assertNotEqual(exc.active_task, None)
        self.mock_task.async_launch.assert_called_once_with()
        self.assertEqual(len(self.conn.send.call_args_list), 1)
        conn_msg = "call(Message[TASK_STARTED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_process_task_running(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.RUNNING
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc.active_task = self.mock_task
        self.assertEqual(exc._process_task(), None)

    def test_process_task_finished(self):
        self.mock_task.uid = "123"
        self.mock_task.status.return_value = scheduler.Task.FINISHED
        self.mock_task.exit_code = 1
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc.active_task = self.mock_task
        self.assertEqual(exc._process_task(), 1)
        self.assertEqual(exc.active_task, None)
        conn_msg = "call(Message[TASK_FINISHED]{'exit_code': 1, 'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_process_task_cancelled(self):
        self.mock_task.uid = "123"
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        exc.active_task = self.mock_task
        exc.cancel_task_ids = set(["123"])
        # method call
        self.assertEqual(exc._process_task(), None)
        # assertions
        self.assertEqual(exc.active_task, None)
        self.assertEqual(exc.cancel_task_ids, set())
        conn_msg = "call(Message[TASK_CANCELLED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_cancel_task(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=1, logger=mock.Mock())
        # test when no active task
        self.assertEqual(exc.active_task, None)
        exc._cancel_task()
        self.assertEqual(exc.active_task, None)
        # test when active task exists
        self.mock_task.uid = "123"
        exc.active_task = self.mock_task
        exc._cancel_task()
        self.assertEqual(exc.active_task, None)
        self.mock_task.cancel.assert_called_once_with()
        conn_msg = "call(Message[TASK_CANCELLED]{'task_id': '123'})"
        self.assertEqual(str(self.conn.send.call_args_list[0]), conn_msg)

    def test_iteration_executor_terminated(self):
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=0.5, logger=mock.Mock())
        exc._terminated = True
        self.assertEqual(exc.iteration(), False)

    def test_iteration_valid_task_poll(self):
        self.conn.poll.return_value = True
        self.conn.recv.return_value = 123
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=0.5, logger=mock.Mock())
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_task = mock.create_autospec(exc._process_task)
        self.assertEqual(exc.iteration(), True)
        exc._process_message.assert_called_once_with(123)
        exc._process_task.assert_called_once_with()

    def test_iteration_valid_task_no_poll(self):
        self.conn.poll.return_value = False
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=0.5, logger=mock.Mock())
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_task = mock.create_autospec(exc._process_task)
        self.assertEqual(exc.iteration(), True)
        self.assertEqual(exc._process_message.call_count, 0)
        exc._process_task.assert_called_once_with()

    def test_iteration_terminate(self):
        self.conn.poll.return_value = True
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=0.5, logger=mock.Mock())
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_message.side_effect = scheduler.TerminationException()
        exc._process_task = mock.create_autospec(exc._process_task)
        exc._cancel_task = mock.create_autospec(exc._cancel_task)
        self.assertEqual(exc.iteration(), False)
        self.assertEqual(exc._terminated, True)
        self.assertEqual(exc._process_task.call_count, 0)
        exc._cancel_task.assert_called_once_with()

    def test_iteration_terminate_global(self):
        self.conn.poll.return_value = True
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=0.5, logger=mock.Mock())
        exc._process_message = mock.create_autospec(exc._process_message)
        exc._process_message.side_effect = StandardError()
        self.assertEqual(exc.iteration(), False)

    @mock.patch("src.scheduler.time")
    def test_run(self, mock_time):
        # test time.sleep, if iteration returns True, `time.sleep` is called
        exc = scheduler.Executor("name", self.conn, self.queue_map, timeout=0.5, logger=mock.Mock())
        exc.iteration = mock.create_autospec(exc.iteration, return_value=False)
        self.assertEqual(exc.run(), False)
        self.assertEqual(mock_time.sleep.call_count, 0)
# pylint: enable=W0212,protected-access


class Atask(scheduler.Task):
    @property
    def uid(self):
        return "123"
    @property
    def exit_code(self):
        return 1
    def status(self):
        return scheduler.Task.BLOCKED
    def async_launch(self):
        return None
    def cancel(self):
        return None
# pylint: disable=W0212,protected-access
class SchedulerSuite(unittest.TestCase):
    def test_init_1(self):
        sched = scheduler.Scheduler(3, 0.5, None)
        self.assertEqual(sched.num_executors, 3)
        self.assertEqual(sched.timeout, 0.5)
        self.assertEqual(sched.logger, None)

    def test_init_2(self):
        with self.assertRaises(ValueError):
            scheduler.Scheduler("abc", 0.5, None)

    def test_prepare_executor(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
        # prepare mock executor
        exc = sched._prepare_executor("test")
        # assertions
        self.assertEqual(exc.daemon, True)
        self.assertEqual(exc.name, "test")
        self.assertEqual(sched.executors, [exc])
        self.assertTrue("test" in sched.pipe)

    def test_prepare_polling_thread(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
        # check polling thread without consumer
        thread = sched._prepare_polling_thread("test", target=None)
        self.assertEqual(thread, None)
        # check polling thread with consumer
        thread = sched._prepare_polling_thread("test", target=mock.Mock())
        self.assertNotEqual(thread, None)
        self.assertEqual(thread.daemon, True)
        self.assertEqual(thread.name, "test")

    def test_start(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
        mock_exc = mock.Mock()
        sched._prepare_executor = mock.Mock()
        sched._prepare_executor.return_value = mock_exc
        # launch executor
        sched.start()
        # assertions, check that prepare_executor was called number of executors times
        self.assertEqual(sched._prepare_executor.call_count, 3)
        self.assertEqual(mock_exc.start.call_count, 3)

    def test_start_maintenance(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
        sched._prepare_polling_thread = mock.Mock()
        mock_thread = mock.Mock()
        sched._prepare_polling_thread.return_value = mock_thread
        # launch maintenance should not result in any error
        sched.start_maintenance()
        mock_thread.start.assert_called_once_with()
        # launch maintenance when thread is None
        sched._prepare_polling_thread.return_value = None
        sched.start_maintenance()

    def test_put_wrong_priority(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
        with self.assertRaises(KeyError):
            sched.put("abc", scheduler.Task())

    def test_put_wrong_task(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
        with self.assertRaises(TypeError):
            sched.put(const.PRIORITY_0, None)

    def test_put(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
        mock_task = scheduler.Task()
        sched.task_queue_map = {
            const.PRIORITY_0: mock.Mock(),
            const.PRIORITY_1: mock.Mock(),
            const.PRIORITY_2: mock.Mock()
        }
        sched.put(const.PRIORITY_0, mock_task)
        sched.put(const.PRIORITY_1, mock_task)
        sched.put(const.PRIORITY_2, mock_task)
        # assertions
        sched.task_queue_map[const.PRIORITY_0].put.assert_called_once_with(mock_task, block=False)
        sched.task_queue_map[const.PRIORITY_1].put.assert_called_once_with(mock_task, block=False)
        sched.task_queue_map[const.PRIORITY_2].put.assert_called_once_with(mock_task, block=False)

    def test_cancel(self):
        sched = scheduler.Scheduler(3, timeout=0.5, logger=mock.Mock())
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
        conn_msg = "call(Message[TASK_CANCEL]{'task_id': '123'})"
        self.assertEqual(str(mock_conn.send.call_args_list[0]), conn_msg)

    @mock.patch("src.scheduler.time")
    def test_stop(self, mock_time):
        mock_time.sleep.return_value = None
        sched = scheduler.Scheduler(1, timeout=0.5, logger=mock.Mock())
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
# pylint: enable=W0212,protected-access

# Load test suites
def suites():
    return [
        MessageSuite,
        TerminationExceptionSuite,
        TaskSuite,
        ExecutorSuite,
        SchedulerSuite
    ]
