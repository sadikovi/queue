#!/usr/bin/env python

import unittest
import mock
import src.const as const
import src.scheduler as scheduler
import src.simple as simple

# pylint: disable=W0212,protected-access
class SimpleTaskSuite(unittest.TestCase):
    def test_init(self):
        task = simple.SimpleTask("123", 123, logger=mock.Mock())
        self.assertEqual(task.uid, "123")
        self.assertEqual(task.priority, 123)
        self.assertEqual(task._cancelled, False)
        self.assertEqual(task._iterations, 10)
        self.assertEqual(task.name, "SimpleTask")

    @mock.patch("src.simple.time")
    def test_run(self, mock_time):
        task = simple.SimpleTask("123", 123, logger=mock.Mock())
        task.run()
        self.assertEqual(mock_time.sleep.call_count, task._iterations)
        mock_time.sleep.assert_called_with(1.0)

    @mock.patch("src.simple.time")
    def test_run_cancel(self, mock_time):
        task = simple.SimpleTask("123", 123, logger=mock.Mock())
        task.is_cancelled = mock.Mock()
        task.is_cancelled.side_effect = [False, False, True]
        task.run()
        self.assertEqual(mock_time.sleep.call_count, 2)
        mock_time.sleep.assert_called_with(1.0)

    def test_cancel(self):
        task = simple.SimpleTask("123", 123, logger=mock.Mock())
        self.assertEqual(task._cancelled, False)
        self.assertEqual(task.is_cancelled(), False)
        task.cancel()
        self.assertEqual(task._cancelled, True)
        self.assertEqual(task.is_cancelled(), True)
# pylint: enable=W0212,protected-access

class SimpleSessionSuite(unittest.TestCase):
    def setUp(self):
        self.session = simple.SimpleSession(3, timeout=1.0, logger=mock.Mock())

    def test_init(self):
        self.assertEqual(self.session.num_executors, 3)
        self.assertEqual(self.session.timeout, 1.0)
        self.assertTrue(isinstance(self.session.scheduler, scheduler.Scheduler))

    def test_system_code(self):
        self.assertEqual(self.session.system_code(), simple.SIMPLE_SYSTEM_CODE)

    def test_system_uri(self):
        self.assertEqual(self.session.system_uri(), None)

    def test_status(self):
        self.assertEqual(self.session.status(), const.SYSTEM_AVAILABLE)

    def test_create_task(self):
        sub = mock.Mock()
        sub.uid = "123"
        sub.priority = 123
        task = self.session.create_task(sub)
        self.assertEqual(task.uid, "123")
        self.assertEqual(task.priority, 123)

    def test_create(self):
        conf = mock.Mock()
        conf.getConfFloat.return_value = 1.0
        conf.getConfInt.return_value = 10
        session = simple.SimpleSession.create(conf, mock.Mock(), logger=mock.Mock())
        self.assertEqual(session.timeout, 1.0)
        self.assertEqual(session.num_executors, 10)

def suites():
    return [
        SimpleTaskSuite,
        SimpleSessionSuite
    ]
