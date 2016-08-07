#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# Copyright 2016 sadikovi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import cPickle
import unittest
import mock
import src.const as const
import src.scheduler as scheduler
import src.simple as simple

simple.logger = mock.Mock()

# pylint: disable=W0212,protected-access
class SimpleTaskSuite(unittest.TestCase):
    def test_init(self):
        task = simple.SimpleTask("123", 123)
        self.assertEqual(task.uid, "123")
        self.assertEqual(task.priority, 123)
        self.assertEqual(task._cancelled, False)
        self.assertEqual(task._iterations, 10)
        self.assertEqual(task.name, "SimpleTask")

    @mock.patch("src.simple.time")
    def test_run(self, mock_time):
        task = simple.SimpleTask("123", 123)
        task.run()
        self.assertEqual(mock_time.sleep.call_count, task._iterations)
        mock_time.sleep.assert_called_with(1.0)

    @mock.patch("src.simple.time")
    def test_run_cancel(self, mock_time):
        task = simple.SimpleTask("123", 123)
        task.is_cancelled = mock.Mock()
        task.is_cancelled.side_effect = [False, False, True]
        task.run()
        self.assertEqual(mock_time.sleep.call_count, 2)
        mock_time.sleep.assert_called_with(1.0)

    def test_cancel(self):
        task = simple.SimpleTask("123", 123)
        self.assertEqual(task._cancelled, False)
        self.assertEqual(task.is_cancelled(), False)
        task.cancel()
        self.assertEqual(task._cancelled, True)
        self.assertEqual(task.is_cancelled(), True)

    def test_serde(self):
        task = simple.SimpleTask("123", 123)
        ser = cPickle.dumps(task)
        new_task = cPickle.loads(ser)
        self.assertEqual(new_task.uid, task.uid)
        self.assertEqual(new_task.priority, task.priority)
        self.assertEqual(new_task.dumps(), task.dumps())
# pylint: enable=W0212,protected-access

class SimpleSessionSuite(unittest.TestCase):
    def setUp(self):
        self.session = simple.SimpleSession(3, timeout=1.0)

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
        session = simple.SimpleSession.create(conf, mock.Mock())
        self.assertEqual(session.timeout, 1.0)
        self.assertEqual(session.num_executors, 10)

def suites():
    return [
        SimpleTaskSuite,
        SimpleSessionSuite
    ]
