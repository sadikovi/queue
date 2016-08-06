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

import unittest
import src.const as const

class ConstSuite(unittest.TestCase):
    def test_status(self):
        status = const.Status("name", "desc")
        self.assertEqual(status.name, "name")
        self.assertEqual(status.desc, "desc")

    def test_status_pretty_name(self):
        status = const.Status("name", "desc")
        pretty_name = u"name"
        self.assertEqual("%s" % status, pretty_name)
        self.assertEqual(status.__unicode__(), pretty_name)
        self.assertEqual(status.__repr__(), pretty_name)

    def test_status_none(self):
        status = const.Status(None, None)
        self.assertEqual(status.name, None)
        self.assertEqual(status.desc, None)

    # Tests for system statuses
    def test_system_available(self):
        self.assertEqual(const.SYSTEM_AVAILABLE.name, "AVAILABLE")
        self.assertEqual(const.SYSTEM_AVAILABLE.desc, "Available")

    def test_system_busy(self):
        self.assertEqual(const.SYSTEM_BUSY.name, "BUSY")
        self.assertEqual(const.SYSTEM_BUSY.desc, "Busy")

    def test_system_unavailable(self):
        self.assertEqual(const.SYSTEM_UNAVAILABLE.name, "UNAVAILABLE")
        self.assertEqual(const.SYSTEM_UNAVAILABLE.desc, "Unavailable")

    # Tests for submission statuses
    def test_submission_pending(self):
        self.assertEqual(const.SUBMISSION_PENDING, "PENDING")

    def test_submission_waiting(self):
        self.assertEqual(const.SUBMISSION_WAITING, "WAITING")

    def test_submission_running(self):
        self.assertEqual(const.SUBMISSION_RUNNING, "RUNNING")

    def test_submission_success(self):
        self.assertEqual(const.SUBMISSION_SUCCESS, "SUCCESS")

    def test_submission_failure(self):
        self.assertEqual(const.SUBMISSION_FAILURE, "FAILURE")

    def test_submission_discard(self):
        self.assertEqual(const.SUBMISSION_DISCARD, "DISCARD")

    def test_submission_unknown(self):
        self.assertEqual(const.SUBMISSION_UNKNOWN, "UNKNOWN")

    # Tests for priority levels
    def test_priority(self):
        self.assertEqual(const.PRIORITY_0, 0)
        self.assertEqual(const.PRIORITY_1, 1)
        self.assertEqual(const.PRIORITY_2, 2)

    def test_conf_options(self):
        self.assertEqual(const.OPT_NUM_PARALLEL_TASKS, "queue.num.parallel.tasks")
        self.assertEqual(const.OPT_SCHEDULER_TIMEOUT, "queue.scheduler.timeout")
        self.assertEqual(const.OPT_WORKING_DIR, "queue.working.directory")
        self.assertEqual(const.OPT_SERVICE_DIR, "queue.service.directory")
        self.assertEqual(const.OPT_SYSTEM_CODE, "queue.system.code")
        self.assertEqual(const.OPT_SPARK_MASTER, "spark.master")
        self.assertEqual(const.OPT_SPARK_WEB, "spark.web")

# Load test suites
def suites():
    return [
        ConstSuite
    ]
