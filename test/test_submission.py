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
import src.submission as submission

class SubmissionSuite(unittest.TestCase):
    def test_validate_delay(self):
        with self.assertRaises(ValueError):
            submission.validate_delay("str")
        with self.assertRaises(TypeError):
            submission.validate_delay(None)
        with self.assertRaises(ValueError):
            submission.validate_delay(-1)
        self.assertEqual(submission.validate_delay(const.SUBMISSION_DELAY_MIN),
                         const.SUBMISSION_DELAY_MIN)
        self.assertEqual(submission.validate_delay(const.SUBMISSION_DELAY_MAX),
                         const.SUBMISSION_DELAY_MAX)
        self.assertEqual(submission.validate_delay(12), 12)

    def test_validate_name(self):
        with self.assertRaises(TypeError):
            submission.validate_name(None)
        with self.assertRaises(TypeError):
            submission.validate_name({})
        with self.assertRaises(ValueError):
            submission.validate_name("")
        with self.assertRaises(ValueError):
            submission.validate_name("     ")
        self.assertEqual(submission.validate_name(" abc "), "abc")
        self.assertEqual(submission.validate_name("ABC"), "ABC")

    def test_validate_priority(self):
        with self.assertRaises(ValueError):
            submission.validate_priority(None)
        with self.assertRaises(ValueError):
            submission.validate_priority("abc")
        with self.assertRaises(ValueError):
            submission.validate_priority(-1)
        self.assertEqual(submission.validate_priority(const.PRIORITY_0), const.PRIORITY_0)
        self.assertEqual(submission.validate_priority(const.PRIORITY_1), const.PRIORITY_1)
        self.assertEqual(submission.validate_priority(const.PRIORITY_2), const.PRIORITY_2)

    def test_init_simple(self):
        with self.assertRaises(ValueError):
            submission.Submission("", "")
        # simple init
        sub = submission.Submission("abc", "SPARK")
        self.assertNotEqual(sub.uid, None)
        self.assertEqual(sub.name, "abc")
        self.assertEqual(sub.system_code, "SPARK")
        self.assertEqual(sub.priority, const.PRIORITY_2)
        self.assertEqual(sub.is_deleted, False)
        self.assertEqual(sub.is_template, False)
        self.assertEqual(sub.status, const.SUBMISSION_PENDING)
        self.assertNotEqual(sub.createtime, None)
        self.assertNotEqual(sub.submittime, None)
        self.assertEqual(sub.starttime, None)
        self.assertEqual(sub.finishtime, None)
        self.assertEqual(sub.payload, {})
        self.assertEqual(sub.username, "default")

    def test_init_delayed(self):
        sub = submission.Submission("abc", "SPARK", delay=30)
        self.assertTrue((sub.submittime - sub.createtime).total_seconds >= 30)
        self.assertEqual(sub.is_delayed(), True)

    def test_set_status(self):
        sub = submission.Submission("abc", "SPARK")
        self.assertEqual(sub.status, const.SUBMISSION_PENDING)
        sub.set_status(const.SUBMISSION_RUNNING)
        self.assertEqual(sub.status, const.SUBMISSION_RUNNING)
        with self.assertRaises(ValueError):
            sub.set_status(None)

    def test_is_delayed(self):
        sub = submission.Submission("abc", "SPARK")
        self.assertEqual(sub.is_delayed(), False)
        sub = submission.Submission("abc", "SPARK", delay=const.SUBMISSION_DELAY_MIN)
        self.assertEqual(sub.is_delayed(), False)
        sub = submission.Submission("abc", "SPARK", delay=const.SUBMISSION_DELAY_MAX)
        self.assertEqual(sub.is_delayed(), True)

    def test_mark_start(self):
        sub = submission.Submission("abc", "SPARK")
        self.assertEqual(sub.starttime, None)
        sub.mark_start()
        self.assertNotEqual(sub.starttime, None)
        self.assertTrue((sub.starttime - sub.createtime).total_seconds() >= 0)

    def test_mark_finish(self):
        sub = submission.Submission("abc", "SPARK")
        self.assertEqual(sub.finishtime, None)
        sub.mark_finish()
        self.assertNotEqual(sub.finishtime, None)
        self.assertTrue((sub.finishtime - sub.createtime).total_seconds() >= 0)

    def test_mark_deleted(self):
        sub = submission.Submission("abc", "SPARK")
        self.assertEqual(sub.is_deleted, False)
        sub.mark_deleted()
        self.assertEqual(sub.is_deleted, True)

    def test_dumps_loads(self):
        sub1 = submission.Submission("abc", "SPARK", priority=const.PRIORITY_0, delay=30,
                                     payload={"a": 1})
        sub2 = submission.Submission.loads(sub1.dumps())
        self.assertEqual(sub2.uid, sub1.uid)
        self.assertEqual(sub2.name, sub1.name)
        self.assertEqual(sub2.system_code, sub1.system_code)
        self.assertEqual(sub2.priority, sub1.priority)
        self.assertEqual(sub2.is_deleted, sub1.is_deleted)
        self.assertEqual(sub2.is_template, sub1.is_template)
        self.assertEqual(sub2.status, sub1.status)
        self.assertEqual(sub2.createtime, sub1.createtime)
        self.assertEqual(sub2.submittime, sub1.submittime)
        self.assertEqual(sub2.starttime, sub1.starttime)
        self.assertEqual(sub2.finishtime, sub1.finishtime)
        self.assertEqual(sub2.payload, sub1.payload)
        self.assertEqual(sub2.username, sub1.username)

class TemplateSuite(unittest.TestCase):
    def test_init(self):
        with self.assertRaises(ValueError):
            submission.Template("", "")
        template = submission.Template("abc", "SPARK")
        self.assertEqual(template.name, "abc")
        self.assertEqual(template.system_code, "SPARK")
        self.assertEqual(template.is_template, True)

    def test_dumps_loads(self):
        template1 = submission.Template("abc", "SPARK")
        template2 = submission.Template.loads(template1.dumps())
        self.assertEqual(template1.is_template, True)
        self.assertEqual(template2.is_template, True)
        self.assertEqual(template2.uid, template1.uid)
        self.assertEqual(template2.name, template1.name)

# Load test suites
def suites():
    return [
        SubmissionSuite,
        TemplateSuite
    ]
