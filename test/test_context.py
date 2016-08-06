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
import src.context as context

class SessionSuite(unittest.TestCase):
    def setUp(self):
        self.session = context.Session()

    def test_system_code(self):
        with self.assertRaises(StandardError):
            self.session.system_code()

    def test_system_uri(self):
        with self.assertRaises(NotImplementedError):
            self.session.system_uri()

    def test_status(self):
        with self.assertRaises(NotImplementedError):
            self.session.status()

    def test_scheduler(self):
        with self.assertRaises(NotImplementedError):
            test = self.session.scheduler
            self.assertTrue(test is None)

    def test_create(self):
        with self.assertRaises(NotImplementedError):
            context.Session.create({"a": 1}, "work-dir", None)

    def test_create_task(self):
        with self.assertRaises(NotImplementedError):
            self.session.create_task(None)

# Load test suites
def suites():
    return [
        SessionSuite
    ]
