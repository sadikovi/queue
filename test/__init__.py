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

import importlib
import sys
import unittest

# Select what tests to run
RUN_TESTS = {
    "test.test_const": True,
    "test.test_queue": True,
    "test.test_util": True,
    "test.test_context": True,
    "test.test_scheduler": True,
    "test.test_spark": True,
    "test.test_simple": True,
    "test.test_submission": True
}

suites = unittest.TestSuite()

# Add individual test module
def addTests(module_name):
    if module_name in RUN_TESTS and RUN_TESTS[module_name]:
        module = importlib.import_module(module_name)
        batch = loadSuites(module)
        suites.addTest(batch)
    else:
        print "@skip: '%s' tests" % module_name

# Load test suites for module
def loadSuites(module):
    gsuite = unittest.TestSuite()
    for suite in module.suites():
        print "Adding %s" % suite
        gsuite.addTest(unittest.TestLoader().loadTestsFromTestCase(suite))
    return gsuite

def collectSystemTests():
    for test_name in RUN_TESTS.keys():
        addTests(test_name)

def main():
    print ""
    print "== Gathering tests info =="
    print "-" * 70
    collectSystemTests()
    print ""
    print "== Running tests =="
    print "-" * 70
    results = unittest.TextTestRunner(verbosity=2).run(suites)
    num = len([x for x in RUN_TESTS.values() if not x])
    print "%s Number of test modules skipped: %d" %("OK" if num == 0 else "WARN", num)
    print ""
    # Fail if there is at least 1 error or failure
    if results and len(results.failures) == 0 and len(results.errors) == 0:
        return 0
    else:
        return 1
