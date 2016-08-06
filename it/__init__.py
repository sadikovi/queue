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

import it.it_scheduler as it_scheduler
# Run integration tests using setup.py command "run_it", or use bin/it --python=bin/python

def addTests(tests):
    # add scheduler tests
    tests += [
        it_scheduler.SchedulerSimpleSuite(),
        it_scheduler.SchedulerCancelSuite(),
        it_scheduler.SchedulerFinishCancelSuite(),
        it_scheduler.SchedulerCancelAheadSuite(),
        it_scheduler.SchedulerFailedTasksSuite()
    ]

def runTests(tests):
    for test in tests:
        print "- Running test %s" % type(test).__name__
        test.setUp()
        try:
            test.runTest()
        finally:
            test.tearDown()
        print "-" * 70

def main():
    print ""
    print "== Running integration tests =="
    print "-" * 70
    tests = []
    addTests(tests)
    runTests(tests)
