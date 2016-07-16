#!/usr/bin/env python

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
