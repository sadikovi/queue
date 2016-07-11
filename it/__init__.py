#!/usr/bin/env python

import it.it_scheduler as it_scheduler
# Run integration tests using setup.py command "run_it", or use bin/it --python=bin/python

def addTests(tests):
    # add scheduler tests
    scheduler1_test = it_scheduler.Scheduler1IntegrationTest()
    scheduler2_test = it_scheduler.Scheduler2IntegrationTest()
    scheduler3_test = it_scheduler.Scheduler3IntegrationTest()
    scheduler4_test = it_scheduler.Scheduler4IntegrationTest()
    scheduler5_test = it_scheduler.Scheduler5IntegrationTest()
    tests.append(scheduler1_test)
    tests.append(scheduler2_test)
    tests.append(scheduler3_test)
    tests.append(scheduler4_test)
    tests.append(scheduler5_test)

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
