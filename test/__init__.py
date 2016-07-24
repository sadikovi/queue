#!/usr/bin/env python

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
