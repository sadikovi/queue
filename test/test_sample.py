#!/usr/bin/env python

import os, unittest
from types import IntType, LongType, StringType
from src.sample import A

class SampleSuite(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_sample(self):
        self.assertEqual("1", "1")

    def test_sample2(self):
        obj = A("name")
        self.assertEqual(obj.show(), "name")

# Load test suites
def _suites():
    return [
        SampleSuite
    ]

# Load tests
def loadSuites():
    # global test suite for this module
    gsuite = unittest.TestSuite()
    for suite in _suites():
        gsuite.addTest(unittest.TestLoader().loadTestsFromTestCase(suite))
    return gsuite

if __name__ == '__main__':
    suite = loadSuites()
    print ""
    print "### Running tests ###"
    print "-" * 70
    unittest.TextTestRunner(verbosity=2).run(suite)
