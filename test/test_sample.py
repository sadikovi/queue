#!/usr/bin/env python

import unittest
from src.sample import Aclass

class SampleSuite(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_sample(self):
        self.assertEqual("1", "1")

    def test_sample2(self):
        obj = Aclass("name")
        self.assertEqual(obj.show(), "name")

# Load test suites
def suites():
    return [
        SampleSuite
    ]
