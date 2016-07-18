#!/usr/bin/env python

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

# Load test suites
def suites():
    return [
        SessionSuite
    ]
