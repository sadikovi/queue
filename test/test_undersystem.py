#!/usr/bin/env python

import unittest
import src.const as const
import src.undersystem as us

# Dummy submission class
# pylint: disable=W0223,W0231
class TestSubmit(us.SubmissionRequest):
    def __init__(self):
        pass
# pylint: enable=W0223,W0231

class SubmissionRequestSuite(unittest.TestCase):
    def setUp(self):
        self.request_submit = TestSubmit()

    def test_init(self):
        with self.assertRaises(StandardError):
            us.SubmissionRequest()

    def test_interfaceCode(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.interfaceCode()

    def test_workingDirectory(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.workingDirectory()

    def test_dispatch(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.dispatch()

    def test_ping(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.ping()

    def test_close(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.close()

# Dummy system interface
# pylint: disable=W0223,W0231
class TestUnderSystemInterface(us.UnderSystemInterface):
    def __init__(self):
        pass
# pylint: enable=W0223,W0231

class UnderSystemInterfaceSuite(unittest.TestCase):
    def setUp(self):
        self.testus = TestUnderSystemInterface()

    def test_init(self):
        with self.assertRaises(StandardError):
            us.UnderSystemInterface()

    def test_name(self):
        with self.assertRaises(NotImplementedError):
            self.testus.name()

    def test_code(self):
        with self.assertRaises(NotImplementedError):
            self.testus.code()

    def test_status(self):
        with self.assertRaises(NotImplementedError):
            self.testus.status()

    def test_link(self):
        with self.assertRaises(NotImplementedError):
            self.testus.link()

    def test_request(self):
        with self.assertRaises(NotImplementedError):
            self.testus.request()
        with self.assertRaises(NotImplementedError):
            self.testus.request(a=1, b=2)

    def test_canCreateRequest(self):
        with self.assertRaises(NotImplementedError):
            self.testus.can_create_request()

    def test_available(self):
        self.assertEqual(self.testus.available(), const.SYSTEM_AVAILABLE.desc)

    def test_busy(self):
        self.assertEqual(self.testus.busy(), const.SYSTEM_BUSY.desc)

    def test_unavailable(self):
        self.assertEqual(self.testus.unavailable(), const.SYSTEM_UNAVAILABLE.desc)

# Load test suites
def suites():
    return [
        SubmissionRequestSuite,
        UnderSystemInterfaceSuite
    ]
