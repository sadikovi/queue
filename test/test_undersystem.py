#!/usr/bin/env python

import unittest
import src.undersystem as us

class StatusSuite(unittest.TestCase):
    def test_status(self):
        status = us.Status("a", "b")
        self.assertEqual(status.name, "a")
        self.assertEqual(status.desc, "b")

    def test_statusWithNone(self):
        status = us.Status(None, None)
        self.assertEqual(status.name, None)
        self.assertEqual(status.desc, None)

    def test_statusAvailable(self):
        self.assertEqual(us.AVAILABLE.name, "available")
        self.assertEqual(us.AVAILABLE.desc, "Available")

    def test_statusBusy(self):
        self.assertEqual(us.BUSY.name, "busy")
        self.assertEqual(us.BUSY.desc, "Busy")

    def test_statusUnavailable(self):
        self.assertEqual(us.UNAVAILABLE.name, "unavailable")
        self.assertEqual(us.UNAVAILABLE.desc, "Unavailable")

class LinkSuite(unittest.TestCase):
    def test_link(self):
        link = us.Link("http://test.com", "name")
        self.assertEqual(link.name, "name")
        self.assertEqual(link.link, "http://test.com")

class SubmissionRequestSuite(unittest.TestCase):
    def test_init(self):
        with self.assertRaises(StandardError):
            a = us.SubmissionRequest()

    def test_submit(self):
        class TestSubmit(us.SubmissionRequest):
            def __init__(self):
                pass
        a = TestSubmit()
        with self.assertRaises(NotImplementedError):
            a.submit()

# Dummy system interface
class TestUnderSystemInterface(us.UnderSystemInterface):
    def __init__(self):
        pass

class UnderSystemInterfaceSuite(unittest.TestCase):
    def setUp(self):
        self.testus = TestUnderSystemInterface()

    def test_init(self):
        with self.assertRaises(StandardError):
            a = us.UnderSystemInterface()

    def test_name(self):
        with self.assertRaises(NotImplementedError):
            self.testus.name()

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

    def test_available(self):
        self.assertEqual(self.testus.available(), us.AVAILABLE.desc)

    def test_busy(self):
        self.assertEqual(self.testus.busy(), us.BUSY.desc)

    def test_unavailable(self):
        self.assertEqual(self.testus.unavailable(), us.UNAVAILABLE.desc)

# Load test suites
def suites():
    return [
        StatusSuite,
        LinkSuite,
        SubmissionRequestSuite,
        UnderSystemInterfaceSuite
    ]
