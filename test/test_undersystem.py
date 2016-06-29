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

    def test_system_available(self):
        self.assertEqual(us.AVAILABLE.name, "available")
        self.assertEqual(us.AVAILABLE.desc, "Available")

    def test_system_busy(self):
        self.assertEqual(us.BUSY.name, "busy")
        self.assertEqual(us.BUSY.desc, "Busy")

    def test_system_unavailable(self):
        self.assertEqual(us.UNAVAILABLE.name, "unavailable")
        self.assertEqual(us.UNAVAILABLE.desc, "Unavailable")

    def test_submit_pending(self):
        self.assertEqual(us.PENDING.name, "pending")
        self.assertEqual(us.PENDING.desc, "Pending")

    def test_submit_success(self):
        self.assertEqual(us.SUCCESS.name, "success")
        self.assertEqual(us.SUCCESS.desc, "Success")

    def test_submit_failure(self):
        self.assertEqual(us.FAILURE.name, "failure")
        self.assertEqual(us.FAILURE.desc, "Failure")

class URISuite(unittest.TestCase):
    def setUp(self):
        self.uri1 = us.URI("http://localhost:8080", "Spark UI")
        self.uri2 = us.URI("spark://sandbox:7077")

    def test_init(self):
        uri = us.URI("http://localhost:8080")
        self.assertNotEqual(uri, None)

    def test_invalidScheme(self):
        with self.assertRaises(StandardError):
            us.URI("localhost:8080")

    def test_invalidHost(self):
        with self.assertRaises(StandardError):
            us.URI("http://:8080")

    def test_invalidPort(self):
        with self.assertRaises(StandardError):
            us.URI("http://localhost")
        with self.assertRaises(ValueError):
            us.URI("http://localhost:ABC")

    def test_host(self):
        self.assertEqual(self.uri1.host, "localhost")
        self.assertEqual(self.uri2.host, "sandbox")

    def test_port(self):
        self.assertEqual(self.uri1.port, 8080)
        self.assertEqual(self.uri2.port, 7077)

    def test_scheme(self):
        self.assertEqual(self.uri1.scheme, "http")
        self.assertEqual(self.uri2.scheme, "spark")

    def test_netloc(self):
        self.assertEqual(self.uri1.netloc, "localhost:8080")
        self.assertEqual(self.uri2.netloc, "sandbox:7077")

    def test_fragment(self):
        self.assertEqual(self.uri1.fragment, "")
        self.assertEqual(self.uri2.fragment, "")

    def test_url(self):
        self.assertEqual(self.uri1.url, "http://localhost:8080")
        self.assertEqual(self.uri2.url, "spark://sandbox:7077")

    def test_alias(self):
        self.assertEqual(self.uri1.alias, "Spark UI")
        self.assertEqual(self.uri2.alias, "spark://sandbox:7077")

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

    def test_dispatch(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.dispatch()

    def test_workingDirectory(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.workingDirectory()

    def test_ping(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.ping()

    def test_close(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.close()

    def test_interface(self):
        with self.assertRaises(NotImplementedError):
            self.request_submit.interface()

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
        self.assertEqual(self.testus.available(), us.AVAILABLE.desc)

    def test_busy(self):
        self.assertEqual(self.testus.busy(), us.BUSY.desc)

    def test_unavailable(self):
        self.assertEqual(self.testus.unavailable(), us.UNAVAILABLE.desc)

# Load test suites
def suites():
    return [
        StatusSuite,
        URISuite,
        SubmissionRequestSuite,
        UnderSystemInterfaceSuite
    ]
