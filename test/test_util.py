#!/usr/bin/env python

import unittest
import mock
import src.util as util

class UtilSuite(unittest.TestCase):
    @mock.patch("src.util.os")
    def test_readwriteDirectory_fail_1(self, mock_os):
        mock_os.path.abspath.side_effect = StandardError("Test")
        with self.assertRaises(StandardError):
            util.readwriteDirectory(".")

    @mock.patch("src.util.os")
    def test_readwriteDirectory_fail_2(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.side_effect = StandardError("Test")
        with self.assertRaises(StandardError):
            util.readwriteDirectory(".")

    @mock.patch("src.util.os")
    def test_readwriteDirectory_fail_3(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.return_value = "/tmp"
        mock_os.path.isdir.return_value = False
        with self.assertRaises(StandardError):
            util.readwriteDirectory(".")

    @mock.patch("src.util.os")
    def test_readwriteDirectory_fail_4(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.return_value = "/tmp"
        mock_os.path.isdir.return_value = True
        mock_os.access.return_value = False
        with self.assertRaises(StandardError):
            util.readwriteDirectory(".")

    @mock.patch("src.util.os")
    def test_readwriteDirectory_success(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp/a"
        mock_os.path.realpath.return_value = "/tmp/a/b"
        mock_os.path.isdir.return_value = True
        mock_os.access.return_value = True
        self.assertEqual(util.readwriteDirectory("."), "/tmp/a/b")

class URISuite(unittest.TestCase):
    def setUp(self):
        self.uri1 = util.URI("http://localhost:8080", "Spark UI")
        self.uri2 = util.URI("spark://sandbox:7077")

    def test_init(self):
        uri = util.URI("http://localhost:8080")
        self.assertNotEqual(uri, None)

    def test_invalidScheme(self):
        with self.assertRaises(StandardError):
            util.URI("localhost:8080")

    def test_invalidHost(self):
        with self.assertRaises(StandardError):
            util.URI("http://:8080")

    def test_invalidPort(self):
        with self.assertRaises(StandardError):
            util.URI("http://localhost")
        with self.assertRaises(ValueError):
            util.URI("http://localhost:ABC")

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

# Load test suites
def suites():
    return [
        UtilSuite,
        URISuite
    ]
