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


# Load test suites
def suites():
    return [
        UtilSuite
    ]
