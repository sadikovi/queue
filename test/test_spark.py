#!/usr/bin/env python

import os
import unittest
import urllib2
import mock
import src.const as const
import src.spark as spark

class SparkSubmissionRequestSuite(unittest.TestCase):
    def setUp(self):
        self.req = spark.SparkSubmissionRequest("SPARK", ".", {"a": 1}, "class", "jar", ["a", "b"])

    @mock.patch("src.util.os")
    def test_init(self, mock_os):
        working_dir = "/some/dir"
        mock_os.readwriteDirectory.return_value = working_dir
        mock_os.path.realpath.return_value = working_dir
        mock_os.path.isdir.return_value = True
        mock_os.access.return_value = True

        req = spark.SparkSubmissionRequest("SPARK", ".", {"a": 1}, "class", "jar", ["a", "b"])
        self.assertEqual(req.spark_code, "SPARK")
        self.assertEqual(req.working_directory, working_dir)
        self.assertEqual(req.spark_options, {"a": 1})
        self.assertEqual(req.main_class, "class")
        self.assertEqual(req.jar, "jar")
        self.assertEqual(req.job_options, ["a", "b"])

    def test_init_none_dir(self):
        with self.assertRaises(StandardError):
            spark.SparkSubmissionRequest("SPARK", None, {"a": 1}, "class", "jar", ["a", "b"])

    @mock.patch("src.util.os")
    def test_init_nonexistent_dir(self, mock_os):
        mock_os.path.isdir.return_value = False

        try:
            spark.SparkSubmissionRequest("SPARK", "nonexistent path", {}, "class", "jar", [])
        except StandardError as err:
            self.assertTrue("not a directory" in str(err))

    @mock.patch("src.util.os")
    def test_init_readonly_dir(self, mock_os):
        mock_os.path.isdir.return_value = True
        mock_os.access.return_value = False

        try:
            spark.SparkSubmissionRequest("SPARK", "dir", {"a": 1}, "class", "jar", ["a", "b"])
        except StandardError as err:
            self.assertTrue("Insufficient permissions" in str(err))

    def test_working_directory(self):
        self.assertEqual(self.req.workingDirectory(), os.path.abspath("."))

    def test_interfaceCode(self):
        self.assertEqual(self.req.interfaceCode(), "SPARK")

    def test_dispatch(self):
        with self.assertRaises(NotImplementedError):
            self.req.dispatch()

    def test_ping(self):
        with self.assertRaises(NotImplementedError):
            self.req.ping()

    def test_close(self):
        with self.assertRaises(NotImplementedError):
            self.req.close()

class SparkBackendSuite(unittest.TestCase):
    def setUp(self):
        # Mock response for file object
        class MockResponse(object):
            def read(self):
                return None
        self.spark = spark.SparkBackend("spark://sandbox:7077", "http://localhost:8080", 3, ".")
        self.mock_response = MockResponse()
        self.mock_applications = mock.Mock()

    def test_init_wrong_slots(self):
        with self.assertRaises(ValueError):
            spark.SparkBackend("spark://sandbox:7077", "http://localhost:8080", "abc", ".")

    def test_init_wrong_master_url(self):
        with self.assertRaises(StandardError):
            spark.SparkBackend("http://sandbox:7077", "http://localhost:8080", 3, ".")

    def test_init_wrong_rest_url(self):
        with self.assertRaises(StandardError):
            spark.SparkBackend("http://sandbox:7077", "abc", 3, ".")

    @mock.patch("src.util.os")
    def test_init_wrong_directory(self, mock_os):
        mock_os.path.abspath.return_value = "/tmp"
        mock_os.path.realpath.return_value = "/tmp"
        mock_os.path.isdir.return_value = False
        with self.assertRaises(StandardError):
            spark.SparkBackend("spark://sandbox:7077", "http://localhost:8080", 3, ".")

    @mock.patch("src.util.os")
    def test_init_wrong_permissions(self, mock_os):
        mock_os.abspath.return_value = "/tmp"
        mock_os.normpath.return_value = "/tmp"
        mock_os.isdir.return_value = True
        mock_os.access.side_effect = False
        with self.assertRaises(StandardError):
            spark.SparkBackend("spark://sandbox:7077", "http://localhost:8080", 3, ".")

    def test_name(self):
        self.assertEqual(self.spark.name(), "Spark cluster")

    def test_code(self):
        self.assertEqual(self.spark.code(), "SPARK")

    def test_link(self):
        self.assertEqual(self.spark.link().alias, "Spark UI")
        self.assertEqual(self.spark.link().url, "http://localhost:8080")

    def test_request(self):
        with self.assertRaises(NotImplementedError):
            self.spark.request()

    @mock.patch("src.spark.urllib2")
    def test_applications_fail_request(self, mock_urllib2):
        mock_urllib2.urlopen.side_effect = urllib2.URLError("Test")
        self.assertEqual(self.spark.applications("dummy"), None)

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_fail_json(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = self.mock_response
        mock_json.loads.side_effect = ValueError("Test")
        with self.assertRaises(ValueError):
            self.spark.applications("url")

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_no_data(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = self.mock_response
        mock_json.loads.return_value = []
        self.assertEqual(self.spark.applications("url"), [])

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_invalid_data(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = self.mock_response
        mock_json.loads.return_value = [
            {"a": "2", "b": "abc", "c": [{"completed": True}]}
        ]
        self.assertEqual(self.spark.applications("url"), [])

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_no_running(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = self.mock_response
        mock_json.loads.return_value = [
            {"id": "1", "name": "abc", "attempts": [{"completed": True}, {"completed": True}]},
            {"id": "2", "name": "abc", "attempts": [{"completed": True}]}
        ]
        self.assertEqual(self.spark.applications("url"), [
            {"id": "1", "name": "abc", "completed": True},
            {"id": "2", "name": "abc", "completed": True}
        ])

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_running(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = self.mock_response
        mock_json.loads.return_value = [
            {"id": "1", "name": "abc", "attempts": [{"completed": True}, {"completed": False}]},
            {"id": "2", "name": "abc", "attempts": [{"completed": True}]}
        ]
        self.assertEqual(self.spark.applications("url"), [
            {"id": "1", "name": "abc", "completed": False},
            {"id": "2", "name": "abc", "completed": True}
        ])

    def test_status_available_empty(self):
        self.mock_applications.return_value = []
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.status(), const.SYSTEM_AVAILABLE)

    def test_status_available_complete(self):
        self.mock_applications.return_value = [
            {"id": "2", "name": "abc", "completed": True}
        ]
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.status(), const.SYSTEM_AVAILABLE)

    def test_status_busy(self):
        self.mock_applications.return_value = [
            {"id": "2", "name": "abc", "completed": False}
        ]
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.status(), const.SYSTEM_BUSY)

    def test_status_unavailable(self):
        self.mock_applications.return_value = None
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.status(), const.SYSTEM_UNAVAILABLE)

    def test_can_create_request_none(self):
        self.mock_applications.return_value = None
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.can_create_request(), False)

    def test_can_create_request_empty(self):
        self.mock_applications.return_value = []
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.can_create_request(), True)

    def test_can_create_request_running1(self):
        # less than max number of slots, one running application
        self.mock_applications.return_value = [
            {"id": "1", "name": "abc", "completed": False}
        ]
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.can_create_request(), True)

    def test_can_create_request_running2(self):
        # more or equal to max number of slots, one running application
        self.mock_applications.return_value = [
            {"id": "1", "name": "abc", "completed": False},
            {"id": "2", "name": "abc", "completed": False},
            {"id": "2", "name": "abc", "completed": False},
            {"id": "2", "name": "abc", "completed": False}
        ]
        self.spark.applications = self.mock_applications
        self.assertEqual(self.spark.can_create_request(), False)

# Load test suites
def suites():
    return [
        SparkSubmissionRequestSuite,
        SparkBackendSuite
    ]
