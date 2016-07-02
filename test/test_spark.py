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
        # Mock response for uuid.uuid4()
        class MockUUID(object):
            @property
            def hex(self):
                return "abcdef123456"
        self.spark = spark.SparkBackend("spark://sandbox:7077", "http://localhost:8080", 3, ".")
        self.mock_response = MockResponse()
        self.mock_uuid = MockUUID()
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

    def test_validateMainClass(self):
        valid_names = ["Class", "com.Class", "com.github.Class", "Main1Class", "1.2.Class"]
        invalid_names = [None, "invalid-class", "invalid..class", ".invalid.class", "InvalidClass."]
        for name in valid_names:
            self.assertEqual(self.spark.validateMainClass(name), name)
        for name in invalid_names:
            with self.assertRaises(ValueError):
                self.spark.validateMainClass(name)

    def test_validateJobOptions(self):
        # test non-list values
        with self.assertRaises(TypeError):
            self.spark.validateJobOptions({"a": 1})
        with self.assertRaises(TypeError):
            self.spark.validateJobOptions(None)
        with self.assertRaises(TypeError):
            self.spark.validateJobOptions("str")
        with self.assertRaises(TypeError):
            self.spark.validateJobOptions(1)
        # test valid list with mixed values
        raw = [1, True, "str"]
        expected = ["1", "True", "str"]
        self.assertEqual(self.spark.validateJobOptions(raw), expected)
        # test empty list
        self.assertEqual(self.spark.validateJobOptions([]), [])

    def test_validateSparkOptions(self):
        # test non-dict values
        with self.assertRaises(TypeError):
            self.spark.validateSparkOptions(["a", 1])
        with self.assertRaises(TypeError):
            self.spark.validateSparkOptions(None)
        with self.assertRaises(TypeError):
            self.spark.validateSparkOptions("str")
        with self.assertRaises(TypeError):
            self.spark.validateSparkOptions(1)
        # test valid dictionary with mixed values
        raw = {"spark.a.x": "1", "spark.a.y": True, "spark.a.z": 2, "a.b": "3"}
        expected = {"spark.a.x": "1", "spark.a.y": "True", "spark.a.z": "2"}
        self.assertEqual(self.spark.validateSparkOptions(raw), expected)
        # test empty dictionary
        self.assertEqual(self.spark.validateSparkOptions({}), {})

    def test_request_wrong_keys(self):
        with self.assertRaises(KeyError):
            self.spark.request()
        with self.assertRaises(KeyError):
            self.spark.request(spark_options=1, job_options=2, main_class=3)
        with self.assertRaises(KeyError):
            self.spark.request(spark_options=1, job_options=2, jar=4)
        with self.assertRaises(KeyError):
            self.spark.request(spark_options=1, main_class=3, jar=4)
        with self.assertRaises(KeyError):
            self.spark.request(job_options=2, main_class=3, jar=4)

    @mock.patch("src.spark.uuid.uuid4")
    @mock.patch("src.spark.util")
    def test_request(self, mock_util, mock_uuid4):
        # submission request expected directory
        req_dir = self.spark.working_directory + "/" + self.mock_uuid.hex
        # mock of utils and libs
        mock_util.readonlyFile.return_value = "/tmp/a.jar"
        mock_util.readwriteDirectory.return_value = req_dir
        mock_util.concat.return_value = req_dir
        mock_util.mkdir.return_value = None
        mock_uuid4.return_value = self.mock_uuid
        # create submission request
        res = self.spark.request(spark_options={"spark.a.b": "1"}, job_options=["a", "b", "c"],
                                 main_class="Class", jar="a.jar")
        self.assertTrue(isinstance(res, spark.SparkSubmissionRequest))
        self.assertEqual(res.spark_code, self.spark.code())
        self.assertEqual(res.main_class, "Class")
        self.assertEqual(res.jar, "/tmp/a.jar")
        self.assertEqual(res.spark_options, {"spark.a.b": "1"})
        self.assertEqual(res.job_options, ["a", "b", "c"])
        self.assertEqual(res.working_directory, req_dir)
        mock_util.readonlyFile.assert_called_with("a.jar")
        mock_util.concat.assert_called_with(self.spark.working_directory, self.mock_uuid.hex)
        mock_util.mkdir.assert_called_with(req_dir, 0774)
        mock_util.readwriteDirectory.assert_called_with(req_dir)

# Load test suites
def suites():
    return [
        SparkSubmissionRequestSuite,
        SparkBackendSuite
    ]
