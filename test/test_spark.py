#!/usr/bin/env python

import unittest
import mock
import src.spark as spark

class SparkStandaloneTaskSuite(unittest.TestCase):
    # Test of general funcionality
    def test_defaults(self):
        self.assertEqual(spark.SPARK_SYSTEM_CODE, "SPARK")
        self.assertEqual(spark.SPARK_SUBMIT, "spark-submit")
        self.assertEqual(spark.SPARK_MASTER_URL, "spark://master:7077")
        self.assertEqual(spark.SPARK_WEB_URL, "http://localhost:8080")
        self.assertEqual(spark.SPARK_APP_NAME, "SPARK QUEUE APP")

    @mock.patch("src.spark.urllib2")
    def test_applications_fail_request(self, mock_urllib2):
        mock_urllib2.urlopen.side_effect = StandardError("Test")
        self.assertEqual(spark.applications("dummy"), None)

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_fail_json(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = mock.Mock()
        mock_json.loads.side_effect = ValueError("Test")
        with self.assertRaises(ValueError):
            spark.applications("url")

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_no_data(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = mock.Mock()
        mock_json.loads.return_value = []
        self.assertEqual(spark.applications("url"), [])

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_invalid_data(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = mock.Mock()
        mock_json.loads.return_value = [
            {"a": "2", "b": "abc", "c": [{"completed": True}]}
        ]
        self.assertEqual(spark.applications("url"), [])

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_no_running(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = mock.Mock()
        mock_json.loads.return_value = [
            {"id": "1", "name": "abc", "attempts": [{"completed": True}, {"completed": True}]},
            {"id": "2", "name": "abc", "attempts": [{"completed": True}]}
        ]
        self.assertEqual(spark.applications("url"), [
            {"id": "1", "name": "abc", "completed": True},
            {"id": "2", "name": "abc", "completed": True}
        ])

    @mock.patch("src.spark.urllib2")
    @mock.patch("src.spark.json")
    def test_applications_running(self, mock_json, mock_urllib2):
        mock_urllib2.urlopen.return_value = mock.Mock()
        mock_json.loads.return_value = [
            {"id": "1", "name": "abc", "attempts": [{"completed": True}, {"completed": False}]},
            {"id": "2", "name": "abc", "attempts": [{"completed": True}]}
        ]
        self.assertEqual(spark.applications("url"), [
            {"id": "1", "name": "abc", "completed": False},
            {"id": "2", "name": "abc", "completed": True}
        ])

    # Test of validation
    def test_validate_main_class(self):
        valid_names = ["Class", "com.Class", "com.github.Class", "Main1Class", "1.2.Class"]
        invalid_names = [None, "invalid-class", "invalid..class", ".invalid.class", "InvalidClass."]
        for name in valid_names:
            self.assertEqual(spark.validate_main_class(name), name)
        for name in invalid_names:
            with self.assertRaises(ValueError):
                spark.validate_main_class(name)

    def test_validate_job_options(self):
        # test non-list values
        with self.assertRaises(TypeError):
            spark.validate_job_options({"a": 1})
        with self.assertRaises(TypeError):
            spark.validate_job_options(None)
        with self.assertRaises(TypeError):
            spark.validate_job_options("str")
        with self.assertRaises(TypeError):
            spark.validate_job_options(1)
        # test valid list with mixed values
        raw = [1, True, "str"]
        expected = ["1", "True", "str"]
        self.assertEqual(spark.validate_job_options(raw), expected)
        # test empty list
        self.assertEqual(spark.validate_job_options([]), [])

    def test_validate_spark_options(self):
        # test non-dict values
        with self.assertRaises(TypeError):
            spark.validate_spark_options(["a", 1])
        with self.assertRaises(TypeError):
            spark.validate_spark_options(None)
        with self.assertRaises(TypeError):
            spark.validate_spark_options("str")
        with self.assertRaises(TypeError):
            spark.validate_spark_options(1)
        # test valid dictionary with mixed values
        raw = {"spark.a.x": "1", "spark.a.y": True, "spark.a.z": 2, "a.b": "3"}
        expected = {"spark.a.x": "1", "spark.a.y": "True", "spark.a.z": "2"}
        self.assertEqual(spark.validate_spark_options(raw), expected)
        # test empty dictionary
        self.assertEqual(spark.validate_spark_options({}), {})

    def test_init(self):
        mock_logger = mock.Mock()
        task = spark.SparkStandaloneTask("123", logger=mock_logger)
        # pylint: disable=W0212,protected-access
        self.assertEqual(task.uid, "123")
        self.assertEqual(task.logger, mock_logger)
        self.assertEqual(task._spark_submit, spark.SPARK_SUBMIT)
        self.assertEqual(task._master_url, spark.SPARK_MASTER_URL)
        self.assertEqual(task._web_url, spark.SPARK_WEB_URL)
        self.assertEqual(task.name, spark.SPARK_APP_NAME)
        self.assertEqual(task.spark_options, {})
        self.assertEqual(task.main_class, None)
        self.assertEqual(task.jar, None)
        self.assertEqual(task.job_options, [])
        # pylint: enable=W0212,protected-access

    @mock.patch("src.spark.util")
    def test_working_directory_1(self, mock_util):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        self.assertEqual(task.working_directory, None)
        # test setting working directory, should validate path
        mock_util.readwriteDirectory.side_effect = ValueError("Test")
        with self.assertRaises(ValueError):
            task.working_directory = None

    @mock.patch("src.spark.util")
    def test_working_directory_2(self, mock_util):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        # should set fully resolved path
        mock_util.readwriteDirectory.return_value = "/tmp/work"
        task.working_directory = "work"
        self.assertEqual(task.working_directory, "/tmp/work")

    def test_uid(self):
        task = spark.SparkStandaloneTask(None, logger=mock.Mock())
        self.assertEqual(task.uid, None)

    def test_exit_code(self):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        self.assertEqual(task.exit_code, None)

    # Test of Spark cluster information: submit, master url, and web url
    @mock.patch("src.spark.util")
    def test_spark_submit(self, mock_util):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        task.spark_submit = spark.SPARK_SUBMIT
        self.assertEqual(task.spark_submit, spark.SPARK_SUBMIT)
        mock_util.readonlyFile.return_value = "/tmp/other-spark-submit"
        task.spark_submit = "other-spark-submit"
        self.assertEqual(task.spark_submit, "/tmp/other-spark-submit")

    def test_master_url(self):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        task.master_url = spark.SPARK_MASTER_URL
        self.assertEqual(task.master_url, spark.SPARK_MASTER_URL)
        with self.assertRaises(ValueError):
            task.master_url = spark.SPARK_WEB_URL
        with self.assertRaises(AttributeError):
            task.master_url = None
        with self.assertRaises(StandardError):
            task.master_url = "abc"
        task.master_url = "spark://sandbox:7077"
        self.assertEqual(task.master_url, "spark://sandbox:7077")

    def test_web_url(self):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        task.web_url = spark.SPARK_WEB_URL
        self.assertEqual(task.web_url, spark.SPARK_WEB_URL)
        with self.assertRaises(AttributeError):
            task.web_url = None
        with self.assertRaises(StandardError):
            task.web_url = "abc"
        task.web_url = "http://1.1.1.1:8000"
        self.assertEqual(task.web_url, "http://1.1.1.1:8000")

    @mock.patch("src.spark.util")
    def test_cmd(self, mock_util):
        # should create cmd from default task
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        answer = [
            spark.SPARK_SUBMIT,
            "--master", spark.SPARK_MASTER_URL,
            "--name", spark.SPARK_APP_NAME,
            "--class", "None",
            "None"
        ]
        self.assertEqual(task.cmd(), answer)
        # should create cmd with arbitrary options
        mock_util.readonlyFile.return_value = "/tmp/file"
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        task.spark_options = {"spark.a": 1}
        task.job_options = ["a", "b"]
        task.main_class = "Class"
        task.jar = "jar"
        answer = [
            spark.SPARK_SUBMIT,
            "--master", spark.SPARK_MASTER_URL,
            "--name", spark.SPARK_APP_NAME,
            "--conf", "spark.a=1",
            "--class", "Class",
            "jar",
            "a", "b"
        ]
        self.assertEqual(task.cmd(), answer)

    def test_set_application_empty(self):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        # use defaults
        task.set_application()
        self.assertEqual(task.name, spark.SPARK_APP_NAME)
        self.assertEqual(task.main_class, None)
        self.assertEqual(task.jar, None)
        self.assertEqual(task.spark_options, {})
        self.assertEqual(task.job_options, [])

    @mock.patch("src.spark.util")
    def test_set_application_no_options(self, mock_util):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        # set application options
        mock_util.readonlyFile.return_value = "/tmp/file.jar"
        task.set_application(name="test", main_class="Class", jar="file.jar")
        self.assertEqual(task.name, "test")
        self.assertEqual(task.main_class, "Class")
        self.assertEqual(task.jar, "/tmp/file.jar")
        self.assertEqual(task.spark_options, {})
        self.assertEqual(task.job_options, [])

    def test_set_application_with_options(self):
        task = spark.SparkStandaloneTask("123", logger=mock.Mock())
        task.set_application(name="test", spark_options={"spark.a": 1, "b": 2},
                             job_options=["a", "b", 3])
        self.assertEqual(task.name, "test")
        self.assertEqual(task.spark_options, {"spark.a": "1"})
        self.assertEqual(task.job_options, ["a", "b", "3"])

# Load test suites
def suites():
    return [
        SparkStandaloneTaskSuite
    ]
