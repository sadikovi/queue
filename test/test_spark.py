#!/usr/bin/env python

import unittest
import mock
import src.const as const
import src.scheduler as scheduler
import src.spark as spark
import src.submission as submission

# pylint: disable=W0212,protected-access
class SparkStandaloneTaskSuite(unittest.TestCase):
    def setUp(self):
        self.logger = mock.Mock()

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

    @mock.patch("src.spark.applications")
    def test_can_submit_task_1(self, mock_apps):
        mock_apps.return_value = None
        self.assertEqual(spark.can_submit_task("url"), False)
        self.assertEqual(spark.can_submit_task("url", max_available_slots=3), False)

    @mock.patch("src.spark.applications")
    def test_can_submit_task_2(self, mock_apps):
        mock_apps.return_value = []
        self.assertEqual(spark.can_submit_task("url"), True)
        self.assertEqual(spark.can_submit_task("url", max_available_slots=3), True)

    @mock.patch("src.spark.applications")
    def test_can_submit_task_3(self, mock_apps):
        mock_apps.return_value = [{"completed": False}, {"completed": False}]
        self.assertEqual(spark.can_submit_task("url"), False)
        self.assertEqual(spark.can_submit_task("url", max_available_slots=3), True)

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
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        self.assertEqual(task.uid, "123")
        self.assertEqual(task.priority, const.PRIORITY_0)
        self.assertEqual(task._spark_submit, spark.SPARK_SUBMIT)
        self.assertEqual(task._master_url, spark.SPARK_MASTER_URL)
        self.assertEqual(task._web_url, spark.SPARK_WEB_URL)
        self.assertEqual(task.name, spark.SPARK_APP_NAME)
        self.assertEqual(task.spark_options, {})
        self.assertEqual(task.main_class, None)
        self.assertEqual(task.jar, None)
        self.assertEqual(task.job_options, [])

    @mock.patch("src.spark.util")
    def test_working_directory_1(self, mock_util):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        self.assertEqual(task.working_directory, None)
        # test setting working directory, should validate path
        mock_util.readwriteDirectory.side_effect = ValueError("Test")
        with self.assertRaises(ValueError):
            task.working_directory = "/failed/dir"

    @mock.patch("src.spark.util")
    def test_working_directory_2(self, mock_util):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        # should set fully resolved path
        mock_util.readwriteDirectory.return_value = "/tmp/work"
        task.working_directory = "work"
        self.assertEqual(task.working_directory, "/tmp/work")

    def test_working_directory_3(self):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.working_directory = None
        self.assertEqual(task.working_directory, None)

    def test_uid(self):
        task = spark.SparkStandaloneTask(None, const.PRIORITY_0, logger=self.logger)
        self.assertEqual(task.uid, None)

    def test_priority(self):
        task = spark.SparkStandaloneTask(None, const.PRIORITY_0, logger=self.logger)
        self.assertEqual(task.priority, const.PRIORITY_0)

    # Test of Spark cluster information: submit, master url, and web url
    @mock.patch("src.spark.util")
    def test_spark_submit(self, mock_util):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.spark_submit = spark.SPARK_SUBMIT
        self.assertEqual(task.spark_submit, spark.SPARK_SUBMIT)
        mock_util.readonlyFile.return_value = "/tmp/other-spark-submit"
        task.spark_submit = "other-spark-submit"
        self.assertEqual(task.spark_submit, "/tmp/other-spark-submit")

    def test_master_url(self):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.master_url = spark.SPARK_MASTER_URL
        self.assertEqual(task.master_url, spark.SPARK_MASTER_URL)
        with self.assertRaises(ValueError):
            task.master_url = spark.SPARK_WEB_URL
        with self.assertRaises(StandardError):
            task.master_url = None
        with self.assertRaises(StandardError):
            task.master_url = "abc"
        task.master_url = "spark://sandbox:7077"
        self.assertEqual(task.master_url, "spark://sandbox:7077")

    def test_web_url(self):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.web_url = spark.SPARK_WEB_URL
        self.assertEqual(task.web_url, spark.SPARK_WEB_URL)
        with self.assertRaises(StandardError):
            task.web_url = None
        with self.assertRaises(StandardError):
            task.web_url = "abc"
        task.web_url = "http://1.1.1.1:8000"
        self.assertEqual(task.web_url, "http://1.1.1.1:8000")

    @mock.patch("src.spark.util")
    def test_cmd(self, mock_util):
        # should create cmd from default task
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
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
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
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
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        # use defaults
        task.set_application()
        self.assertEqual(task.name, spark.SPARK_APP_NAME)
        self.assertEqual(task.main_class, None)
        self.assertEqual(task.jar, None)
        self.assertEqual(task.spark_options, {})
        self.assertEqual(task.job_options, [])

    @mock.patch("src.spark.util")
    def test_set_application_no_options(self, mock_util):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        # set application options
        mock_util.readonlyFile.return_value = "/tmp/file.jar"
        task.set_application(name="test", main_class="Class", jar="file.jar")
        self.assertEqual(task.name, "test")
        self.assertEqual(task.main_class, "Class")
        self.assertEqual(task.jar, "/tmp/file.jar")
        self.assertEqual(task.spark_options, {})
        self.assertEqual(task.job_options, [])

    def test_set_application_with_options(self):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.set_application(name="test", spark_options={"spark.a": 1, "b": 2},
                             job_options=["a", "b", 3])
        self.assertEqual(task.name, "test")
        self.assertEqual(task.spark_options, {"spark.a": "1"})
        self.assertEqual(task.job_options, ["a", "b", "3"])

    @mock.patch("src.spark.subprocess")
    def test_launch_process_no_stdout(self, mock_popen):
        mock_popen.Popen = mock.Mock() # redefine Popen object
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.working_directory = None
        task.launch_process()
        mock_popen.Popen.assert_called_with(task.cmd(), bufsize=4096, stdout=None, stderr=None,
                                            close_fds=True)

    @mock.patch("src.spark.subprocess")
    @mock.patch("src.spark.util.readwriteDirectory")
    @mock.patch("src.spark.util.open")
    def test_launch_process_with_stdout(self, mock_util_open, mock_rw_dir, mock_popen):
        mock_popen.Popen = mock.Mock() # redefine Popen object
        mock_util_open.return_value = "stream"
        mock_rw_dir.return_value = "/tmp/work"
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.working_directory = "work"
        task.launch_process()
        mock_popen.Popen.assert_called_with(task.cmd(), bufsize=4096, stdout="stream",
                                            stderr="stream", close_fds=True)
        calls = [mock.call("/tmp/work/stdout", "wb"), mock.call("/tmp/work/stderr", "wb")]
        mock_util_open.assert_has_calls(calls)

    @mock.patch("src.spark.time.sleep")
    def test_run_no_ps(self, mock_sleep):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.launch_process = mock.Mock()
        with self.assertRaises(AttributeError):
            task.run()
        mock_sleep.assert_called_with(task.timeout)

    @mock.patch("src.spark.time.sleep")
    @mock.patch("src.spark.subprocess.Popen")
    def test_run_exit_code_ok(self, mock_popen, mock_sleep):
        popen_instance = mock.Mock()
        popen_instance.poll.side_effect = [None, None, 0]
        mock_popen.return_value = popen_instance
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.run()
        self.assertEqual(mock_sleep.call_count, 3)
        mock_sleep.assert_called_with(task.timeout)

    @mock.patch("src.spark.time.sleep")
    @mock.patch("src.spark.subprocess.Popen")
    def test_run_exit_code_fail(self, mock_popen, mock_sleep):
        popen_instance = mock.Mock()
        popen_instance.poll.side_effect = [None, None, 127]
        mock_popen.return_value = popen_instance
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        with self.assertRaises(IOError):
            task.run()
        mock_sleep.assert_called_with(task.timeout)

    def test_cancel_no_ps(self):
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        self.assertEqual(task.cancel(), None)

    @mock.patch("src.spark.subprocess")
    @mock.patch("src.spark.time")
    def test_cancel_with_ps_no_exitcode(self, mock_time, mock_popen):
        mock_time.sleep.return_value = None # remove sleep function
        mock_popen.Popen = mock.Mock() # redefine Popen object
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.working_directory = None
        task.launch_process()
        task._current_ps().poll.return_value = 127
        exit_code = task.cancel()
        self.assertEqual(exit_code, 127)
        task._current_ps().terminate.assert_called_with()
        task._current_ps().kill.assert_has_calls([])

    @mock.patch("src.spark.subprocess")
    @mock.patch("src.spark.time")
    def test_cancel_with_ps_with_exitcode(self, mock_time, mock_popen):
        mock_time.sleep.return_value = None # remove sleep function
        mock_popen.Popen = mock.Mock() # redefine Popen object
        task = spark.SparkStandaloneTask("123", const.PRIORITY_0, logger=self.logger)
        task.working_directory = None
        task.launch_process()
        task._current_ps().poll.return_value = None
        task._current_ps().wait.return_value = None
        exit_code = task.cancel()
        self.assertEqual(exit_code, None)
        task._current_ps().terminate.assert_called_with()
        task._current_ps().kill.assert_called_with()
# pylint: enable=W0212,protected-access

class SparkStandaloneExecutorSuite(unittest.TestCase):
    def setUp(self):
        self.queue_map = {}
        self.conn = mock.Mock()
        self.logger = mock.Mock()

    def test_init_1(self):
        exc = spark.SparkStandaloneExecutor("a", self.conn, self.queue_map, logger=self.logger)
        self.assertEqual(exc.timeout, 1.0)
        self.assertEqual(exc.master_url, spark.SPARK_MASTER_URL)
        self.assertEqual(exc.web_url, spark.SPARK_WEB_URL)
        self.assertEqual(exc.max_available_slots, 1)

    def test_init_2(self):
        exc = spark.SparkStandaloneExecutor("a", self.conn, self.queue_map, timeout=5.0,
                                            logger=self.logger)
        self.assertEqual(exc.timeout, 5.0)

    def test_external_system_available(self):
        spark.can_submit_task = mock.Mock()
        exc = spark.SparkStandaloneExecutor("a", self.conn, self.queue_map, logger=self.logger)
        exc.external_system_available()
        spark.can_submit_task.assert_called_once_with(exc.web_url, exc.max_available_slots)

class SparkStandaloneSchedulerSuite(unittest.TestCase):
    def setUp(self):
        self.logger = mock.Mock()
        self.master_url = "spark://SANDBOX:7077"
        self.web_url = "http://LOCALHOST:8080"

    def test_init(self):
        sched = spark.SparkStandaloneScheduler(self.master_url, self.web_url, 5, 1.0, self.logger)
        self.assertEqual(sched.master_url, self.master_url)
        self.assertEqual(sched.web_url, self.web_url)
        self.assertEqual(sched.max_available_slots, 5)

    def test_executor_class(self):
        sched = spark.SparkStandaloneScheduler(self.master_url, self.web_url, 5, 1.0, self.logger)
        self.assertEqual(sched.executor_class(), spark.SparkStandaloneExecutor)

    def test_update_executor(self):
        sched = spark.SparkStandaloneScheduler(self.master_url, self.web_url, 5, 1.0, self.logger)
        mock_executor = mock.create_autospec(spark.SparkStandaloneExecutor)
        sched.update_executor(mock_executor)
        self.assertEqual(mock_executor.master_url, self.master_url)
        self.assertEqual(mock_executor.web_url, self.web_url)
        self.assertEqual(mock_executor.max_available_slots, 5)

class SparkSessionSuite(unittest.TestCase):
    def setUp(self):
        self.master_url = "spark://master:7077"
        self.web_url = "http://localhost:8080"
        self.working_dir = "./work"

    def test_init(self):
        with self.assertRaises(StandardError):
            spark.SparkSession("abc", self.web_url, self.working_dir)
        with self.assertRaises(StandardError):
            spark.SparkSession(self.master_url, "abc", self.working_dir)
        with self.assertRaises(ValueError):
            spark.SparkSession(self.master_url, self.web_url, self.working_dir, num_executors="abc")
        with self.assertRaises(ValueError):
            spark.SparkSession(self.master_url, self.web_url, self.working_dir, num_executors=1,
                               timeout="abc")
        with self.assertRaises(AttributeError):
            spark.SparkSession(self.master_url, self.web_url, self.working_dir, num_executors=0)
        with self.assertRaises(AttributeError):
            spark.SparkSession(self.master_url, self.web_url, self.working_dir, num_executors=1,
                               timeout=0.0)
        # valid instance
        session = spark.SparkSession(self.master_url, self.web_url, self.working_dir)
        self.assertEqual(session.master_url, self.master_url)
        self.assertEqual(session.web_url, self.web_url)
        self.assertEqual(session.working_dir, self.working_dir)
        self.assertEqual(session.num_executors, 1)
        self.assertEqual(session.timeout, 1.0)
        self.assertNotEqual(session.scheduler, None)

    def test_system_code(self):
        session = spark.SparkSession(self.master_url, self.web_url, self.working_dir)
        self.assertEqual(session.system_code(), spark.SPARK_SYSTEM_CODE)

    def test_system_uri(self):
        session = spark.SparkSession(self.master_url, self.web_url, self.working_dir)
        self.assertEqual(session.system_uri().url, self.web_url)
        self.assertEqual(session.system_uri().alias, "Spark Web UI")

    def test_scheduler(self):
        session = spark.SparkSession(self.master_url, self.web_url, self.working_dir)
        self.assertTrue(isinstance(session.scheduler, scheduler.Scheduler))

    @mock.patch("src.spark.applications")
    def test_status(self, mock_applications):
        session = spark.SparkSession(self.master_url, self.web_url, self.working_dir)
        mock_applications.return_value = None
        self.assertEqual(session.status(), const.SYSTEM_UNAVAILABLE)
        mock_applications.return_value = []
        self.assertEqual(session.status(), const.SYSTEM_AVAILABLE)
        mock_applications.return_value = [{"completed": True}, {"completed": True}]
        self.assertEqual(session.status(), const.SYSTEM_AVAILABLE)
        mock_applications.return_value = [{"completed": True}, {"completed": False}]
        self.assertEqual(session.status(), const.SYSTEM_BUSY)

    def test_create(self):
        conf = mock.Mock()
        conf.getConfString.side_effect = [self.master_url, self.web_url]
        conf.getConfInt.return_value = 1
        conf.getConfFloat.return_value = 1.0
        session = spark.SparkSession.create(conf, self.working_dir, mock.Mock())
        self.assertEqual(session.master_url, self.master_url)
        self.assertEqual(session.web_url, self.web_url)
        self.assertEqual(session.working_dir, self.working_dir)
        self.assertEqual(session.num_executors, 1)
        self.assertEqual(session.timeout, 1.0)

    @mock.patch("src.spark.util.readwriteDirectory")
    @mock.patch("src.spark.util.mkdir")
    def test_create_task(self, mock_mkdir, mock_dir):
        session = spark.SparkSession(self.master_url, self.web_url, self.working_dir)
        with self.assertRaises(AttributeError):
            session.create_task(None)
        with self.assertRaises(AttributeError):
            session.create_task("abc")
        template = mock.create_autospec(submission.Submission, is_template=True)
        with self.assertRaises(ValueError):
            session.create_task(template)
        del_sub = mock.create_autospec(submission.Submission, is_template=False, is_deleted=True)
        with self.assertRaises(ValueError):
            session.create_task(del_sub)
        sub = mock.create_autospec(submission.Submission, is_deleted=False, is_template=False,
                                   priority=const.PRIORITY_0, uid="123", payload={},
                                   system_code=spark.SPARK_SYSTEM_CODE)
        sub.name = "abc"
        mock_dir.return_value = "/mock-work-dir/123"
        task = session.create_task(sub)
        self.assertEqual(task.master_url, self.master_url)
        self.assertEqual(task.web_url, self.web_url)
        self.assertEqual(task.name, "abc")
        self.assertEqual(task.working_directory, "/mock-work-dir/123")
        mock_mkdir.assert_called_once_with("./work/123", 0775)
        mock_dir.assert_called_once_with("./work/123")

    @mock.patch("src.spark.util.readwriteDirectory")
    @mock.patch("src.spark.util.mkdir")
    def test_create_task_fail_sys_code(self, mock_mkdir, mock_dir):
        session = spark.SparkSession(self.master_url, self.web_url, self.working_dir)
        sub = mock.create_autospec(submission.Submission, is_template=True, system_code="ABC")
        with self.assertRaises(ValueError):
            session.create_task(sub)

# Load test suites
def suites():
    return [
        SparkStandaloneTaskSuite,
        SparkStandaloneExecutorSuite,
        SparkStandaloneSchedulerSuite,
        SparkSessionSuite
    ]
