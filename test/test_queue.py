#!/usr/bin/env python

import unittest
import cherrypy
import mock
import src.const as const
import src.queue as queue
import src.simple as simple
import src.spark as spark
import src.util as util
from test.cptestcase import BaseCherryPyTestCase

# == Test session + scheduler ==
test_scheduler = mock.create_autospec(spark.SparkStandaloneScheduler, spec_set=True, instance=True)
test_scheduler.get_num_executors.return_value = 5
test_scheduler.executor_class.return_value = spark.SparkStandaloneExecutor
test_scheduler.get_metrics.return_value = {"a": 1, "b": 2}
test_scheduler.get_is_alive_statuses.return_value = {"ex1": util.utcnow(), "ex2": util.utcnow()}

test_session = mock.create_autospec(spark.SparkSession, spec_set=True, instance=True)
test_session.system_code.return_value = "TEST"
test_session.system_uri.return_value = util.URI("http://local:8080", "link")
test_session.status.return_value = const.SYSTEM_BUSY
test_session.scheduler = test_scheduler

@mock.patch("src.queue.util")
def setUpModule(mock_util):
    # for testing web-service functionality only we mock session, util in controller
    mock_util.readwriteDirectory.return_value = "/None"
    mock_util.readonlyDirectory.return_value = "/None"
    with mock.patch.object(queue.QueueController, "_create_session", return_value=test_session):
        controller = queue.QueueController(args={}, logger=mock.Mock())
        cherrypy.tree.mount(controller, "/", queue.getConf())
    cherrypy.engine.start()
setup_module = setUpModule

def tearDownModule():
    cherrypy.engine.exit()
teardown_module = tearDownModule

class QueueSuite(BaseCherryPyTestCase):
    """
    Testing suite for web-service functionality only. All internal methods that are not related to
    REST API or serving web-pages, should be in controller suite.
    """
    def test_template(self):
        self.assertEqual(queue.TEMPLATE_HOME, "home.html")
        self.assertEqual(queue.TEMPLATE_STATUS, "status.html")

    def test_home(self):
        response = self.request("/")
        self.assertEqual(response.output_status, "200 OK")
        self.assertEqual(response.headers["Content-Type"], "text/html;charset=utf-8")
        self.assertTrue("<title>Home &middot; Queue</title>" in response.body[0])

    def test_status(self):
        response = self.request("/status")
        self.assertEqual(response.output_status, "200 OK")
        self.assertEqual(response.headers["Content-Type"], "text/html;charset=utf-8")
        self.assertTrue("<title>Status &middot; Queue</title>" in response.body[0])

# pylint: disable=W0212,protected-access,W0613,unused-argument
class QueueControllerSuite(unittest.TestCase):
    def setUp(self):
        test_scheduler.reset_mock()
        test_session.reset_mock()
        self.args = {const.OPT_SYSTEM_CODE: spark.SPARK_SYSTEM_CODE}

    @mock.patch("src.queue.util.readwriteDirectory")
    @mock.patch("src.queue.util.readonlyDirectory")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_create_session(self, mock_session, mock_r, mock_rw):
        controller = queue.QueueController(args=self.args, logger=mock.Mock())
        with self.assertRaises(AttributeError):
            controller._create_session(None)
        with self.assertRaises(AttributeError):
            controller._create_session({"a": 1})
        conf = util.QueueConf()
        with self.assertRaises(StandardError):
            controller._create_session(conf)
        # check spark session
        conf.setAllConf(self.args)
        session = controller._create_session(conf)
        self.assertTrue(isinstance(session, spark.SparkSession))
        # check simple session
        conf = util.QueueConf()
        conf.setConf(const.OPT_SYSTEM_CODE, simple.SIMPLE_SYSTEM_CODE)
        conf.setConf(const.OPT_SCHEDULER_TIMEOUT, 1.0)
        conf.setConf(const.OPT_NUM_PARALLEL_TASKS, 1)
        session = controller._create_session(conf)
        self.assertTrue(isinstance(session, simple.SimpleSession))

    @mock.patch("src.queue.util.readwriteDirectory")
    @mock.patch("src.queue.util.readonlyDirectory")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_pretty_name(self, mock_session, mock_r, mock_rw):
        controller = queue.QueueController(args=self.args, logger=mock.Mock())
        self.assertEqual(controller._pretty_name(controller), "QueueController")
        self.assertEqual(controller._pretty_name(queue.QueueController), "QueueController")

    @mock.patch("src.queue.util.readwriteDirectory")
    @mock.patch("src.queue.util.readonlyDirectory")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_get_status_dict(self, mock_session, mock_r, mock_rw):
        controller = queue.QueueController(args=self.args, logger=mock.Mock())
        metrics = controller.get_status_dict()
        self.assertEqual(metrics["code"], "TEST")
        self.assertEqual(metrics["url"], {"href": "http://local:8080", "alias": "link"})
        self.assertEqual(metrics["status"], const.SYSTEM_BUSY)
        self.assertEqual(metrics["scheduler"]["executor_class"], "SparkStandaloneExecutor")
        self.assertEqual(metrics["scheduler"]["num_executors"], 5)
        self.assertEqual(metrics["scheduler"]["metrics"], {"a": 1, "b": 2})
        self.assertTrue("ex1" in metrics["scheduler"]["is_alive_statuses"])
        self.assertTrue("ex2" in metrics["scheduler"]["is_alive_statuses"])

    @mock.patch("src.queue.util.readwriteDirectory")
    @mock.patch("src.queue.util.readonlyDirectory")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_start(self, mock_session, mock_r, mock_rw):
        controller = queue.QueueController(args=self.args, logger=mock.Mock())
        controller.start()
        test_scheduler.start_maintenance.assert_called_once_with()
        test_scheduler.start.assert_called_once_with()

    @mock.patch("src.queue.util.readwriteDirectory")
    @mock.patch("src.queue.util.readonlyDirectory")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_stop(self, mock_session, mock_r, mock_rw):
        controller = queue.QueueController(args=self.args, logger=mock.Mock())
        controller.stop()
        test_scheduler.stop.assert_called_once_with()
# pylint: enable=W0212,protected-access,W0613,unused-argument

# Load test suites
def suites():
    return [
        QueueSuite,
        QueueControllerSuite
    ]
