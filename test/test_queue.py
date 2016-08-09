#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# Copyright 2016 sadikovi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import unittest
import cherrypy
import mock
import src.const as const
import src.queue as queue
import src.simple as simple
import src.spark as spark
import src.util as util
from test.cptestcase import BaseCherryPyTestCase

# == Test task + session + scheduler ==
test_task = mock.create_autospec(spark.SparkStandaloneTask, spec_set=True, instance=True)

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
test_session.create_task.return_value = test_task

queue.logger = mock.Mock()
cherrypy.log = mock.Mock()

# mock publish + subscribe services
cherrypy.engine.publish = mock.Mock()
cherrypy.engine.subscribe = mock.Mock()

@mock.patch("src.queue.util")
@mock.patch("src.queue.pymongo")
def setUpModule(mock_pymongo, mock_util):
    # for testing web-service functionality only we mock session, util in controller
    mock_pymongo.MongoClient = mock.Mock()
    mock_util.readwriteDirectory.return_value = "/None"
    mock_util.readonlyDirectory.return_value = "/None"
    with mock.patch.object(queue.QueueController, "_create_session", return_value=test_session):
        controller = queue.QueueController(args={})
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

    def test_rest_submission_put_1(self):
        response = self.request("/api/submission", method="PUT", data="{abc}",
                                headers={"Content-Type": "application/json"})
        self.assertEqual(response.output_status, "400 Bad Request")
        self.assertEqual(response.headers["Content-Type"], "text/html;charset=utf-8")
        self.assertTrue("<p>Invalid JSON document</p>" in response.body[0])

    def test_rest_submission_put_2(self):
        response = self.request(
            "/api/submission",
            method="PUT",
            data="""{"key": "value"}""",
            headers={"Content-Type": "application/json"})
        self.assertEqual(response.output_status, "400 Bad Request")
        self.assertEqual(response.headers["Content-Type"], "application/json")
        self.assertTrue("Expected required field" in response.body[0])

    def test_rest_submission_put_3(self):
        response = self.request(
            "/api/submission",
            method="PUT",
            data="""{"name": "test"}""",
            headers={"Content-Type": "application/json"})
        self.assertEqual(response.output_status, "400 Bad Request")
        self.assertEqual(response.headers["Content-Type"], "application/json")
        self.assertTrue("Expected required field 'code'" in response.body[0])

    def test_rest_submission_put_4(self):
        response = self.request(
            "/api/submission",
            method="PUT",
            data="""{"name": "test", "code": "simple"}""",
            headers={"Content-Type": "application/json"})
        self.assertEqual(response.output_status, "200 OK")
        self.assertEqual(response.headers["Content-Type"], "application/json")
        answer = json.loads(response.body[0])
        self.assertEqual(answer["status"], "OK")
        self.assertEqual(answer["code"], 200)
        self.assertEqual(answer["data"]["msg"], "Submission created")
        self.assertTrue("uid" in answer["data"])

# pylint: disable=W0212,protected-access,W0613,unused-argument
class QueueControllerSuite(unittest.TestCase):
    def setUp(self):
        test_scheduler.reset_mock()
        test_session.reset_mock()
        self.args = {const.OPT_SYSTEM_CODE: spark.SPARK_SYSTEM_CODE}

    def test_rest_json_out(self):
        def test1():
            raise StandardError("test")
        self.assertEqual(
            queue.rest_json_out(test1).__call__(),
            {"status": "ERROR", "msg": "test", "code": 400})
        def test2():
            raise Exception("test")
        self.assertEqual(
            queue.rest_json_out(test2).__call__(),
            {"status": "ERROR", "msg": "test", "code": 500})
        def test3():
            return {"key": "value"}
        self.assertEqual(
            queue.rest_json_out(test3).__call__(),
            {"status": "OK", "data": {"key": "value"}, "code": 200})

    @mock.patch("src.queue.util")
    @mock.patch("src.queue.pymongo")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_validate(self, mock_session, mock_pymongo, mock_util):
        controller = queue.QueueController(args=self.args)
        mock_util.URI.side_effect = ValueError()
        mock_util.readwriteDirectory.side_effect = ValueError()
        mock_util.readonlyDirectory.side_effect = ValueError()
        # test for all validations in controller
        with self.assertRaises(StandardError):
            controller._validate_mongodb_url("abc")
        with self.assertRaises(StandardError):
            controller._validate_working_dir("abc")
        with self.assertRaises(StandardError):
            controller._validate_service_dir("abc")

    @mock.patch("src.queue.util")
    @mock.patch("src.queue.pymongo")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_create_session(self, mock_session, mock_pymongo, mock_util):
        controller = queue.QueueController(args=self.args)
        with self.assertRaises(AttributeError):
            controller._create_session(None)
        with self.assertRaises(AttributeError):
            controller._create_session({"a": 1})
        conf = queue.QueueConf()
        with self.assertRaises(StandardError):
            controller._create_session(conf)
        # check spark session
        conf.setAllConf(self.args)
        session = controller._create_session(conf)
        self.assertTrue(isinstance(session, spark.SparkSession))
        # check simple session
        conf = queue.QueueConf()
        conf.setConf(const.OPT_SYSTEM_CODE, simple.SIMPLE_SYSTEM_CODE)
        conf.setConf(const.OPT_SCHEDULER_TIMEOUT, 1.0)
        conf.setConf(const.OPT_NUM_PARALLEL_TASKS, 1)
        session = controller._create_session(conf)
        self.assertTrue(isinstance(session, simple.SimpleSession))

    @mock.patch("src.queue.util")
    @mock.patch("src.queue.pymongo")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_pretty_name(self, mock_session, mock_pymongo, mock_util):
        controller = queue.QueueController(args=self.args)
        self.assertEqual(controller._pretty_name(controller), "QueueController")
        self.assertEqual(controller._pretty_name(queue.QueueController), "QueueController")

    @mock.patch("src.queue.util")
    @mock.patch("src.queue.pymongo")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_get_status_dict(self, mock_session, mock_pymongo, mock_util):
        controller = queue.QueueController(args=self.args)
        metrics = controller.get_status_dict()
        self.assertEqual(metrics["code"], "TEST")
        self.assertEqual(metrics["url"], {"href": "http://local:8080", "alias": "link"})
        self.assertEqual(metrics["status"], const.SYSTEM_BUSY)
        self.assertEqual(metrics["scheduler"]["executor_class"], "SparkStandaloneExecutor")
        self.assertEqual(metrics["scheduler"]["num_executors"], 5)
        self.assertEqual(metrics["scheduler"]["metrics"], {"a": 1, "b": 2})
        self.assertTrue("ex1" in metrics["scheduler"]["is_alive_statuses"])
        self.assertTrue("ex2" in metrics["scheduler"]["is_alive_statuses"])
        self.assertTrue("storage_url" in metrics)

    @mock.patch("src.queue.util")
    @mock.patch("src.queue.pymongo")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_start(self, mock_session, mock_pymongo, mock_util):
        controller = queue.QueueController(args=self.args)
        controller.start()
        controller.session.scheduler.start_maintenance.assert_called_once_with()
        controller.session.scheduler.start.assert_called_once_with()
        self.assertTrue(controller.client.queue.submissions.create_index.called)
        self.assertTrue(controller.client.queue.tasks.create_index.called)

    @mock.patch("src.queue.util")
    @mock.patch("src.queue.pymongo")
    @mock.patch.object(spark.SparkSession, "create", return_value=test_session)
    def test_stop(self, mock_session, mock_pymongo, mock_util):
        test_mongo_client = mock.Mock()
        mock_pymongo.MongoClient = test_mongo_client
        controller = queue.QueueController(args=self.args)
        controller.stop()
        controller.session.scheduler.stop.assert_called_once_with()
        controller.client.close.assert_called_once_with()

# pylint: enable=W0212,protected-access,W0613,unused-argument

class QueueConfSuite(unittest.TestCase):
    def test_set_conf(self):
        conf = queue.QueueConf()
        with self.assertRaises(AttributeError):
            conf.setConf(None, None)
        with self.assertRaises(AttributeError):
            conf.setConf("", None)
        # non-empty key and None value should succeed
        conf.setConf("key", None)
        self.assertEqual(conf.getConf("key"), None)

    def test_set_all_conf(self):
        conf = queue.QueueConf()
        with self.assertRaises(TypeError):
            conf.setAllConf(None)
        with self.assertRaises(TypeError):
            conf.setAllConf([])
        conf.setAllConf({"a": 1, "b": True})
        self.assertEqual(conf.getConf("a"), 1)
        self.assertEqual(conf.getConf("b"), True)

    def test_get_conf(self):
        conf = queue.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        self.assertEqual(conf.getConf("a"), 1)
        self.assertEqual(conf.getConf("b"), None)
        self.assertEqual(conf.getConf("c"), None)

    def test_get_conf_string(self):
        conf = queue.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        self.assertEqual(conf.getConfString("a"), "1")
        self.assertEqual(conf.getConfString("b"), "None")
        self.assertEqual(conf.getConfString("c"), "None")

    def test_get_conf_boolean(self):
        conf = queue.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        conf.setConf("c", False)
        self.assertEqual(conf.getConfBoolean("a"), True)
        self.assertEqual(conf.getConfBoolean("b"), False)
        self.assertEqual(conf.getConfBoolean("c"), False)

    def test_get_conf_int(self):
        conf = queue.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", "2")
        conf.setConf("c", None)
        conf.setConf("d", False)
        self.assertEqual(conf.getConfInt("a"), 1)
        self.assertEqual(conf.getConfInt("b"), 2)
        with self.assertRaises(TypeError):
            conf.getConfInt("c")
        self.assertEqual(conf.getConfInt("d"), 0)

    def test_get_conf_float(self):
        conf = queue.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", "2.0")
        conf.setConf("c", None)
        conf.setConf("d", False)
        self.assertEqual(conf.getConfFloat("a"), 1.0)
        self.assertEqual(conf.getConfFloat("b"), 2.0)
        with self.assertRaises(TypeError):
            conf.getConfFloat("c")
        self.assertEqual(conf.getConfFloat("d"), 0.0)

    def test_contains(self):
        conf = queue.QueueConf()
        conf.setConf("a", 1)
        conf.setConf("b", None)
        self.assertEqual(conf.contains("a"), True)
        self.assertEqual(conf.contains("b"), True)
        self.assertEqual(conf.contains("c"), False)

    def test_parse(self):
        self.assertEqual(queue.QueueConf.parse(None), {})
        self.assertEqual(queue.QueueConf.parse(""), {})
        self.assertEqual(queue.QueueConf.parse("a=1 b=2 c='3 4'"), {"a": "1", "b": "2", "c": "3 4"})
        self.assertEqual(queue.QueueConf.parse("a=1 b= c"), {"a": "1", "b": ""})
        self.assertEqual(queue.QueueConf.parse("a=1 b=2=3"), {"a": "1", "b": "2=3"})

    def test_copy(self):
        conf = queue.QueueConf()
        opts = {"a": 1, "b": True, "c": 1.0}
        conf.setAllConf(opts)
        self.assertEqual(conf.copy(), opts)

# Load test suites
def suites():
    return [
        QueueSuite,
        QueueControllerSuite,
        QueueConfSuite
    ]
