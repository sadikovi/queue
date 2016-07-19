#!/usr/bin/env python

import cherrypy
import mock
import src.queue as queue
import src.util as util
from test.cptestcase import BaseCherryPyTestCase

def setUpModule():
    controller = queue.QueueController(args={}, logger=mock.Mock())
    cherrypy.tree.mount(controller, "/", queue.getConf())
    cherrypy.engine.start()
setup_module = setUpModule

def tearDownModule():
    cherrypy.engine.exit()
teardown_module = tearDownModule

# pylint: disable=W0212,protected-access
class QueueControllerSuite(BaseCherryPyTestCase):
    @mock.patch("src.queue.spark")
    def test_create_session(self, mock_spark):
        controller = queue.QueueController(args={}, logger=mock.Mock())
        with self.assertRaises(AttributeError):
            controller._create_session(None)
        with self.assertRaises(AttributeError):
            controller._create_session({"a": 1})
        conf = util.QueueConf()
        controller._create_session(conf)
        mock_spark.SparkSession.create.assert_called_once_with(conf, None)

    def test_template(self):
        self.assertEqual(queue.TEMPLATE_HOME, "home.html")

    def test_home(self):
        response = self.request("/")
        self.assertEqual(response.output_status, "200 OK")
        self.assertEqual(response.headers["Content-Type"], "text/html;charset=utf-8")
        self.assertTrue("<title>Home &middot; Queue</title>" in response.body[0])
# pylint: enable=W0212,protected-access

# Load test suites
def suites():
    return [
        QueueControllerSuite
    ]
