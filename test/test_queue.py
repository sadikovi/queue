#!/usr/bin/env python

import cherrypy
import mock
import src.queue as queue
from test.cptestcase import BaseCherryPyTestCase

def setUpModule():
    controller = queue.QueueController(args={}, logger=mock.Mock())
    cherrypy.tree.mount(controller, "/", queue.getConf())
    cherrypy.engine.start()
setup_module = setUpModule

def tearDownModule():
    cherrypy.engine.exit()
teardown_module = tearDownModule

class QueueControllerSuite(BaseCherryPyTestCase):
    def test_template(self):
        self.assertEqual(queue.TEMPLATE_HOME, "home.html")

    def test_home(self):
        response = self.request("/")
        self.assertEqual(response.output_status, "200 OK")
        self.assertEqual(response.headers["Content-Type"], "text/html;charset=utf-8")
        self.assertTrue("<title>Home &middot; Queue</title>" in response.body[0])

# Load test suites
def suites():
    return [
        QueueControllerSuite
    ]
