#!/usr/bin/env python

import cherrypy
import src.queue as queue
from test.cptestcase import BaseCherryPyTestCase

def setUpModule():
    cherrypy.tree.mount(queue.QueueServer(), "/", queue.getConf())
    cherrypy.engine.start()
setup_module = setUpModule

def tearDownModule():
    cherrypy.engine.exit()
teardown_module = tearDownModule

class QueueServerSuite(BaseCherryPyTestCase):
    def test_sample(self):
        response = self.request("/")
        self.assertEqual(response.output_status, "200 OK")
        self.assertEqual(response.headers["Content-Type"], "text/html;charset=utf-8")
        self.assertTrue("<title>Home &middot; Queue</title>" in response.body[0])

# Load test suites
def suites():
    return [
        QueueServerSuite
    ]
