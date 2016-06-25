#!/usr/bin/env python

import os
import cherrypy
from jinja2 import Environment, FileSystemLoader
from init import STATIC_PATH, WEB_PATH

# Loading jinja templates
env = Environment(loader=FileSystemLoader(os.path.join(WEB_PATH, "view")))
TEMPLATE_HOME = "home.html"

"""
Queue server main entry point. It defines all URLs that are used either for static content or
REST API. Note that entire configuration for server is set in here.
"""
class QueueController(object):
    @cherrypy.expose
    def index(self):
        template = env.get_template(TEMPLATE_HOME)
        return template.render()

# Configuration setup for application except host and port
def getConf(): # pragma: no cover
    conf = {
        "/": {
            "tools.gzip.on": True
        },
        "/favicon.ico": {
            "tools.staticfile.on": True,
            "tools.staticfile.filename": os.path.join(WEB_PATH, "favicon.ico")
        },
        "/static": {
            "tools.staticdir.on": True,
            "tools.staticdir.dir": STATIC_PATH
        }
    }
    return conf

# Start server
def start(host="127.0.0.1", port=8080): # pragma: no cover
    cherrypy.config.update({"server.socket_host": host})
    cherrypy.config.update({"server.socket_port": port})
    cherrypy.quickstart(QueueController(), "/", getConf())
