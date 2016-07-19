#!/usr/bin/env python

import os
import cherrypy
from jinja2 import Environment, FileSystemLoader
from init import STATIC_PATH

# Loading jinja templates, we bind web directory to serve as collection of views
env = Environment(loader=FileSystemLoader(os.path.join(STATIC_PATH, "view")))
TEMPLATE_HOME = "home.html"

"""
Queue server main entry point. It defines all URLs that are used either for static content or
REST API. Note that entire configuration for server is set in here.
"""
class QueueController(object):
    def __init__(self, args, logger=None):
        """
        Create instance of application controller.

        :param args: raw arguments dictionary
        :param logger: logger function to use
        """
        import src.util as util
        self.name = "QUEUE"
        self.logger = logger(self.name) if logger else util.get_default_logger(self.name)
        # parse queue configuration based on additional arguments passed
        conf = util.QueueConf()
        conf.setAllConf(args)
        # log options processed
        all_options = ["  %s -> %s" % (key, value) for key, value in conf.copy().items()]
        self.logger.debug("Configuration:\n%s" % "\n".join(all_options))

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
        "/static": {
            "tools.staticdir.on": True,
            "tools.staticdir.dir": STATIC_PATH
        }
    }
    return conf

# Start server
def start(host="127.0.0.1", port=8080, args=None): # pragma: no cover
    """
    Start service.
    Additional arguments will be parsed in controller.

    :param host: host to bind
    :param port: port to bind
    :param args: additional arguments as dict
    """
    cherrypy.config.update({"server.socket_host": host})
    cherrypy.config.update({"server.socket_port": port})
    cherrypy.quickstart(QueueController(args), "/", getConf())
