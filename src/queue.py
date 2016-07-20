#!/usr/bin/env python

import os
import cherrypy
from jinja2 import Environment, FileSystemLoader
from init import STATIC_PATH
import src.const as const
import src.spark as spark
import src.util as util

# Loading jinja templates, we bind web directory to serve as collection of views
env = Environment(loader=FileSystemLoader(os.path.join(STATIC_PATH, "view")))
TEMPLATE_HOME = "home.html"
TEMPLATE_STATUS = "status.html"

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
        self.name = "QUEUE"
        self.logger = logger(self.name) if logger else util.get_default_logger(self.name)
        # parse queue configuration based on additional arguments passed
        conf = util.QueueConf()
        conf.setAllConf(args)
        # log options processed
        all_options = ["  %s -> %s" % (key, value) for key, value in conf.copy().items()]
        self.logger.debug("Configuration:\n%s" % "\n".join(all_options))
        # resolve options either directly or through session and other services
        self.working_dir = util.readwriteDirectory(conf.getConfString(const.OPT_WORKING_DIR))
        self.service_dir = util.readonlyDirectory(conf.getConfString(const.OPT_SERVICE_DIR))
        self.session = self._create_session(conf)

    def _create_session(self, conf):
        """
        Create new session based on QueueConf.

        :param conf: QueueConf instance
        :return: Session instance
        """
        if not isinstance(conf, util.QueueConf):
            raise AttributeError("Invalid configuration, got %s" % conf)
        # currently we only have one session for Spark, default logger is used for now
        return spark.SparkSession.create(conf, None)

    def get_status_dict(self):
        """
        Get dictionary of all statuses, metrics from session and scheduler.

        :return: dict of statuses and metrics
        """
        session_status = {}
        # generic directories (displayed as part of the session)
        session_status["working_dir"] = self.working_dir
        session_status["service_dir"] = self.service_dir
        # session metrics
        session_status["code"] = self.session.system_code()
        # system URI can be None
        sys_uri = self.session.system_uri()
        session_status["url"] = {"href": sys_uri.url, "alias": sys_uri.alias} if sys_uri else None
        session_status["status"] = self.session.status()
        # scheduler metrics
        session_status["scheduler"] = {
            "name": type(self.session.scheduler).__name__,
            "num_executors": self.session.scheduler.get_num_executors(),
            "executor_class": self.session.scheduler.executor_class().__name__,
            "metrics": self.session.scheduler.get_metrics(),
            "is_alive_statuses": self.session.scheduler.get_is_alive_statuses()
        }
        return session_status

    @cherrypy.expose
    def index(self):
        template = env.get_template(TEMPLATE_HOME)
        return template.render()

    @cherrypy.expose
    def status(self):
        import datetime
        template = env.get_template(TEMPLATE_STATUS)
        return template.render(self.get_status_dict())

    def start(self):
        """
        Start all session services.
        """
        self.session.scheduler.start_maintenance()
        self.session.scheduler.start()

    def stop(self):
        """
        Stop all session services.
        """
        self.session.scheduler.stop()

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
    controller = QueueController(args)
    cherrypy.engine.subscribe("start", controller.start)
    cherrypy.engine.subscribe("stop", controller.stop)
    cherrypy.quickstart(controller, "/", getConf())
