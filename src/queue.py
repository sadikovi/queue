#!/usr/bin/env python

import inspect
import os
import types
import cherrypy
from jinja2 import Environment, FileSystemLoader
from init import STATIC_PATH
import src.const as const
import src.simple as simple
import src.spark as spark
import src.util as util

# Loading jinja templates, we bind web directory to serve as collection of views
env = Environment(loader=FileSystemLoader(os.path.join(STATIC_PATH, "view")))
TEMPLATE_HOME = "home.html"
TEMPLATE_STATUS = "status.html"

def rest_json_response(func):
    """
    Decorator to process function and return error as JSON payload.
    """
    # pylint: disable=W0703,broad-except
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except StandardError as std:
            cherrypy.response.status = 400
            return {"code": 400, "status": "ERROR", "msg": "%s" % std}
        except Exception as exc:
            cherrypy.response.status = 500
            return {"code": 500, "status": "ERROR", "msg": "%s" % exc}
    # pylint: enable=W0703,broad-except
    return wrapper

# pylint: disable=C0103,invalid-name
class SubmissionDispatcher(object):
    """
    Request dispatcher for submission, allows to store and retrieve submission by id.
    """
    exposed = True

    def __init__(self):
        self.id = 123

    @cherrypy.tools.json_out()
    @rest_json_response
    def GET(self):
        # raise ValueError("Invalid parameters")
        return {"method": cherrypy.request.method, "id": self.id}

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @rest_json_response
    def PUT(self):
        # Currently we expected these fields to be present in JSON with certain type
        # - name [string]
        # - code [string]
        # - priority [int]
        # - delay [int], default is 0
        # - payload [dict] default is {}
        data = cherrypy.request.json
        if not isinstance(data, types.DictType):
            msg = "Expected dictionary with structure: {'name': 'NAME', 'code': 'CODE', " + \
                "[optional]'priority': '2', [optional]'delay': '0', [optional]'payload': {...}}"
            raise TypeError("%s, got %s" % (msg, data))
        if "name" not in data:
            raise KeyError("Expected required field 'name' from %s" % data)
        name = data["name"]
        if "code" not in data:
            raise KeyError("Expected required field 'code' from %s" % data)
        system_code = data["code"]
        priority = int(data["priority"]) if "priority" in data else const.PRIORITY_2
        delay = int(data["delay"]) if "delay" in data else 0
        payload = dict(data["payload"]) if "payload" in data else {}
        username = None
        return {"name": name, "code": system_code, "priority": priority, "delay": delay,
                "payload": payload, "username": username}
# pylint: enable=C0103,invalid-name

class RestApiDispatcher(object):
    def __init__(self):
        self.submission = SubmissionDispatcher()

class QueueController(object):
    """
    Queue server main entry point. It defines all URLs that are used either for static content or
    REST API. Note that entire configuration for server is set in here.
    """
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
        self.api = RestApiDispatcher()

    def _create_session(self, conf):
        """
        Create new session based on QueueConf.

        :param conf: QueueConf instance
        :return: Session instance
        """
        if not isinstance(conf, util.QueueConf):
            raise AttributeError("Invalid configuration, got %s" % conf)
        system_code = conf.getConf(const.OPT_SYSTEM_CODE)
        session_class = None
        if system_code == spark.SPARK_SYSTEM_CODE:
            session_class = spark.SparkSession
        elif system_code == simple.SIMPLE_SYSTEM_CODE:
            session_class = simple.SimpleSession
        else:
            raise StandardError("System code %s is unrecognized" % system_code)
        return session_class.create(conf, self.working_dir, None)

    def _pretty_name(self, obj):
        """
        Pretty name for object that can be either class or instance of a class.

        :param obj: class or instance of class
        :return: string representation of obj
        """
        return obj.__name__ if inspect.isclass(obj) else type(obj).__name__

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
            "executor_class": self._pretty_name(self.session.scheduler.executor_class()),
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
        },
        "/api/submission": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher(),
            "tools.response_headers.on": True,
            "tools.response_headers.headers": [("Content-Type", "application/json")]
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
    # cherrypy.engine.subscribe("start", controller.start)
    # cherrypy.engine.subscribe("stop", controller.stop)
    cherrypy.quickstart(controller, "/", getConf())
