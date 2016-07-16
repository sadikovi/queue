#!/usr/bin/env python

import json
import re
import subprocess
import time
import types
import urllib2
import src.scheduler as scheduler
import src.util as util

# Class to hold all Spark standalone cluster related functionality, including methods to validate
# different aspects of submission, launching process and system status.

# == General functionality ==
# Undersystem code for Spark
SPARK_SYSTEM_CODE = "SPARK"
# Default spark-submit
SPARK_SUBMIT = "spark-submit"
# Default Spark master url
SPARK_MASTER_URL = "spark://master:7077"
# Default Spark web (REST) url
SPARK_WEB_URL = "http://localhost:8080"
# Default application name
SPARK_APP_NAME = "SPARK QUEUE APP"

def applications(uri):
    """
    Fetch applications for Spark REST url. This method returns list of dict object, each of
    them contains "id", "name", and "completed" status of a job. If URL is invalid or Spark
    cluster cannot be reached, None is returned.

    :param uri: Spark REST url, e.g. http://localhost:8080 (all), or http://localhost:4040 (active)
    :return: list of Spark applications as dict objects
    """
    try:
        # perform request with timeout of 30 seconds
        fileobject = urllib2.urlopen("%s/api/v1/applications" % uri, timeout=30)
    except StandardError:
        return None
    else:
        # when requested, Spark returns list of jobs
        apps = json.loads(fileobject.read())
        parsed = []
        for app in apps:
            if "id" in app and "name" in app and "attempts" in app:
                completed = True
                for attempt in app["attempts"]:
                    completed = completed and attempt["completed"]
                parsed.append({"id": app["id"], "name": app["name"], "completed": completed})
        return parsed

def can_submit_task(uri, max_available_slots=1):
    """
    Return True, if task can be submitted in Spark cluster. If there are no applications, or
    number or running applications is less than maximum available slots, then it is considered okay
    to launch task (True), otherwise return False.

    :param uri: Spark REST url, e.g. http://localhost:8080
    :param max_available_slots: maximum available slots, default is 1
    :return: True if can submit task, False otherwise
    """
    apps = applications(uri)
    if apps is None:
        return False
    running = [app for app in apps if not app["completed"]]
    return len(running) < max_available_slots

# == Validation ==
def validate_spark_options(value):
    """
    Validate Spark options, must be a dictionary where key as a name of option, and value is
    option's value as string. If option name does not start with "spark.", it is ignored.
    Raises error, if value is not a dictionary; non-string value is converted into string.

    :param value: raw value to process
    :return: validated Spark options as dictionary
    """
    if not isinstance(value, types.DictType):
        raise TypeError("Expected dictionary of Spark options, got '%s'" % value)
    temp_buffer = {}
    for maybe_key, maybe_value in value.items():
        tkey = str(maybe_key).strip()
        tvalue = str(maybe_value).strip()
        if tkey.startswith("spark."):
            temp_buffer[tkey] = tvalue
    return temp_buffer

def validate_job_options(value):
    """
    Validate job options, must be a list of string values, though non-string values are
    converted to strings, if type is invalid, raises error.

    :param value: raw value to process
    :return: validated job options as list of strings
    """
    if not isinstance(value, types.ListType):
        raise TypeError("Expected list of job options, got '%s'" % value)
    return [str(x) for x in value]

def validate_main_class(value):
    """
    Validate main class syntax, must have "." separated word boundaries. Raises error if name
    does not confirm to pattern or is None.

    :param value: raw value to process
    :return: validated main class name
    """
    if not value:
        raise ValueError("Invalid main class as None, expected package?.Class")
    groups = re.match(r"^(\w+)(\.\w+)*$", value.strip())
    if not groups:
        raise ValueError("Invalid main class syntax '%s', expected package?.Class" % value)
    return groups.group(0)

class SparkStandaloneTask(scheduler.Task):
    """
    Special task to process Spark submission for standalone cluster manager. Essentially creates
    spark-submit command and executes it.
    """
    def __init__(self, uid, priority, logger=None):
        """
        Create new instance of Spark standalone task. Most of the options are set to default
        values. Use different setters to adjust parameters.

        :param uid: unique identifier for a task
        :param priority: task priority, should be one of PRIORITY_0, PRIORITY_1, PRIORITY_2
        :param logger: logger function
        """
        # == Task options ==
        # Unique task identifier
        self.__uid = uid
        # Task priority
        self.__priority = priority
        # Task refresh timeout
        self.timeout = 1.0
        # Task logger
        logger_name = "%s[%s]" % (type(self).__name__, self.__uid)
        self.logger = logger(logger_name) if logger else util.get_default_logger(logger_name)
        # == Spark cluster related options ==
        # Define how to connect and retrieve cluster information
        self._spark_submit = SPARK_SUBMIT
        self._master_url = SPARK_MASTER_URL
        self._web_url = SPARK_WEB_URL
        # Shell process options (process, exit code, working directory for output streams)
        self.__ps = None
        self.__working_directory = None
        # == Spark application options ==
        self.name = SPARK_APP_NAME
        self.spark_options = {}
        self.main_class = None
        self.jar = None
        self.job_options = []

    @property
    def uid(self):
        return self.__uid

    @property
    def priority(self):
        return self.__priority

    # == Get and set for Spark cluster related options ==
    @property
    def spark_submit(self):
        """
        Get current path to spark-submit or default global value.

        :return: spark-submit executable
        """
        return self._spark_submit

    @spark_submit.setter
    def spark_submit(self, value):
        """
        Set spark-submit, must be path to the executable, or default value.

        :param value: new spark-submit path or default value
        """
        if value == SPARK_SUBMIT:
            self._spark_submit = value
        else:
            self._spark_submit = util.readonlyFile(value)

    @property
    def master_url(self):
        """
        Return Spark master url.

        :return: current master url
        """
        return self._master_url

    @master_url.setter
    def master_url(self, value):
        """
        Set Spark master url, must have format spark://host:port.

        :param value: new master url
        """
        uri = util.URI(value)
        if uri.scheme != "spark":
            raise ValueError("Expected scheme to be 'spark' for master url: %s" % value)
        self._master_url = uri.url

    @property
    def web_url(self):
        """
        Return current Spark web url.

        :return: Spark web (REST) url
        """
        return self._web_url

    @web_url.setter
    def web_url(self, value):
        """
        Set Spark web url, must have format http://host:port.

        :param value: new web url
        """
        self._web_url = util.URI(value).url

    # == Shell process methods
    @property
    def working_directory(self):
        return self.__working_directory

    @working_directory.setter
    def working_directory(self, value):
        # working directory can either be None (no stdout) or valid directory
        self.__working_directory = value if value is None else util.readwriteDirectory(value)

    def cmd(self):
        """
        Construct shell command to execute Spark submit, must preserve certain order of components:
        'path/to/spark-submit', 'name', 'master-url', 'spark-options', 'main-class', 'path/to/jar',
        'job-options', e.g. spark-submit --master spark://sandbox:7077
        --conf "spark.driver.memory=2g" --conf "spark.executor.memory=2g" --class entrypoint jar

        :return: shell command to execute as a list of arguments
        """
        # each Spark option starts with "--conf"
        pairs = [["--conf", "%s=%s" % (key, value)] for key, value in self.spark_options.items()]
        command = \
            [str(self._spark_submit)] + \
            ["--master", str(self._master_url)] + \
            ["--name", str(self.name)] + \
            [conf for pair in pairs for conf in pair] + \
            ["--class", str(self.main_class)] + \
            [str(self.jar)] + \
            self.job_options
        return command

    # == Set application options ==
    def set_application(self, **kwargs):
        """
        Set application parameters: name, main class, options, etc. Currently supported keys:
        - 'name' name of Spark application
        - 'main_class' main entrypoint (fully qualified class name) to run
        - 'jar' fully resolved path to a jar file
        - 'spark_options' dictionary of Spark options, e.g. spark.executor.memory
        - 'job_options' specific application (job) options

        :param kwargs: application options as dictionary
        """
        for key, value in kwargs.items():
            if key == "name":
                self.name = str(value)
            if key == "main_class":
                self.main_class = validate_main_class(value)
            if key == "jar":
                self.jar = util.readonlyFile(value)
            if key == "spark_options":
                self.spark_options = validate_spark_options(value)
            if key == "job_options":
                self.job_options = validate_job_options(value)

    def launch_process(self):
        """
        Launch process for spark-submit.
        """
        # spark-submit command to launch
        command = self.cmd()
        self.logger.info("Launch command %s", command)
        # only create stdout and stderr when working directory is provided
        if self.__working_directory:
            stdout_path = util.concat(self.__working_directory, "stdout")
            stderr_path = util.concat(self.__working_directory, "stderr")
            stdout = util.open(stdout_path, "wb")
            stderr = util.open(stderr_path, "wb")
            self.__ps = subprocess.Popen(command, bufsize=4096, stdout=stdout, stderr=stderr,
                                         close_fds=True)
        else:
            self.__ps = subprocess.Popen(command, bufsize=4096, stdout=None, stderr=None,
                                         close_fds=True)
        self.logger.info("Process pid=%s", self.__ps.pid)

    def run(self):
        """
        Launch process and refresh status with fixed interval.
        """
        self.launch_process()
        returncode = None
        while returncode is None:
            time.sleep(self.timeout)
            returncode = self.__ps.poll()
        # once process has finished, check return code, If it does not equal 0, raise exception
        if returncode != 0:
            raise IOError("Process finished with non-zero exit code %s" % returncode)

    def cancel(self):
        """
        Cancel running task. Process is sent SIGTERM first, and if it is not terminated within 10
        seconds, SIGKILL is sent permanently and blocked until exit code is returned. Note that
        status is also assigned as TASK_FINISHED.

        :return: exit code (for testing purposes)
        """
        return_code = None
        if self.__ps:
            self.__ps.terminate()
            attempts = 5 # attempts (seconds) to wait before killing process
            while attempts > 0 and return_code is None:
                time.sleep(1)
                return_code = self.__ps.poll()
                attempts -= 1
            if return_code is None:
                self.__ps.kill()
                return_code = self.__ps.wait()
        return return_code

    def _current_ps(self):
        """
        Get current process, for testing purposes only.

        :return: current process for the task
        """
        return self.__ps
