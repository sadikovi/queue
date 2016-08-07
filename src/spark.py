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
import re
import subprocess
import time
import types
import urllib2
import src.const as const
import src.context as context
from src.log import logger
import src.scheduler as scheduler
import src.submission as submission
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

def validate_master_url(value):
    """
    Validate Spark master address, which has a format: spark://host:port.

    :param value: unresolved master address value
    :return: valid Spark master address as string
    """
    try:
        uri = util.URI(value)
    except StandardError as err:
        raise StandardError("Failed to assign Spark master url for '%s', reason: %s" % (value, err))
    else:
        if uri.scheme != "spark":
            raise ValueError("Expected scheme to be 'spark' for master url '%s'" % value)
        return uri.url

def validate_web_url(value):
    """
    Validate web url for Spark (Spark Web UI), this is also used to access Spark REST API.
    Has format: http(s)://host:port.

    :param value: unresolved web url value
    :return: valid Spark web url as string
    """
    try:
        return util.URI(value).url
    except StandardError as err:
        raise StandardError("Failed to assign Spark web url for '%s', reason: %s" % (value, err))

class SparkStandaloneTask(scheduler.Task):
    """
    Special task to process Spark submission for standalone cluster manager. Essentially creates
    spark-submit command and executes it.
    """
    def __init__(self, uid, priority):
        """
        Create new instance of Spark standalone task. Most of the options are set to default
        values. Use different setters to adjust parameters.

        :param uid: unique identifier for a task
        :param priority: task priority, should be one of PRIORITY_0, PRIORITY_1, PRIORITY_2
        """
        # == Task options ==
        # Unique task identifier
        self.__uid = uid
        # Task name
        self.name = "Task[%s]" % self.__uid
        # Task priority
        self.__priority = priority
        # Task refresh timeout
        self.timeout = 1.0
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
        self._master_url = validate_master_url(value)

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
        self._web_url = validate_web_url(value)

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
        logger.info("%s - Launch command %s", self.name, command)
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
        logger.info("%s - Process pid=%s", self.name, self.__ps.pid)

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

class SparkStandaloneExecutor(scheduler.Executor):
    """
    Spark scheduler executor for standalone cluster manager. Provides method to detect Spark
    cluster availability.
    """
    def __init__(self, name, conn, task_queue_map, timeout=1.0):
        super(SparkStandaloneExecutor, self).__init__(name, conn, task_queue_map, timeout)
        # Spark master url, e.g. spark://master:7077, we do not validate value
        self.master_url = SPARK_MASTER_URL
        # Spark web url, e.g. http://localhost:8080, we do not validate value
        self.web_url = SPARK_WEB_URL
        # Maximum number of running jobs after which stop submitting tasks
        self.max_available_slots = 1

    def external_system_available(self):
        """
        Check whether or not Spark can accept another task to run.

        :return: True, if cluster can process task, False otherwise
        """
        return can_submit_task(self.web_url, self.max_available_slots)

class SparkStandaloneScheduler(scheduler.Scheduler):
    """
    Spark standalone scheduler for Queue. Launches executors and processes task to run on Spark
    cluster.
    """
    def __init__(self, master_url, web_url, num_executors, timeout=1.0):
        super(SparkStandaloneScheduler, self).__init__(num_executors, timeout)
        self.master_url = master_url
        self.web_url = web_url
        # maximum available slots is number of executors to run parallel tasks
        self.max_available_slots = num_executors

    def executor_class(self):
        """
        Return executor for Spark standalone Queue scheduler.

        :return: Executor class for Spark standalone
        """
        return SparkStandaloneExecutor

    def update_executor(self, executor):
        """
        Update executor with Spark options: master url and web url, and max available slots.

        :param executor: executor to update
        """
        executor.master_url = self.master_url
        executor.web_url = self.web_url
        executor.max_available_slots = self.max_available_slots

class SparkSession(context.Session):
    """
    Session context to manage launching tasks in Spark, and providing statistics.
    """
    def __init__(self, master_url, web_url, working_dir, num_executors=1, timeout=1.0):
        self.master_url = validate_master_url(master_url)
        self.web_url = validate_web_url(web_url)
        # working directory is not validated
        self.working_dir = working_dir
        self.num_executors = scheduler.validate_num_executors(num_executors)
        self.timeout = scheduler.validate_timeout(timeout)
        self._scheduler = SparkStandaloneScheduler(self.master_url, self.web_url,
                                                   self.num_executors, self.timeout)

    def system_code(self):
        return SPARK_SYSTEM_CODE

    def system_uri(self):
        return util.URI(self.web_url, "Spark Web UI")

    def status(self):
        apps = applications(self.web_url)
        if apps is None:
            return const.SYSTEM_UNAVAILABLE
        running = [app for app in apps if not app["completed"]]
        return const.SYSTEM_BUSY if running else const.SYSTEM_AVAILABLE

    def create_task(self, sub):
        if not isinstance(sub, submission.Submission):
            raise AttributeError("Expected Submission instance, got %s" % sub)
        if sub.is_template:
            raise ValueError("Cannot create task from template %s" % sub.dumps())
        if sub.is_deleted:
            raise ValueError("Cannot create task from deleted submission %s" % sub.dumps())
        if sub.system_code != SPARK_SYSTEM_CODE:
            raise ValueError("Incompatible code, %s != %s" % (sub.system_code, SPARK_SYSTEM_CODE))
        # After this point submission can be converted into task
        task = SparkStandaloneTask(sub.uid, sub.priority)
        task.master_url = self.master_url
        task.web_url = self.web_url
        # create working directory for the task
        task_working_dir = util.concat(self.working_dir, task.uid)
        util.mkdir(task_working_dir, 0775)
        task.working_directory = task_working_dir
        # update payload to include name
        task.set_application(name=sub.name, **sub.payload)
        return task

    @property
    def scheduler(self):
        return self._scheduler

    @classmethod
    def create(cls, conf, working_dir):
        timeout = conf.getConfFloat(const.OPT_SCHEDULER_TIMEOUT)
        num_parallel_tasks = conf.getConfInt(const.OPT_NUM_PARALLEL_TASKS)
        spark_master = conf.getConfString(const.OPT_SPARK_MASTER)
        spark_web = conf.getConfString(const.OPT_SPARK_WEB)
        return cls(spark_master, spark_web, working_dir, num_parallel_tasks, timeout)
