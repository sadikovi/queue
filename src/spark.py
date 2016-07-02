#!/usr/bin/env python

import json
import re
import types
import urllib2
import uuid
import src.const as const
import src.undersystem as undersystem
import src.util as util

class SparkSubmissionRequest(undersystem.SubmissionRequest):
    """
    Spark submission request is low-level representation of Spark job with some extra handling of
    file system, and pinging job to retrieve status.
    """
    def __init__(self, spark_code, working_directory, spark_options, main_class, jar, job_options):
        # interface code
        self.spark_code = spark_code
        # normalize path and check on existence, also check we have read-write access to the folder
        self.working_directory = util.readwriteDirectory(working_directory)
        # build command from options, currently just assign them
        self.spark_options = spark_options
        self.main_class = main_class
        self.jar = jar
        self.job_options = job_options

    def workingDirectory(self):
        """
        Get unique working directory for request.

        :return: working directory for request
        """
        return self.working_directory

    def interfaceCode(self):
        """
        Spark backend's unique identifier.

        :return: SparkBackend code
        """
        return self.spark_code

    def dispatch(self):
        raise NotImplementedError()

    def ping(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

class SparkBackend(undersystem.UnderSystemInterface):
    """
    Spark Backend as implementation of UnderSystemInterface. Provides access to check status and
    whether or not job can be submitted.
    """
    def __init__(self, master_url, rest_url, num_slots, working_directory):
        """
        Create instance of Spark backend.

        :param master_url: Spark Master URL, e.g. spark://sandbox:7077
        :param rest_url: Spark UI (REST) URL, normally it is http://localhost:8080
        :param num_slots: number of slots available for submission, i.e. number of concurrent jobs
        :param working_directory: working directory root, each job has a subdirectory under root
        """
        self.master_url = util.URI(master_url)
        if self.master_url.scheme != "spark":
            raise StandardError("Expected 'spark' scheme for url %s", self.master_url.url)
        self.rest_url = util.URI(rest_url, "Spark UI")
        self.num_slots = int(num_slots)
        self.working_directory = util.readwriteDirectory(working_directory)

    def applications(self, uri):
        """
        Fetch applications for Spark REST url. This method returns list of dict object, each of
        them contains "id", "name", and "completed" status of a job. If URL is invalid or Spark
        cluster cannot be reached, None is returned.

        :param uri: Spark REST url
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

    def name(self):
        """
        Name for Spark cluster.

        :return: Spark cluster alias
        """
        return "Spark cluster"

    def code(self):
        """
        Code identifier for Spark backend.

        :return: Spark unique identifier
        """
        return "SPARK"

    def link(self):
        """
        Return link for Spark UI.

        :return: uri for Spark UI
        """
        return self.rest_url

    def can_create_request(self):
        """
        Whether or not Spark can submit a job. This is valid operation, if status is either
        SYSTEM_AVAILABLE or SYSTEM_BUSY, and number of slots are greater than number of running
        applications.

        :return: True if Spark can submit job, False otherwise
        """
        apps = self.applications(self.rest_url)
        if apps is None:
            return False
        running = [x for x in apps if not x["completed"]]
        return len(running) < self.num_slots

    def status(self):
        """
        Status of Spark cluster, based on `applications()` method. If all applications are
        completed, we say that cluster is available, as nothing is running. If at least one
        application is running - cluster is busy, in any other cases - cluster is unavailable.

        :return: status as one of SYSTEM_AVAILABLE, SYSTEM_BUSY, SYSTEM_UNAVAILABLE
        """
        apps = self.applications(self.rest_url)
        if apps is None:
            return const.SYSTEM_UNAVAILABLE
        running = [x for x in apps if not x["completed"]]
        return const.SYSTEM_BUSY if running else const.SYSTEM_AVAILABLE

    def request(self, **kwargs):
        """
        Create new SparkSubmissionRequest based on options passed. This will create unique job
        directory for submission request, parse Spark and job specific options. Backend expects
        object like this as kwargs:
        ```
        {
            "spark_options": {
                "spark.sql.shuffle.partitions": "200",
                "spark.driver.memory": "10g",
                "spark.executor.memory": "10g"
                ...
            },
            "job_options": ["a", "b", "c", ...],
            "main_class": "com.github.sadikovi.Test",
            "jar": "/path/to/jar/file"
        }
        ```

        :param **kwargs: method attributes for extracting Spark options
        :return: Spark submission request
        """
        # Keys to search in kwargs:
        # - spark options in format {"spark.x.y": "z"}
        spark_opts = None
        # - job options as list of strings ["a", "b", "c"]
        job_opts = None
        # - fully qualified main class name including package
        main_class = None
        # - path to the jar file, must exist and have read access
        jar = None

        # Check that dictionary has expected options set
        if "spark_options" not in kwargs:
            raise KeyError("Spark options key are not specified")
        if "job_options" not in kwargs:
            raise KeyError("Job options key are not specified")
        if "main_class" not in kwargs:
            raise KeyError("Main class key is not specified")
        if "jar" not in kwargs:
            raise KeyError("Jar key is not specified")

        spark_opts = self.validateSparkOptions(kwargs["spark_options"])
        job_opts = self.validateJobOptions(kwargs["job_options"])
        main_class = self.validateMainClass(kwargs["main_class"])
        jar = util.readonlyFile(kwargs["jar"])

        # generate unique directory for submission request
        directory_suffix = uuid.uuid4().hex
        req_dir = util.concat(self.working_directory, directory_suffix)
        print req_dir
        # we assume that request directory does not exist, otherwise it will raise OSError
        util.mkdir(req_dir, 0774)
        return SparkSubmissionRequest(self.code(), req_dir, spark_opts, main_class, jar, job_opts)

    @staticmethod
    def validateSparkOptions(value):
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

    @staticmethod
    def validateJobOptions(value):
        """
        Validate job options, must be a list of string values, though non-string values are
        converted to strings, if type is invalid, raises error.

        :param value: raw value to process
        :return: validated job options as list of strings
        """
        if not isinstance(value, types.ListType):
            raise TypeError("Expected list of job options, got '%s'" % value)
        return [str(x) for x in value]

    @staticmethod
    def validateMainClass(value):
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
