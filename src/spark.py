#!/usr/bin/env python

import json
import urllib2
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
        object like this:
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

        # Unique job directory as subdirectory of SparkBackend root
        # if not isinstance(uid, types.StringType):
        #     raise StandardError("UID is not of String type")
        # if not uid.isalnum():
        #     raise StandardError("UID is not alphanumeric")
        # job_dir = os.path.join(self.working_directory, uid)
        # Attempt to create directory on file system, this will fail if directory already exists
        # os.mkdir(job_dir)
        # Parse system and job options
        # spark_options = raw_system_options
        # job_options = raw_options
        # Parse files to extract jar
        # jar = files
        # return SparkSubmissionRequest(uid, name, self, job_dir, spark_options, job_options, jar)
        raise NotImplementedError()
