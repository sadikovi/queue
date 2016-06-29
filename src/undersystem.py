#!/usr/bin/env python

import urlparse

# Module provides means to create interface for under-system, e.g. Spark to report status,
# availability, and submission procedure. Every backend that implements UnderSystemInterface should
# also overwrite SubmissionRequest.

class Status(object):
    """
    Generic status object for the system.
    """
    def __init__(self, name, desc):
        """
        Create status instance.

        :param name: name of the status
        :param desc: status description
        """
        self.name = name
        self.desc = desc

# == System availability ==
# Available status, e.g. when system is operational or idle
AVAILABLE = Status("available", "Available")
# Busy status when system is under load, but still reachable
BUSY = Status("busy", "Busy")
# Unavailable status when system is unreachable because of timeout or is down
UNAVAILABLE = Status("unavailable", "Unavailable")

# == Submission status ==
# Submission is pending, this is the initial status
PENDING = Status("pending", "Pending")
# Submission is finished successfully
SUCCESS = Status("success", "Success")
# Submission is finished with error
FAILURE = Status("failure", "Failure")

class URI(object):
    """
    URI class to keep information about URL and components, parses and validates components
    """
    def __init__(self, rawURI, alias=None):
        uri = urlparse.urlsplit(rawURI)
        # we expect scheme, hostname and port to be set, otherwise uri is considered invalid
        if not uri.port or not uri.hostname or not uri.scheme:
            raise StandardError("Invalid URI - expected host, port and scheme from %s" % rawURI)
        self._host = uri.hostname
        self._port = int(uri.port)
        self._scheme = uri.scheme
        self._netloc = uri.netloc
        self._fragment = uri.fragment
        self._url = uri.geturl()
        # Alias for URL if it is too long, if None provided url is used
        self._alias = alias

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def scheme(self):
        return self._scheme

    @property
    def netloc(self):
        return self._netloc

    @property
    def fragment(self):
        return self._fragment

    @property
    def url(self):
        return self._url

    @property
    def alias(self):
        return self._alias if self._alias else self._url

class SubmissionRequest(object):
    """
    Abstract class for job submission, logic is defined in subclasses, e.g. building shell command
    from options provided and submitting through Java API. Instance of this class has all
    information to launch process or retrieve status, or close and remove threads once finished.
    """
    def __init__(self):
        raise StandardError("Cannot be instantiated")

    def interface(self):
        """
        Return pointer to interface that was used to create instance. Note that interface is always
        created once per application.

        :return: instance of UnderSystemInterface
        """
        raise NotImplementedError()

    def workingDirectory(self):
        """
        Return resolved absolute directory path as a string in which all data for the submission is
        stored including logs stdout/stderror, job details, metadata, etc. Note that working
        directory is optional, and may not exist depending on implementation. Structure of working
        directory can vary, so it might not have logs, or metadata.

        :return: resolved absolute filepath or None, if not applicable
        """
        raise NotImplementedError()

    def dispatch(self, **kwargs):
        """
        Submit request to the underlying system. For example, for Spark it is invoking process to
        launch Spark job using `spark-submit`. Note that this method should be asynchronous.

        :param **kwargs: different parameters to submit request
        """
        raise NotImplementedError()

    def ping(self):
        """
        Return status of the submission request, note that this method is called periodically to
        refresh status of request. Can return PENDING when submission is still running, SUCCESS when
        submission is finished successfully, and FAILURE when submission is finished with error.

        :return: PENDING if request is being processed, SUCCESS/FAILURE when finished
        """
        raise NotImplementedError()

    def close(self):
        """
        Close all resources associated with submission request, including closing file stream,
        stopping processes or threads, removing temporary files, updating metadata, etc.
        """
        raise NotImplementedError()

class UnderSystemInterface(object):
    """
    Abstract class for under-system, subclasses should overwrite mentioned below methods. Instance
    is created once per application, globally.
    """
    def __init__(self):
        raise StandardError("Cannot be instantiated")

    def name(self):
        """
        Return custom name of the service, should be fairly short.

        :return: system name as string
        """
        raise NotImplementedError()

    def status(self):
        """
        Return current status of the system as one of predefined statuses AVAILABLE, BUSY,
        UNAVAILABLE defined above.

        :return: system status as Status class
        """
        raise NotImplementedError()

    def link(self):
        """
        Link to the system UI, if available, otherwise should return None.

        :return: URI instance pointing to the system UI
        """
        raise NotImplementedError()

    def request(self, **kwargs):
        """
        Create new submission request for the system. It should raise an error, if request cannot
        be created, or return a valid instance of SubmissionRequest.

        :param **kwargs: different options to construct submission request
        :return: instance of SubmissionRequest
        """
        raise NotImplementedError()

    def can_create_request(self):
        """
        Whether or not interface can create new request. This should depend on system settings, not
        on actual request being created.

        :return: True if request can be created, False otherwise
        """
        raise NotImplementedError()

    @staticmethod
    def available():
        """
        Optional method to provide custom message for `available` status. By default returns status
        description.

        :return: custom message for availability of the system
        """
        return AVAILABLE.desc

    @staticmethod
    def busy():
        """
        Optional method to provide custom message for `busy` status. By default returns status
        description.

        :return: custom message, when system is busy or processing a submission request
        """
        return BUSY.desc

    @staticmethod
    def unavailable():
        """
        Optional method to provide custom message for `unuavailable` status. By default return
        status description.

        :return: custom message, when system is unreachable
        """
        return UNAVAILABLE.desc
