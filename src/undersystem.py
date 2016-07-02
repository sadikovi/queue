#!/usr/bin/env python

import src.const as const

# Module provides means to create interface for under-system, e.g. Spark to report status,
# availability, and submission procedure. Every backend that implements UnderSystemInterface should
# also overwrite SubmissionRequest.

class SubmissionRequest(object):
    """
    Abstract class for job submission, logic is defined in subclasses, e.g. building shell command
    from options provided and submitting through Java API. Instance of this class has all
    information to launch process or retrieve status, or close and remove threads once finished.
    """
    def __init__(self):
        raise StandardError("Cannot be instantiated")

    def interfaceCode(self):
        """
        Unique identifier of the interface that was used to create submission request, is assigned
        value of `UnderSystemInterface.code()`.

        :return: unique system identifier (code)
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
        refresh status of request. Should return one of the statuses listed in 'const' module

        :return: constant instance of const.Status, which is listed in module 'const'
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

    def code(self):
        """
        Return code for the service, e.g. SPARK, that is unique to a service. Must be less than 10
        characters.

        :return: code name/identifier for the service
        """
        raise NotImplementedError()

    def status(self):
        """
        Return current status of the system as one of predefined statuses SYSTEM_AVAILABLE,
        SYSTEM_BUSY, SYSTEM_UNAVAILABLE defined in 'const' module.

        :return: system status from 'const' module
        """
        raise NotImplementedError()

    def link(self):
        """
        Link to the system UI, if available, otherwise should return None.

        :return: src.util.URI instance pointing to the system UI
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
        Optional method to provide custom message for `available` status. By default, returns status
        description.

        :return: custom message for availability of the system
        """
        return const.SYSTEM_AVAILABLE.desc

    @staticmethod
    def busy():
        """
        Optional method to provide custom message for `busy` status. By default, returns status
        description.

        :return: custom message, when system is busy or processing a submission request
        """
        return const.SYSTEM_BUSY.desc

    @staticmethod
    def unavailable():
        """
        Optional method to provide custom message for `unavailable` status. By default, returns
        status description.

        :return: custom message, when system is unreachable
        """
        return const.SYSTEM_UNAVAILABLE.desc
