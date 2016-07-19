#!/usr/bin/env python

class Session(object):
    """
    Generic interface to give access to functionality of scheduler and under-system.
    """
    def system_code(self):
        """
        Return special system code (short) to identify under-system.

        :return: short string code of the under-system
        """
        raise NotImplementedError("Not implemented")

    def system_uri(self):
        """
        Return under-system link as util.URI to provide access to external system UI. Can be None.

        :return: URI object with a link or None
        """
        raise NotImplementedError("Not implemented")

    def status(self):
        """
        Return current status of the system availability, this should correlate with ability of
        scheduler to launch task, but it is not required.

        :return: availability status: SYSTEM_BUSY, SYSTEM_AVAILABLE, SYSTEM_UNAVAILABLE
        """
        raise NotImplementedError("Not implemented")

    @property
    def scheduler(self):
        """
        Get scheduler for the context.

        :return: Scheduler implementation for the session
        """
        raise NotImplementedError("Not implemented")

    @classmethod
    def create(cls, conf, logger):
        """
        Create instance of session using dictionary of configuration options. All options are in
        format "group.name", where "group" is option group, e.g. spark, and "name" is a name of
        the option, e.g. spark.master.

        :param conf: QueueConf instance
        :param logger: logger function
        :return: instance of session
        """
        raise NotImplementedError("Not implemented")
