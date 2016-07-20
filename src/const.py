#!/usr/bin/env python

# Module contains all constants defined in application
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
        self.pretty_name = "%s" % self.name

    # pylint: disable=R0801,duplicate-code
    def __str__(self):
        return self.pretty_name

    def __unicode__(self):
        return self.pretty_name

    def __repr__(self):
        return self.pretty_name
    # pylint: enable=R0801,duplicate-code

# == System availability ==
# Available status, e.g. when system is operational or idle
SYSTEM_AVAILABLE = Status("AVAILABLE", "Available")
# Busy status when system is under load, but still reachable
SYSTEM_BUSY = Status("BUSY", "Busy")
# Unavailable status when system is unreachable because of timeout or is down
SYSTEM_UNAVAILABLE = Status("UNAVAILABLE", "Unavailable")

# == Submission status ==
# Submission is pending, this is the initial status
SUBMISSION_PENDING = Status("PENDING", "Pending")
# Submission is waiting because of scheduled delay
SUBMISSION_WAITING = Status("WAITING", "Waiting")
# Submission is running on system backend
SUBMISSION_RUNNING = Status("RUNNING", "Running")
# Submission is finished successfully
SUBMISSION_SUCCESS = Status("SUCCESS", "Success")
# Submission is finished with error
SUBMISSION_FAILURE = Status("FAILURE", "Failure")
# Submission is discarded, as it is no longer needed
SUBMISSION_DISCARD = Status("DISCARD", "Discard")
# Submission status is unknown, most likely failed,
# reason is usually unsuccessfull shut down
SUBMISSION_UNKNOWN = Status("UNKNOWN", "Unknown")

# == Priority ==
# Different priorities for submission, smaller number indicates higher priority, when there are
# different levels of priority, highest should be selected.
PRIORITY_0 = 0
PRIORITY_1 = 1
PRIORITY_2 = 2
# Priority order
PRIORITIES = [PRIORITY_0, PRIORITY_1, PRIORITY_2]

# == Configuration options for session ==
OPT_NUM_PARALLEL_TASKS = "queue.num.parallel.tasks"
OPT_SCHEDULER_TIMEOUT = "queue.scheduler.timeout"
OPT_WORKING_DIR = "queue.working.directory"
OPT_SERVICE_DIR = "queue.service.directory"
OPT_SPARK_MASTER = "spark.master"
OPT_SPARK_WEB = "spark.web"
