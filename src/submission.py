#!/usr/bin/env python

import types
import uuid
import src.const as const
import src.util as util

def validate_delay(value):
    """
    Validate delay in seconds, should be between min and max delays.

    :param value: raw value to validate
    :return: correct delay in seconds
    """
    delay = int(value)
    if delay < const.SUBMISSION_DELAY_MIN or delay > const.SUBMISSION_DELAY_MAX:
        raise ValueError("Delay %s must be between %s and %s" % (
            delay, const.SUBMISSION_DELAY_MIN, const.SUBMISSION_DELAY_MAX))
    return delay

def validate_name(value):
    """
    Validate name and set it to default if conversion fails.

    :param value: raw name value
    :return: correct name
    """
    if not isinstance(value, types.StringType) and not isinstance(value, types.UnicodeType):
        raise TypeError("Expected value %s to be string" % value)
    name = str(value).strip()
    if not name:
        raise ValueError("Name is not specified or contains spaces only")
    return name

def validate_priority(value):
    """
    Validate priority value by comparing to list of standard priorities.

    :param value: raw priority value
    :return: correct priority
    """
    if value not in const.PRIORITIES:
        raise ValueError("Invalid priority %s, must be one of %s" % (value, const.PRIORITIES))
    return value

class Submission(object):
    """
    Submission interface for queue, external wrapper for Task interface. Note that it contains only
    generic job information, and specific to the under-system data is stored in 'payload'.
    Submission status is also more generic that task status, though there is direct one-to-one
    mapping with some additional values for submission. Do not use this interface to create
    templates, use Template class instead.

    Submission validates generic part of the request to create an application, when Task validates
    payload part that is specific to under-system.
    """
    DEFAULT_PAYLOAD = {}
    DEFAULT_USERNAME = "default"

    def __init__(self, name, system, priority=const.PRIORITY_2, delay=0, payload=None,
                 username=None):
        """
        Create Submission instance. Note that unique identifier should be set afterwards, if
        submission is valid.

        :param name: submission name
        :param system: under-system code
        :param priority: submission priority
        :param delay: delay in seconds, will be converted into submission datetime
        :param payload: payload for specific task, default is None
        :param username: user created submission, default is None
        """
        self.uid = uuid.uuid4().hex
        self.name = validate_name(name)
        # code for under-system
        self.system_code = system
        # submission priority
        self.priority = validate_priority(priority)
        # whether or not submission is deleted
        self.is_deleted = False
        # template settings
        self.is_template = False
        # submission status
        self.status = const.SUBMISSION_PENDING
        # different datetimes of the submission
        self.createtime = util.utcnow()
        self.submittime = util.utcnow(validate_delay(delay))
        self.starttime = None
        self.finishtime = None
        # task payload (replaced with serialization string of the task on successful conversion)
        self.payload = payload if payload else self.DEFAULT_PAYLOAD
        # submission username (currently is not used)
        self.username = username if username else self.DEFAULT_USERNAME

    def set_status(self, status):
        """
        Set submission status. Status must be one of SUBMISSION_STATUSES.

        :param status: status to update to
        """
        if status not in const.SUBMISSION_STATUSES:
            raise ValueError("Status %s is unrecognized" % status)
        self.status = status

    def is_delayed(self):
        """
        Whether or not submission is delayed. Note that 'delayed_seconds' can be negative, in this
        case it is assumed that submission is not delayed and equivalent of delay = 0 seconds.

        :return: True if submission is delayed, False otherwise
        """
        delayed_seconds = int((self.submittime - self.createtime).total_seconds())
        return delayed_seconds > const.SUBMISSION_DELAY_MIN

    def mark_start(self):
        """
        Mark start of the submission by setting start time to current UTC time.
        """
        self.starttime = util.utcnow()

    def mark_finish(self):
        """
        Mark finish of the submission by setting finish time to current UTC time.
        """
        self.finishtime = util.utcnow()

    def mark_deleted(self):
        """
        Mark this job as deleted.
        """
        self.is_deleted = True

    def dumps(self):
        """
        Create dictionary of job properties, for example to store in database. This is different
        from generic serialization with pickle, because we need searchable fields.

        :return: object dictionary that can be used to create Submission
        """
        return {
            "uid": self.uid,
            "name": self.name,
            "system_code": self.system_code,
            "priority": self.priority,
            "is_deleted": self.is_deleted,
            "is_template": self.is_template,
            "status": self.status,
            "createtime": self.createtime,
            "submittime": self.submittime,
            "starttime": self.starttime,
            "finishtime": self.finishtime,
            "payload": self.payload,
            "username": self.username
        }

    @classmethod
    def loads(cls, obj):
        """
        Create Submission from dictionary of submission properties. Note that validation is not
        applied, this method should be used internally to load data from database.

        :param obj: submission dictionary obtained with 'dumps()' method
        :return: Submission instance
        """
        submission = cls(obj["name"], obj["system_code"])
        submission.uid = obj["uid"]
        submission.priority = obj["priority"]
        submission.is_deleted = obj["is_deleted"]
        submission.is_template = obj["is_template"]
        submission.status = obj["status"]
        # different datetimes of the submission
        submission.createtime = obj["createtime"]
        submission.submittime = obj["submittime"]
        submission.starttime = obj["starttime"]
        submission.finishtime = obj["finishtime"]
        # submission payload (can be serialized Task)
        submission.payload = obj["payload"]
        # submission username
        submission.username = obj["username"]
        return submission

class Template(Submission):
    """
    Template interface to store incomplete submissions. These templates are only used as base points
    to construct submissions, currently submission can only have one template.
    """
    def __init__(self, name, system, payload=None, username=None):
        # template has dummy priority of PRIORITY_2
        super(Template, self).__init__(name, system, priority=const.PRIORITY_2, payload=payload,
                                       username=username)
        self.is_template = True
