#!/usr/bin/env python

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
    name = str(value).strip()
    return const.SUBMISSION_DEFAULT_NAME if not name else name

class Submission(object):
    """
    Submission interface for queue, external wrapper for Task interface. Note that it contains only
    generic job information, and specific to the under-system data is stored in 'payload'.
    Submission status is also more generic that task status, though there is direct one-to-one
    mapping with some additional values for submission. Do not use this interface to create
    templates, use Template class instead.
    """
    DEFAULT_PAYLOAD = {}
    DEFAULT_USERNAME = "default"

    def __init__(self, name, system, delay=0, payload=None, username=None, template_uid=None):
        """
        Create Submission instance. Note that unique identifier should be set afterwards, if
        submission is valid.

        :param name: submission name
        :param system: under-system code
        :param delay: delay in seconds, will be converted into submission datetime
        :param payload: payload for specific task, default is None
        :param username: user created submission, default is None
        :param template_uid: template to use in addition to payload, default is None
        """
        self.__uid = None
        self.__name = validate_name(name)
        # code for under-system
        self.system_code = system
        # template settings
        self.template_uid = template_uid
        self._is_template = False
        # options with setters
        self.__status = const.SUBMISSION_PENDING
        self.__createtime = util.utcnow()
        self.__submittime = util.utcnow(validate_delay(delay))
        self.__starttime = None
        self.__finishtime = None
        # task payload
        self.payload = payload if payload else self.DEFAULT_PAYLOAD
        # submission username (currently is not used)
        self.username = username if username else self.DEFAULT_USERNAME

    @property
    def uid(self):
        return self.__uid

    @property
    def name(self):
        return self.__name

    def set_uid(self, uid):
        """
        Set uid for submission, only available if current id is None, otherwise raises error.
        """
        if not uid:
            raise AttributeError("Invalid value %s for submission id" % uid)
        if self.__uid is not None:
            raise ValueError("Submission id is already set to %s" % self.__uid)
        self.__uid = uid

    def set_status(self, status):
        """
        Set submission status. Status must be one of SUBMISSION_STATUSES.

        :param status: status to update to
        """
        if status not in const.SUBMISSION_STATUSES:
            raise ValueError("Status %s is unrecognized" % status)
        self.__status = status

    def mark_start(self):
        """
        Mark start of the submission by setting start time to current UTC time.
        """
        self.__starttime = util.utcnow()

    def mark_finish(self):
        """
        Mark finish of the submission by setting finish time to current UTC time.
        """
        self.__finishtime = util.utcnow()

class Template(object):
    """
    Template interface to store incomplete submissions. These templates are only used as base points
    to construct submissions, currently submission can only have one template.
    """
    def __init__(self, name, system, payload=None, username=None):
        super(Template, self).__init__(name, system, payload=payload, username=username)
        self._is_template = True
