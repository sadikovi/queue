#!/usr/bin/env python

import logging
import multiprocessing
import Queue as threadqueue
import time

class Message(object):
    """
    Base class for message task. Takes dictionary of values to return, note that values must be
    pickle-able, even better if it is just single string text, or a list.
    """
    def __init__(self, status, **kwargs):
        self.status = status
        self.arguments = kwargs
        self.pretty_name = u"%s[%s]%s" % (type(self).__name__, self.status, self.arguments)

    def __str__(self):
        return self.pretty_name

    def __unicode__(self):
        return self.pretty_name

    def __repr__(self):
        return self.pretty_name

# Set of messages for executors
MESSAGE_SHUTDOWN = "SHUTDOWN"
MESSAGE_TASK_STARTED = "MESSAGE_TASK_STARTED"
MESSAGE_TASK_FINISHED = "MESSAGE_TASK_FINISHED"

class TerminationException(Exception):
    """
    Custom exception for executor termination or any other valid termination.
    """
    def __init__(self, message=None):
        msg = message if message else "Requested termination"
        super(TerminationException, self).__init__(msg)

class Executor(multiprocessing.Process):
    """
    Executor process to run tasks and receive messages from scheduler. It represents long running
    process with polling interval, all communications are done through pipe. Executor guarantees
    termination of task with termination of subprocess, assuming that task implements interface
    correctly.
    """
    def __init__(self, name, conn, task_queue, timeout=0.5, logger=None):
        """
        Create instance of Executor.

        :param name: friendly executor name, e.g. "executor-1"
        :param conn: Connection instance to receive and send messages
        :param task_queue: task queue
        :param timeout: polling interval
        :param logger: provided logger, if None then default logger is used
        """
        self.name = name
        self.conn = conn
        self.task_queue = task_queue
        self.timeout = timeout
        # if no logger defined create new logger and add null handler
        if logger:
            self.logger = logger
        else:
            log_name = "%s[%s]" % (type(self).__name__, self.name)
            self.logger = self._get_default_logger(log_name)
        # we also keep reference to active task, this will be reassigned for every iteration
        self.active_task = None
        super(Executor, self).__init__()

    def _get_default_logger(self, name):
        """
        Internal method to set default logger. Should be moved into utility functions or unified
        package instead. Creates logger for name provided.

        :param name: logger name
        :return: default logger
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        form = logging.Formatter("LOG :: %(asctime)s :: %(name)s :: %(levelname)s :: %(message)s")
        stderr = logging.StreamHandler()
        stderr.setFormatter(form)
        logger.addHandler(stderr)
        return logger

    def iteration(self):
        """
        Run single iteration, entire logic of executor should be specified in this method, unless
        there is an additional logic between iterations.

        :return: boolean flag, True - run next iteration, False - terminate
        """
        self.logger.info("Run iteration for %s, timeout=%s", self.name, self.timeout)
        try:
            # check if there are any messages in connection
            if self.conn.poll():
                self._process_message(self.conn.recv())
            # check if there is any outstanding task to run, otherwise poll data for current task
            self._process_task()
        except TerminationException:
            self.logger.info("Requested termination of executor %s", self.name)
            return False
        # pylint: disable=W0703,broad-except
        except Exception as e:
            self.logger.exception("Unrecoverable error %s, shutting down executor %s", e, self.name)
            return False
        # pylint: enable=W0703,broad-except
        else:
            return True

    def _process_message(self, msg):
        """
        Process message and take action, e.g. terminate process, execute callback, etc. Message
        types are defined above in the package.

        :param msg: message to process
        """
        if isinstance(msg, Message) and msg.status == MESSAGE_SHUTDOWN:
            raise TerminationException()
        else:
            self.logger.debug("Invalid message %s is ignored", msg)

    def _process_task(self):
        """
        Process individual task, returns exit code for each task following available API.

        :return: task exit code, see Task API for more information
        """
        # if we have active task, we poll status of the task. If task is finished, we send message
        # to the main thread
        if not self.active_task:
            try:
                # extract element without blocking, raises Queue.Empty, if no tasks are available
                self.active_task = self.task_queue.get(block=False)
            except threadqueue.Empty:
                self.logger.debug("No tasks available")
                self.active_task = None
        # at this point we check if we can actually launch task
        if self.active_task:
            task_id = self.active_task.uid
            if self.active_task.is_pending():
                # now we need to launch it and log action, note that launch should be asynchronous
                self.active_task.async_launch()
                self.conn.send(Message(MESSAGE_TASK_STARTED, task_id=task_id))
                self.logger.info("Started task %s", task_id)
                return None
            elif self.active_task.is_running():
                # task is running, nothing we can do, but wait
                return None
            elif self.active_task.is_done():
                exit_code = self.active_task.exit_status()
                self.conn.send(Message(MESSAGE_TASK_FINISHED, task_id=task_id, exit_code=exit_code))
                self.logger.info("Finished task %s, exit_code=%s", task_id, exit_code)
                self.active_task = None
                return exit_code

    def run(self):
        """
        Method to run tasks on executor, this runs in iterations with each timeout interval. Each
        iteration polls new messages from connection and checks running task. If iteration fails we
        immediately return status False.
        """
        proceed = True
        while proceed:
            proceed = self.iteration()
            if not proceed:
                return False
            time.sleep(self.timeout)
