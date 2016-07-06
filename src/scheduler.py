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
MESSAGE_TASK_STARTED = "TASK_STARTED"
MESSAGE_TASK_FINISHED = "TASK_FINISHED"
MESSAGE_TASK_KILLED = "TASK_KILLED"

class TerminationException(Exception):
    """
    Custom exception for executor termination or any other valid termination.
    """
    def __init__(self, message=None):
        msg = message if message else "Requested termination"
        super(TerminationException, self).__init__(msg)

class Task(object):
    """
    Task is a unit of execution in scheduler, all backends should implement this interface for
    their execution requests, so they can be launched on executor. Note that some methods might
    require availability status of backend. Main task API consists of several methods:
    - status()
    - async_launch()
    - terminate()

    Also each task must overwrite property for a unique identifier, and exit_code to return status
    of finished task.
    """

    # == Task statuses ==
    # Task is blocked, e.g. by backend availability, will not be scheduled until resolved
    BLOCKED = "BLOCKED"
    # Task is pending, next after BLOCKED status, meaning good to launch
    PENDING = "PENDING"
    # Task is running on backend
    RUNNING = "RUNNING"
    # Task is finished, either failed or succeeded, which is determined by exit code
    FINISHED = "FINISHED"

    @property
    def uid(self):
        """
        Return unique identifier for this task, it is recommended to be globally unique.

        :return: unique identifier
        """
        raise NotImplementedError()

    @property
    def exit_code(self):
        """
        Return exit code (status) of the task. If task has not been launched or running, should
        return None, and return valid status in case it was finished or terminated.

        :return: exit code
        """
        raise NotImplementedError()

    def status(self):
        """
        Return current status of the task. Should take into account backend availability, state of
        the resources allocated, or background process running.

        :return: enum status of the task (see above)
        """
        raise NotImplementedError()

    def async_launch(self):
        """
        Asynchronously launch task, because executor does not support launching tasks in a separate
        thread yet. For example, spawning shell process in the background, etc.
        """
        raise NotImplementedError()

    def cancel(self):
        """
        Cancel task, this should ask termination of all background processes and close resources
        associated with task. Can block thread to execute code, must set exit code for the task.
        """
        raise NotImplementedError()

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
        # flag to indicate if executor is terminated
        self._terminated = False
        super(Executor, self).__init__(name=name)

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
        there is an additional logic between iterations. Iteration is cancelled, if executor is
        terminated.

        :return: boolean flag, True - run next iteration, False - terminate
        """
        # we process special case of terminated executor in case someone would launch it again.
        if self._terminated:
            self.logger.warning("Executor has been terminated, iteration is cancelled")
            return False
        self.logger.debug("Run iteration for %s, timeout=%s", self.name, self.timeout)
        try:
            # check if there are any messages in connection
            if self.conn.poll():
                self._process_message(self.conn.recv())
            # check if there is any outstanding task to run, otherwise poll data for current task
            self._process_task()
        except TerminationException:
            self.logger.info("Requested termination of executor %s", self.name)
            self._terminated = True
            # If we encounter termination of executor, we should cancel current running task and
            # set block launch of any other tasks
            self._cancel_task()
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
        types are defined above in the package. Note that this can take actions on tasks, e.g.
        when task is cancelled, so the subsequent processing of task, will work with updated state.

        :param msg: message to process
        """
        self.logger.debug("Received message %s", msg)
        if isinstance(msg, Message) and msg.status == MESSAGE_SHUTDOWN:
            raise TerminationException()
        else:
            self.logger.info("Invalid message %s is ignored", msg)

    def _process_task(self):
        """
        Process individual task, returns exit code for each task following available API.

        :return: task exit code (see Task API), or None, if exit code is not resolved
        """
        if not self.active_task:
            try:
                # Extract element without blocking, raises Queue.Empty, if no tasks are available
                self.active_task = self.task_queue.get(block=False)
            except threadqueue.Empty:
                self.logger.info("No tasks available")
                self.active_task = None
        # At this point we check if we can actually launch task
        exit_code = None
        if self.active_task:
            task_id = self.active_task.uid
            status = self.active_task.status()
            if status == Task.PENDING:
                # now we need to launch it and log action, note that launch should be asynchronous
                self.active_task.async_launch()
                self.conn.send(Message(MESSAGE_TASK_STARTED, task_id=task_id))
                self.logger.info("Started task %s", task_id)
                exit_code = None
            elif status == Task.RUNNING:
                # task is running, nothing we can do, but wait
                exit_code = None
            elif status == Task.FINISHED:
                exit_code = self.active_task.exit_code
                self.conn.send(Message(MESSAGE_TASK_FINISHED, task_id=task_id, exit_code=exit_code))
                self.logger.info("Finished task %s, exit_code=%s", task_id, exit_code)
                self.active_task = None
            else:
                self.logger.info("Blocked task %s, ping under-system implementation", task_id)
        # Return exit code obtained from if-else branches, note that if executor does not have any
        # tasks to run, we return None similar to when task is running.
        return exit_code

    def _cancel_task(self):
        """
        Cancel current running task, if available.
        """
        if self.active_task:
            task_id = self.active_task.uid
            self.active_task.cancel()
            self.conn.send(Message(MESSAGE_TASK_KILLED, task_id=task_id))
            self.logger.info("Cancelled task %s", task_id)
            self.active_task = None
        else:
            self.logger.info("No active task to terminate")

    def run(self):
        """
        Method to run tasks on executor, this runs in iterations with each timeout interval. Each
        iteration polls new messages from connection and checks running task. If iteration fails we
        immediately return status False.
        """
        proceed = True
        while proceed: # pragma: no branch
            proceed = self.iteration()
            if not proceed:
                return False
            time.sleep(self.timeout)
