#!/usr/bin/env python

import time
import src.const as const
import src.context as context
import src.scheduler as scheduler
import src.util as util

SIMPLE_SYSTEM_CODE = "SIMPLE"

class SimpleTask(scheduler.Task):
    """
    Simple test task. Runs simple loop for number of iterations, can be cancelled. Mainly used for
    testing.
    """
    def __init__(self, uid, priority, logger=None):
        self._uid = uid
        self._priority = priority
        self._cancelled = False
        self._iterations = 10
        self.name = "SimpleTask"
        self.logger = logger(self.name) if logger else util.get_default_logger(self.name)

    def is_cancelled(self):
        return self._cancelled

    @property
    def uid(self):
        return self._uid

    @property
    def priority(self):
        return self._priority

    def run(self):
        for i in range(0, self._iterations):
            if self.is_cancelled():
                self.logger.debug("Cancelled on iteration %s", i)
                break
            self.logger.debug("Iteration %s", i)
            time.sleep(1.0)

    def cancel(self):
        self._cancelled = True

class SimpleSession(context.Session):
    """
    Simple test session, used to test functionality of the queue controller. Reports that system
    is always available, uses standard scheduler, can be launched with as many executors as
    required, because 'SimpleTask' does not require external system to run.
    """
    def __init__(self, num_executors, timeout=1.0, logger=None):
        self.num_executors = num_executors
        self.timeout = timeout
        self.log_func = logger
        self._scheduler = scheduler.Scheduler(self.num_executors, timeout=self.timeout,
                                              logger=self.log_func)

    def system_code(self):
        return SIMPLE_SYSTEM_CODE

    def system_uri(self):
        return None

    def status(self):
        return const.SYSTEM_AVAILABLE

    def create_task(self, sub):
        return SimpleTask(sub.uid, sub.priority, self.log_func)

    @property
    def scheduler(self):
        return self._scheduler

    @classmethod
    def create(cls, conf, working_dir, logger):
        timeout = conf.getConfFloat(const.OPT_SCHEDULER_TIMEOUT)
        num_parallel_tasks = conf.getConfInt(const.OPT_NUM_PARALLEL_TASKS)
        return cls(num_parallel_tasks, timeout=timeout, logger=logger)
