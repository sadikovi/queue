#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# Copyright 2016 sadikovi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
        self._logger = logger(self.name) if logger else None

    def is_cancelled(self):
        return self._cancelled

    @property
    def logger(self):
        return self._logger if self._logger else util.get_default_logger(self.name)

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
