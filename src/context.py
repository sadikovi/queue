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

    def create_task(self, submission):
        """
        Create task from provided submission. If submission is invalid or cannot be converted into
        task, method should raise an error. Task must have the same 'uid' field as submission, this
        allows to index the same column and query by the same value.

        :param: Submission instance
        :return: Task instance for this submission
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
    def create(cls, conf, working_dir, logger):
        """
        Create instance of session using dictionary of configuration options. All options are in
        format "group.name", where "group" is option group, e.g. spark, and "name" is a name of
        the option, e.g. spark.master.

        :param conf: QueueConf instance
        :param working_dir: verified working directory
        :param logger: logger function
        :return: instance of session
        """
        raise NotImplementedError("Not implemented")
