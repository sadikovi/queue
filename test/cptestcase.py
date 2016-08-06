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

from StringIO import StringIO
import unittest
import urllib

import cherrypy

# Not strictly speaking mandatory but just makes sense
cherrypy.config.update({'environment': "test_suite"})

# This is mandatory so that the HTTP server isn't started
# if you need to actually start (why would you?), simply
# subscribe it back.
cherrypy.server.unsubscribe()

# simulate fake socket address... they are irrelevant in our context
local = cherrypy.lib.httputil.Host('127.0.0.1', 50000, "")
remote = cherrypy.lib.httputil.Host('127.0.0.1', 50001, "")

__all__ = ['BaseCherryPyTestCase']

class BaseCherryPyTestCase(unittest.TestCase): # pragma: no cover
    def request(self, path='/', method='GET', app_path='', scheme='http',
                proto='HTTP/1.1', data=None, headers=None, **kwargs):
        """
        CherryPy does not have a facility for serverless unit testing.
        However this recipe demonstrates a way of doing it by
        calling its internal API to simulate an incoming request.
        This will exercise the whole stack from there.

        Remember a couple of things:

        * CherryPy is multithreaded. The response you will get
          from this method is a thread-data object attached to
          the current thread. Unless you use many threads from
          within a unit test, you can mostly forget
          about the thread data aspect of the response.

        * Responses are dispatched to a mounted application's
          page handler, if found. This is the reason why you
          must indicate which app you are targetting with
          this request by specifying its mount point.

        You can simulate various request settings by setting
        the `headers` parameter to a dictionary of headers,
        the request's `scheme` or `protocol`.

        .. seealso:
        http://docs.cherrypy.org/stable/refman/_cprequest.html#cherrypy._cprequest.Response
        """
        # This is a required header when running HTTP/1.1
        hdict = {'Host': '127.0.0.1'}

        if headers is not None:
            hdict.update(headers)

        # If we have a POST/PUT request but no data
        # we urlencode the named arguments in **kwargs
        # and set the content-type header
        if method in ('POST', 'PUT') and not data:
            data = urllib.urlencode(kwargs)
            kwargs = None
            hdict['content-type'] = 'application/x-www-form-urlencoded'

        # If we did have named arguments, let's
        # urlencode them and use them as a querystring
        qs = None
        if kwargs:
            qs = urllib.urlencode(kwargs)

        # if we had some data passed as the request entity
        # let's make sure we have the content-length set
        fd = None
        if data is not None:
            hdict['content-length'] = '%d' % len(data)
            fd = StringIO(data)

        # Get our application and run the request against it
        app = cherrypy.tree.apps.get(app_path)
        if not app:
            raise AssertionError("No application mounted at '%s'" % app_path)

        # Cleanup any previous returned response
        # between calls to this method
        app.release_serving()

        # Let's fake the local and remote addresses
        request, response = app.get_serving(local, remote, scheme, proto)
        try:
            hdict = [(k, v) for k, v in hdict.iteritems()]
            response = request.run(method, path, qs, proto, hdict, fd)
        finally:
            if fd:
                fd.close()
                fd = None

        if response.output_status.startswith('500'):
            print response.body
            raise AssertionError("Unexpected error")

        # collapse the response into a bytestring
        response.collapse_body()
        return response
