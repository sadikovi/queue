#!/usr/bin/env python

import os
import urlparse

# == OS related methods and classes ==
def readwriteDirectory(unresolved_directory):
    """
    Resolve absolute path for the unresolved directory, check that it exists and valid, and
    also check read-write access to the directory.

    :param unresolved_directory: unresolved directory
    :return: fully resolved absolute path as directory with read-write access
    """
    normpath = os.path.realpath(os.path.abspath(unresolved_directory))
    if not os.path.isdir(normpath):
        raise StandardError("Path %s is not a directory" % normpath)
    if not os.access(normpath, os.R_OK) or not os.access(normpath, os.W_OK):
        raise StandardError("Insufficient permissions for %s, expected read-write" % normpath)
    return normpath

# == REST API and URI related methods and classes ===
class URI(object):
    """
    URI class to keep information about URL and components, parses and validates components
    """
    def __init__(self, rawURI, alias=None):
        uri = urlparse.urlsplit(rawURI)
        # we expect scheme, hostname and port to be set, otherwise uri is considered invalid
        if not uri.port or not uri.hostname or not uri.scheme:
            raise StandardError("Invalid URI - expected host, port and scheme from %s" % rawURI)
        self._host = uri.hostname
        self._port = int(uri.port)
        self._scheme = uri.scheme
        self._netloc = uri.netloc
        self._fragment = uri.fragment
        self._url = uri.geturl()
        # Alias for URL if it is too long, if None provided url is used
        self._alias = alias

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def scheme(self):
        return self._scheme

    @property
    def netloc(self):
        return self._netloc

    @property
    def fragment(self):
        return self._fragment

    @property
    def url(self):
        return self._url

    @property
    def alias(self):
        return self._alias if self._alias else self._url
