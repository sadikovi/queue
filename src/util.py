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
        raise OSError("Path %s is not a directory" % normpath)
    if not os.access(normpath, os.R_OK) or not os.access(normpath, os.W_OK):
        raise OSError("Insufficient permissions for %s, expected read-write" % normpath)
    return normpath

def readonlyFile(unresolved_filepath):
    """
    Resolve absolute path for the unresolved filepath, check that path exists and valid, and read
    access is granted.

    :param unresolved_filepath: unresolved path to the file
    :return: fully resolved absolute path to the file with read access
    """
    normpath = os.path.realpath(os.path.abspath(unresolved_filepath))
    if not os.path.isfile(normpath):
        raise OSError("Path %s is not a file path" % normpath)
    if not os.access(normpath, os.R_OK):
        raise OSError("Insufficient permissions for %s, expected read access" % normpath)
    return normpath

def concat(root, *paths):
    """
    Shortcut for `os.path.join()` method.

    :param root: main part of path
    :param *paths: suffices to append to the root
    :return: concatenated path
    """
    return os.path.join(root, *paths)

def mkdir(path, mode):
    """
    Shortcut for `os.mkdir()` method.

    :param path: path to create
    :param mode: permissions octal value
    """
    if not path:
        raise ValueError("Invalid path %s is provided" % path)
    if not mode:
        raise ValueError("Invalid mode %s is provided" % mode)
    os.mkdir(path, mode)

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
