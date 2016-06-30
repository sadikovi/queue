#!/usr/bin/env python

import os

# == OS related methods ==

def readwriteDirectory(unresolved_directory):
    """
    Resolve absolute path for the unresolved directory, check that it exists and valid, and
    also check read-write access to the directory.

    :param unresolved_directory: unresolved directory
    """
    normpath = os.path.realpath(os.path.abspath(unresolved_directory))
    if not os.path.isdir(normpath):
        raise StandardError("Path %s is not a directory" % normpath)
    if not os.access(normpath, os.R_OK) or not os.access(normpath, os.W_OK):
        raise StandardError("Insufficient permissions for %s, expected read-write" % normpath)
    return normpath
