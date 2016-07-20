#!/usr/bin/env python

import sys
import argparse
from init import LIB_PATH, VERSION

# Currently Python 2.7 is supported
PYTHON_VERSION_MAJOR = 2
PYTHON_VERSION_MINOR = 7
if sys.version_info.major != PYTHON_VERSION_MAJOR or sys.version_info.minor != PYTHON_VERSION_MINOR:
    print "[ERROR] Only Python %s.%s is supported" % (PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR)
    sys.exit(1)

# Run only on OS X and Linux
if not (sys.platform.startswith("darwin") or sys.platform.startswith("linux")):
    print "[ERROR] Only OS X and Linux are supported"
    sys.exit(1)

# Add dependencies to the path
sys.path.insert(1, LIB_PATH)

# Load src modules (need to load after lib path is added to sys.path)
# pylint: disable=C0413,wrong-import-position
import src.queue as qservice
import src.const as qconst
# pylint: enable=C0413,wrong-import-position

def create_cli_parser():
    """
    Create CLI parser, we expect "host" and "port" options to launch service, the rest should be
    optional in format "x.y", where "x" is a group, e.g. spark, and "y" is a name of the option.
    For example, 'spark.master' is Spark master address, defaults to empty string and specify using
    '--spark.master=spark://master:7077'.

    :return: cli parser
    """
    parser = argparse.ArgumentParser(description="Starts Queue scheduler service")
    parser.add_argument("--host", required=True, help="host to bind application to")
    parser.add_argument("--port", required=True, type=int, help="port to bind application to")
    # add group for generic queue options
    generic_group = parser.add_argument_group("generic", "generic arguments for all groups")
    generic_group.add_argument(
        "--%s" % qconst.OPT_NUM_PARALLEL_TASKS, default=1, type=int, metavar="NUM",
        help="number of tasks to launch in parallel (default: 1)")
    generic_group.add_argument(
        "--%s" % qconst.OPT_SCHEDULER_TIMEOUT, default=1.0, type=float, metavar="SECONDS",
        help="scheduler timeout in seconds (default: 1.0)")
    generic_group.add_argument(
        "--%s" % qconst.OPT_WORKING_DIR, default="", metavar="WORK",
        help="application working directory (logs, job artifacts), must have read/write access")
    generic_group.add_argument(
        "--%s" % qconst.OPT_SERVICE_DIR, default="", metavar="SERVICE",
        help="service directory to traverse (jars, text files), must have read access")
    # add group for Spark options
    spark_group = parser.add_argument_group("spark", "arguments to create Spark session/scheduler")
    spark_group.add_argument(
        "--%s" % qconst.OPT_SPARK_MASTER, default="", help="Spark master address (default: '')")
    spark_group.add_argument(
        "--%s" % qconst.OPT_SPARK_WEB, default="", help="Spark web (REST) address (default: '')")
    return parser

def main():
    """
    Main function to start application.

    Based on arguments it generates Queue controller with selected session and scheduler, and
    starts all services.
    """
    parser = create_cli_parser()
    namespace = parser.parse_args()
    args = vars(namespace)
    print "Start service..."
    print r"""
       ____
      / __ \__  _____  __  _____
     / / / / / / / _ \/ / / / _ \
    / /_/ / /_/ /  __/ /_/ /  __/
    \___\_\__,_/\___/\__,_/\___/  version %s
    """ % VERSION
    qservice.start(host=namespace.host, port=namespace.port, args=args)

if __name__ == "__main__":
    main()
