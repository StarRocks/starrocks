# !/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
#
# Copyright (c) 2020, Dingshi Inc.
# All rights reserved.
#
###########################################################################

"""
run
@Time : 2022/11/4 10:05
@Author : Brook.Ye
"""
import datetime
import getopt
import os
import sys

import nose

from lib import sr_sql_lib


def print_help():
    """help"""
    print(
        """
python run.py [-d dirname/file] [-r] [-l] [-c ${concurrency}] [-t ${time}] [-a ${attr}] [--file_filter=${regex}] [--case_filter=${regex}]
              -d|--dir             Case dirname|filename, default "./sql"
              -r|--record          SQL record mode, run cases in T and generate R
              -v|--validate        [DEFAULT] SQL validate mode, run cases in R, and check the results
              -p|--part            Partial update of results, only works in record mode
              -c|--concurrency     Concurrency limit, >0, default 8.
              -t|--timeout         Timeout(s) of each case, >0, default 600
              -l|--list            Only list cases name and don't run
              -a|--attr            Case attrs for filter, default all cases, e.x: system,admit
              --file_filter        Case file regex for filter, default .*
              --case_filter        Case name regex for filter, default .*
        """
    )


if __name__ == "__main__":
    """ main """

    record = False
    dirname = None
    concurrency = 8
    timeout = 600
    file_filter = ".*"
    case_filter = ".*"
    collect = False
    part = False

    args = "hld:rvc:t:x:y:pa:"
    detail_args = ["help", "list", "dir=", "record", "validate", "concurrency=", "timeout=", "file_filter=",
                   "case_filter=", "part", "attr="]

    case_dir = None
    # case_dir = "/home/disk2/yzy/starRocksTest_fault/case/sql_case/test_dql"
    # filename_regex = "test_show_frontend.*"
    # case_name_regex = ".*"

    filename_regex = ".*"
    # filename_regex = "test_array_slice.*"

    case_name_regex = ".*"

    attr = ""

    try:
        opts, args = getopt.getopt(sys.argv[1:], args, detail_args)
    except Exception as e:
        print(e)
        print_help()
        sys.exit(1)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print_help()
            sys.exit(0)

        if opt in ("-d", "--dir"):
            dirname = arg

        if opt in ("-r", "--record"):
            record = True

        if opt in ("-v", "--validate"):
            record = False

        if opt in ("-c", "--concurrency"):
            concurrency = int(arg)

        if opt in ("-t", "--timeout"):
            timeout = int(arg)

        if opt == "--file_filter":
            file_filter = arg

        if opt == "--case_filter":
            case_filter = arg

        if opt in ("-l", "--list"):
            collect = True

        if opt in ("-p", "--part"):
            part = True

        if opt in ("-a", "--attr"):
            attr = arg

    version = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    print("Version: %s" % version)
    os.environ["version"] = version

    # set environment
    os.environ["record_mode"] = "true" if record else "false"
    os.environ["sql_dir"] = str(dirname)
    os.environ["file_filter"] = file_filter
    os.environ["case_filter"] = case_filter
    os.environ["attr"] = attr

    argv = [
        "nosetests",
        "test_sql_cases.py",
        "-vv",
        "-s",
        "--nologcapture",
        # "--with-xunitmp"
        # "--with-html-output",
        # "--html-out-file=result1.html"
    ]

    # concurrency
    if concurrency <= 0:
        print("-c|--concurrency must > 0!")
        print_help()
        sys.exit(3)
    argv += ["--processes=%s" % concurrency]

    # timeout setting of each case
    if timeout <= 0:
        print("-t|--timeout must > 0!")
        print_help()
        sys.exit(4)
    argv += ["--process-timeout=%s" % timeout]

    if collect:
        argv += ["--collect-only"]

    print("Test cmd: %s" % " ".join(argv))

    nose.run(argv=argv)

    # record mode
    if record:
        sr_sql_lib.StarrocksSQLApiLib().save_r_into_file(part)
