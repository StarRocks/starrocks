# !/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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

if not os.environ.get("version"):
    version = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    os.environ["version"] = version

from lib import sr_sql_lib, ColorEnum
from lib.sr_sql_lib import self_print


DEFAULT_TIMEOUT = 600


def print_help():
    """help"""
    print(
        """
python run.py [-d dirname/file] [-r] [-l] [-c ${concurrency}] [-t ${time}] [-a ${attr}] [-C ${cluster_type}] [--file_filter=${regex}] [--case_filter=${regex}]
              -d|--dir             Case dirname|filename, default "./sql"
              -r|--record          SQL record mode, run cases in T and generate R
              -v|--validate        [DEFAULT] SQL validate mode, run cases in R, and check the results
              -p|--part            Partial update of results, only works in record mode
              -c|--concurrency     Concurrency limit, >0, default 8.
              -t|--timeout         Timeout(s) of each case, >0, default 600
              -l|--list            Only list cases name and don't run
              -a|--attr            Case attrs for filter, default all cases, e.x: system,admit
              -C|--cluster         Cluster Type, cloud|native, default is native"
              --skip_reruns        skip 3 time reruns, all case will be run exactly once, default False
              --file_filter        Case file regex for filter, default .*
              --case_filter        Case name regex for filter, default .*
              --config             Config path, default conf/sr.conf
              --keep_alive         Check cluster status before each case, only works with sequential mode(-c=1)
              --run_info           Extra info
        """
    )


if __name__ == "__main__":
    """main"""

    record = False
    dirname = None
    concurrency = 8
    timeout = DEFAULT_TIMEOUT
    file_filter = ".*"
    case_filter = ".*"
    collect = False
    part = False
    skip_reruns = False
    config = "conf/sr.conf"
    keep_alive = False
    run_info = ""

    args = "hld:rvc:t:x:y:pa:C:"
    detail_args = [
        "help",
        "list",
        "dir=",
        "record",
        "validate",
        "concurrency=",
        "timeout=",
        "file_filter=",
        "case_filter=",
        "part",
        "attr=",
        "cluster=",
        "skip_reruns",
        "config=",
        "keep_alive",
        "run_info=",
        "log_filtered"
    ]

    case_dir = None

    filename_regex = ".*"

    case_name_regex = ".*"

    attr = ""

    cluster = "native"

    log_filtered = False

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

        if opt in ("-C", "--cluster"):
            cluster = arg

        if opt == "--skip_reruns":
            skip_reruns = True

        if opt == "--config":
            config = arg

        if opt == "--keep_alive":
            keep_alive = True

        if opt == "--run_info":
            run_info = arg

        if opt == "--log_filtered":
            log_filtered = True

    # merge cluster info to attr
    cluster_attr = "!cloud" if cluster == "native" else "!native"
    attr = f"{attr},{cluster_attr}".strip(",")
    # check sequential mode with concurrency=1
    if 'sequential' in attr and concurrency != 1:
        print("In sequential mode, set concurrency=1 in default!")
        concurrency = 1
    # check alive mode with concurrency=1
    if keep_alive and concurrency != 1:
        print("In alive mode, set concurrency=1 in default!")
        concurrency = 1

    # set environment
    os.environ["record_mode"] = "true" if record else "false"
    os.environ["sql_dir"] = str(dirname)
    os.environ["file_filter"] = file_filter
    os.environ["case_filter"] = case_filter
    os.environ["attr"] = attr
    os.environ["config_path"] = config
    os.environ["keep_alive"] = str(keep_alive)
    os.environ['run_info'] = run_info
    os.environ['log_filtered'] = str(log_filtered)

    argv = [
        "nosetests",
        "test_sql_cases.py",
        "-v",
        "-s",
        "--nologcapture",
    ]

    if not skip_reruns and not record:
        argv.extend([
            "--with-flaky",
            "--force-flaky",
            "--max-runs=3",
            "--min-passes=3",
        ])

    # concurrency
    if concurrency <= 0:
        print("-c|--concurrency must > 0!")
        print_help()
        sys.exit(3)
    argv += ["--processes=%s" % concurrency]

    # timeout setting of each case
    if not 0 < timeout <= 10 * 60:
        print("-t|--timeout(s) must be in (0, 10min]!")
        print_help()
        sys.exit(4)
    argv += ["--process-timeout=%s" % timeout]
    argv += ["--process-restartworker"]

    # test xml
    if not record:
        argv += ["--with-xunitmp"]

    if collect:
        argv += ["--collect-only"]

    self_print("Test cmd: %s" % " ".join(argv), ColorEnum.GREEN)

    nose.run(argv=argv)

    # record mode
    if record and not collect:
        sr_sql_lib.StarrocksSQLApiLib().save_r_into_file(part)
