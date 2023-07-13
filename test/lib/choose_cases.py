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
choose_case
@Time : 2022/10/28 10:57
@Author : Brook.Ye
"""
import copy
import json
import os
import re
import sys
import uuid
from collections import OrderedDict
from typing import List

from cup import log
from nose import tools

from lib import skip
from lib import sr_sql_lib
from lib.sr_sql_lib import RESULT_FLAG, UNCHECK_FLAG
from lib.sr_sql_lib import RESULT_END_FLAT
from lib.sr_sql_lib import SHELL_FLAG
from lib.sr_sql_lib import FUNCTION_FLAG
from lib.sr_sql_lib import NAME_FLAG


CASE_DIR = "sql"


class ChooseCase(object):
    class CaseTR(object):
        def __init__(self, ctx, name, file, sql, result, info):
            """ init """
            super().__init__()
            self.ctx = ctx
            self.info = info
            self.name = name
            self.file = file
            self.sql: List = sql
            self.ori_sql: List = copy.deepcopy(sql)
            self.result: List = result

            variable_dict = {}

            # get db from lines
            self.db = set()
            self.resource = set()
            self.init_cmd = []

            # identify uuid and replace them, save into variable_dict
            for each_sql in sql:
                uuid_vars = re.findall(r"\${(uuid[0-9]*)}", each_sql)

                for each_uuid in uuid_vars:
                    if each_uuid not in variable_dict:
                        variable_dict[each_uuid] = uuid.uuid4().hex

            for sql_id, each_sql in enumerate(sql):

                # replace uuid
                for each_var in variable_dict:
                    each_sql = each_sql.replace("${%s}" % each_var, variable_dict[each_var])

                sql[sql_id] = each_sql

                if "CREATE DATABASE" in each_sql.upper():
                    # last word is db by default
                    db_name = each_sql.rstrip(";").strip().split(" ")[-1]

                    self.db.add(db_name)

                if "CREATE EXTERNAL RESOURCE " in each_sql.upper():

                    try:
                        self.resource.add(re.findall(r"CREATE EXTERNAL RESOURCE ([a-zA-Z0-9_-]+)", each_sql)[0])
                    except Exception as e:
                        log.info("no resource of CREATE EXTERNAL RESOURCE, %s" % e)

                    try:
                        self.resource.add(re.findall(r"create external resource ([a-zA-Z0-9_-]+)", each_sql)[0])
                    except Exception as e:
                        log.info("no resource of create external resource, %s" % e)

                    try:
                        self.resource.add(re.findall(r"CREATE EXTERNAL RESOURCE \"([a-zA-Z0-9_-]+)\"", each_sql)[0])
                    except Exception as e:
                        log.info("no resource of CREATE EXTERNAL RESOURCE \"\", %s" % e)

                    try:
                        self.resource.add(re.findall(r"create external resource \"([a-zA-Z0-9_-]+)\"", each_sql)[0])
                    except Exception as e:
                        log.info("no resource of create external resource \"\", %s" % e)

            # if no db is confirmed, init one
            default_db = None
            if len(self.db) == 0:
                db_name = "test_db_%s" % uuid.uuid4().hex
                default_db = db_name
                self.db.add(db_name)
                self.init_cmd.append("CREATE DATABASE %s;" % db_name)
                self.init_cmd.append("USE %s;" % db_name)

            # format sql with ctx.
            for sql_id, each_sql in enumerate(sql):
                res = each_sql.format(sr_lib = ctx.sr_lib_obj, default_db = default_db)
                sql[sql_id] = res

        def __lt__(self, other):
            """less than"""
            return self.file < other.file

        def __str__(self):
            """str"""
            tools.assert_equal(len(self.sql), len(self.result), "sql and result nums not match")
            case_dict = OrderedDict(zip(self.sql, self.result))

            case_id = 1
            case_dict_str = ""
            for sql, result in case_dict.items():
                case_dict_str += "ID: %s\nsql: %s\nresult: %s\n" % (case_id, sql, result)
                case_id += 1

            return """---- CASE INFO ----
[name]: {0}
[db]: {1}
[file]: {2}
[SQL]:
{3}
""".format(
                self.name, self.db, self.file, case_dict_str
            )

    def __init__(self, case_dir=None, record_mode=False, file_regex=None, case_regex=None):
        """init"""
        super().__init__()
        self.sr_lib_obj = sr_sql_lib.StarrocksSQLApiLib()

        # case_dir = sql dir by default
        self.case_dir = os.path.join(self.sr_lib_obj.root_path, CASE_DIR) if case_dir is None else case_dir

        self.t: List[str] = []
        self.r: List[str] = []
        self.e: List[str] = []
        self.case_list: List[ChooseCase.CaseTR] = []

        self.list_t_r_files(file_regex)
        self.get_cases(record_mode, case_regex)

        # sort
        self.case_list.sort()

    def list_t_r_files(self, regex):
        """list test files"""
        file_regex = re.compile(r"%s" % regex) if regex is not None else None

        if not os.path.exists(self.case_dir):
            print("** ERROR: Case path not exist: %s! **" % self.case_dir)
            sys.exit(1)

        # path is single file
        if os.path.isfile(self.case_dir):
            abs_file_path = os.path.abspath(self.case_dir)
            if "/T/" in abs_file_path:
                self.t.append(abs_file_path)
            elif "/R/" in abs_file_path:
                self.r.append(abs_file_path)
            else:
                log.error("unknown sql t/r file: %s" % abs_file_path)

            return

        # path is dir
        for (dir_path, _, file_names) in os.walk(self.case_dir):
            if len(file_names) <= 0:
                continue

            if "/T" in dir_path:
                for file in file_names:
                    if file_regex is not None and not file_regex.search(file):
                        # assign filename regex and not match
                        pass
                    else:
                        self.t.append(os.path.join(dir_path, file))
            elif "/R" in dir_path:
                for file in file_names:
                    if file_regex is not None and not file_regex.search(file):
                        # assign filename regex and not match
                        pass
                    else:
                        self.r.append(os.path.join(dir_path, file))

    def get_cases(self, record_mode, case_regex):
        """get cases"""
        file_list = self.t if record_mode else self.r

        for file in file_list:
            base_file = os.path.basename(file)
            if base_file in skip.skip_files:
                print('skip file {} because it is in skip_files'.format(file))
                continue

            self.read_t_r_file(file, case_regex)

        self.case_list = list(filter(lambda x: x.name.strip() != "", self.case_list))

    def read_t_r_file(self, file, case_regex):
        """read t r file and get case & result"""
        with open(file, "r") as f:
            f_lines = f.readlines()

        file = os.path.abspath(file)[len(os.path.abspath(self.sr_lib_obj.root_path)):].lstrip("/")

        tools.assert_greater(len(f_lines), 0, "case file lines must not be empty: %s" % file)

        attr = os.environ.get("attr").split(",") if os.environ.get("attr") != '' else []

        line_id = 0
        name = ""
        info = ""
        tags = []
        tmp_sql = []
        tmp_res = []

        while line_id < len(f_lines):
            line_content = f_lines[line_id].rstrip("\n")

            if line_content == "" or (line_content.startswith("--") and not line_content.startswith(NAME_FLAG)):
                # invalid line
                line_id += 1
                continue

            # line of case info: name/attrs...
            if line_content.startswith(NAME_FLAG):
                # save previous case
                if len(tmp_sql) > 0:
                    if case_regex is not None and not re.compile(case_regex).search(name):
                        # case name don't match regex
                        pass
                    elif attr and any(each_attr not in tags for each_attr in attr):
                        # case attrs don't match attr filter
                        pass
                    elif not attr and "sequential" in tags:
                        # no attr is confirmed, skip sequential cases in default
                        pass
                    else:
                        self.case_list.append(
                            ChooseCase.CaseTR(self, name, file, copy.deepcopy(tmp_sql), copy.deepcopy(tmp_res), info)
                        )

                info = line_content
                # case name
                name = re.compile("name: ([a-zA-Z0-9_-]+)").findall(line_content)[0]
                # case attrs
                tags = re.compile("@([a-zA-Z0-9_-]+)").findall(line_content)

                tmp_sql.clear()
                tmp_res.clear()

                line_id += 1
                continue

            elif line_content.startswith(RESULT_FLAG) or line_content.startswith(RESULT_END_FLAT):
                # 1st line of case can't be result
                tools.assert_true(False, "case file illegal: file: %s, line: %s" % (file, line_id))
            else:
                # 1st line of command, SQL/SHELL/FUNCTION
                this_line_res = []
                this_line_command = [line_content]
                line_id += 1

                if line_content.startswith(UNCHECK_FLAG):
                    line_content = line_content[len(UNCHECK_FLAG):]

                if line_content.startswith(SHELL_FLAG) or line_content.startswith(FUNCTION_FLAG):
                    # shell/function only support 1 line
                    pass
                else:
                    # SQL lines
                    if line_content.endswith(";"):
                        # SQL end with ;, also 1 line
                        pass
                    else:
                        # SQL support lines, read the SQL lines
                        while line_id < len(f_lines):
                            line_content = f_lines[line_id].rstrip("\n")
                            line_id += 1
                            if not line_content.startswith("--"):
                                this_line_command.append(line_content)
                                if line_content.endswith(";"):
                                    # not the end of SQL
                                    break

                # SQL result
                if line_id < len(f_lines) and f_lines[line_id].startswith(RESULT_FLAG):
                    while line_id < len(f_lines):
                        line_content = f_lines[line_id].rstrip("\n")
                        this_line_res.append(line_content)
                        line_id += 1

                        if line_content.startswith(RESULT_END_FLAT):
                            break

                tmp_sql.append("\n".join(this_line_command))
                tmp_res.append("\n".join(this_line_res[1:-1] if len(this_line_res) > 0 else []))

        if len(tmp_sql) > 0:
            if case_regex is not None and not re.compile(case_regex).search(name):
                # case name don't match regex
                pass
            elif attr and any(each_attr not in tags for each_attr in attr):
                # case attrs don't match attr filter
                pass
            elif not attr and "sequential" in tags:
                # no attr is confirmed, skip sequential cases in default
                pass
            else:
                self.case_list.append(
                    ChooseCase.CaseTR(self, name, file, copy.deepcopy(tmp_sql), copy.deepcopy(tmp_res), info)
                )


def choose_cases(record_mode=False):
    # config
    confirm_case_dir = os.environ.get("sql_dir")
    confirm_case_dir = None if confirm_case_dir == "None" else confirm_case_dir

    filename_regex = os.environ.get("file_filter")
    case_name_regex = os.environ.get("case_filter")

    run_info = """
[DIR]: %s
[Mode]: %s
[file regex]: %s
[case regex]: %s
[attr]: %s
    """ % (confirm_case_dir, "RECORD" if record_mode else "VALIDATE", filename_regex, case_name_regex,
           os.environ.get("attr"))
    print(run_info)
    log.info(run_info)
    cases = ChooseCase(confirm_case_dir, record_mode, filename_regex, case_name_regex)

    # check db
    check_db_unique(cases.case_list)

    # log info: case list
    print("case num: %s" % len(cases.case_list))
    log.info("case num: %s" % len(cases.case_list))
    for case in cases.case_list:
        log.info("%s:%s" % (case.file, case.name))

    return cases


def check_db_unique(case_list: List[ChooseCase.CaseTR]):
    """ check db unique in case list """
    db_and_case_dict = {}

    # get info dict, key: db value: [..case_names]
    for case in case_list:
        for each_db in case.db:
            db_and_case_dict.setdefault(each_db, []).append(case.name)

    error_info_dict = {db: cases for db, cases in db_and_case_dict.items() if len(cases) > 1}

    tools.assert_true(len(error_info_dict) <= 0, "Duplicate DBs: \n%s" % json.dumps(error_info_dict, indent=2))
