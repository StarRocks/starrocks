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
from lib import *


CASE_DIR = "sql"
LOG_FILTERED_WARN = "You can use `--log_filtered` to show the details..."


class ChooseCase(object):
    class CaseTR(object):
        def __init__(self, ctx, name, file, sql, result, info):
            """init"""
            super().__init__()
            self.ctx = ctx
            self.info = info
            self.name = name
            self.file = file
            self.sql: List = sql
            self.ori_sql: List = copy.deepcopy(sql)
            self.result: List = result

            # # get db from lines
            # self.db = set()
            # self.resource = set()

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
[file]: {1}
[SQL]:
{2}
""".format(
                self.name, self.file, case_dict_str
            )

    def __init__(self, case_dir=None, record_mode=False, file_regex=None, case_regex=None):
        """init"""
        super().__init__()
        self.sr_lib_obj = sr_sql_lib.StarrocksSQLApiLib()

        # case_dir = sql dir by default
        self.case_dir = os.path.join(sr_sql_lib.root_path, CASE_DIR) if case_dir is None else case_dir

        self.t: List[str] = []
        self.r: List[str] = []
        self.e: List[str] = []
        self.case_list: List[ChooseCase.CaseTR] = []

        self.list_t_r_files(file_regex)
        self.get_cases(record_mode, case_regex)
        self.filter_cases_by_component_status()
        self.filter_cases_by_data_status()

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
        for dir_path, _, file_names in os.walk(self.case_dir):
            if len(file_names) <= 0:
                continue

            if "/T" in dir_path:
                for file in file_names:
                    if file.startswith("."):
                        continue
                    if file_regex is not None and not file_regex.search(file):
                        continue
                    self.t.append(os.path.join(dir_path, file))
            elif "/R" in dir_path:
                for file in file_names:
                    if file.startswith("."):
                        continue
                    if file_regex is not None and not file_regex.search(file):
                        continue
                    self.r.append(os.path.join(dir_path, file))

    def get_cases(self, record_mode, case_regex):
        """get cases"""
        file_list = self.t if record_mode else self.r

        for file in file_list:
            base_file = os.path.basename(file)
            if base_file in skip.skip_files:
                sr_sql_lib.self_print(f'skip file {file} because it is in skip_files', color=ColorEnum.YELLOW)
                continue

            self.read_t_r_file(file, case_regex)

        self.case_list = list(filter(lambda x: x.name.strip() != "", self.case_list))

    def filter_cases_by_component_status(self):
        """ filter cases by component status """
        new_case_list = []

        filtered_cases_dict = {}

        for each_case in self.case_list:
            _case_sqls = []
            for each_stat in each_case.sql:
                if isinstance(each_stat, str):
                    _case_sqls.append(each_stat)
                elif isinstance(each_stat, dict) and each_stat.get("type", "") == LOOP_FLAG:
                    tools.assert_in("stat", each_stat, "LOOP STATEMENT FORMAT ERROR!")
                    _case_sqls.extend(each_stat["stat"])
                elif isinstance(each_stat, dict) and each_stat.get("type", "") == CONCURRENCY_FLAG:
                    tools.assert_in("thread", each_stat, "CONCURRENCY THREAD FORMAT ERROR!")
                    for each_thread in each_stat["thread"]:
                        _case_sqls.extend(each_thread["cmd"])
                else:
                    tools.ok_(False, "Init data error!")

            is_pass = True

            # check trino/spark/hive flag
            for each_client_flag in [TRINO_FLAG, SPARK_FLAG, HIVE_FLAG]:
                if any(_case_sql.lstrip().startswith(each_client_flag) for _case_sql in _case_sqls):
                    # check client status
                    client_name = each_client_flag.split(":")[0].lower() + "-client"
                    if client_name not in self.sr_lib_obj.component_status:
                        sr_sql_lib.self_print(f"[Config ERROR]: {client_name} config not found!")
                        filtered_cases_dict.setdefault(client_name, []).append(each_case.name)
                        is_pass = False
                        break

                    if not self.sr_lib_obj.component_status[client_name]["status"]:
                        filtered_cases_dict.setdefault(client_name, []).append(each_case.name)
                        is_pass = False
                        break

            if not is_pass:
                # client check failed, no need to check component info
                continue

            # check ${} contains component info
            _case_sqls = " ".join(_case_sqls)
            _vars = re.findall(r"\${([a-zA-Z0-9._-]+)}", _case_sqls)
            for _var in _vars:

                if not is_pass:
                    break

                if _var not in self.sr_lib_obj.__dict__:
                    continue

                for each_component_name, each_component_info in self.sr_lib_obj.component_status.items():
                    if _var in each_component_info["keys"] and not each_component_info["status"]:
                        filtered_cases_dict.setdefault(each_component_name, []).append(each_case.name)
                        is_pass = False
                        break

            if is_pass:
                new_case_list.append(each_case)

        if filtered_cases_dict:
            if os.environ.get("log_filtered") == "True":
                sr_sql_lib.self_print(f"\n{'-' * 60}\n[Component filter]\n{'-' * 60}", color=ColorEnum.BLUE,
                                      logout=True, bold=True)
                for k, cases in filtered_cases_dict.items():
                    sr_sql_lib.self_print(f"▶ {k.upper()}", color=ColorEnum.BLUE, logout=True, bold=True)
                    sr_sql_lib.self_print(f"    ▶ %s" % '\n    ▶ '.join(cases), logout=True)
                    sr_sql_lib.self_print('-' * 60, color=ColorEnum.BLUE, logout=True, bold=True)
            else:
                filtered_count = sum([len(x) for x in filtered_cases_dict.values()])
                sr_sql_lib.self_print(f"\n{'-' * 60}\n[Component filter]: {filtered_count}\n{LOG_FILTERED_WARN}\n{'-' * 60}",
                                      color=ColorEnum.BLUE, logout=True, bold=True)

        self.case_list = new_case_list

    def filter_cases_by_data_status(self):
        """ filter cases by data status """
        new_case_list = []

        filtered_cases_dict = {}

        for each_case in self.case_list:
            _case_sqls = []
            for each_stat in each_case.sql:
                if isinstance(each_stat, str):
                    _case_sqls.append(each_stat)
                elif isinstance(each_stat, dict) and each_stat.get("type", "") == LOOP_FLAG:
                    tools.assert_in("stat", each_stat, "LOOP STATEMENT FORMAT ERROR!")
                    _case_sqls.extend(each_stat["stat"])
                elif isinstance(each_stat, dict) and each_stat.get("type", "") == CONCURRENCY_FLAG:
                    tools.assert_in("thread", each_stat, "CONCURRENCY THREAD FORMAT ERROR!")
                    for each_thread in each_stat["thread"]:
                        _case_sqls.extend(each_thread["cmd"])
                else:
                    tools.ok_(False, "Init data error!")

            is_pass = True
            # check trino/spark/hive flag
            function_stats = list(filter(lambda x: x.lstrip().startswith(FUNCTION_FLAG) and "prepare_data(" in x, _case_sqls))

            for func_stat in function_stats:
                if not is_pass:
                    break

                data_source_names = re.findall(r"prepare_data\(['|\"]([a-zA-Z_-]+)['|\"]", func_stat)
                for data_source in data_source_names:

                    if self.sr_lib_obj.data_status.get(data_source, False) is False:
                        filtered_cases_dict.setdefault(data_source, []).append(each_case.name)
                        is_pass = False
                        break

            if is_pass:
                new_case_list.append(each_case)

        if filtered_cases_dict:
            if os.environ.get("log_filtered") == "True":
                sr_sql_lib.self_print(f"\n{'-' * 60}\n[Data filter]\n{'-' * 60}", color=ColorEnum.BLUE, logout=True, bold=True)
                for k in list(sorted(filtered_cases_dict.keys())):
                    cases = filtered_cases_dict[k]
                    sr_sql_lib.self_print(f"▶ {k.upper()}", color=ColorEnum.BLUE, logout=True, bold=True)
                    sr_sql_lib.self_print(f"    ▶ %s" % '\n    ▶ '.join(cases), logout=True)
                    sr_sql_lib.self_print('-' * 60, color=ColorEnum.BLUE, logout=True, bold=True)
            else:
                filtered_count = sum([len(x) for x in filtered_cases_dict.values()])
                sr_sql_lib.self_print(f"\n{'-' * 60}\n[Data filter]: {filtered_count}\n{LOG_FILTERED_WARN}\n{'-' * 60}",
                                      color=ColorEnum.BLUE, logout=True, bold=True)

        self.case_list = new_case_list

    def read_t_r_file(self, file, case_regex):
        """read t r file and get case & result"""

        def __read_single_stat_and_result(_line_content, _line_id, _stat_list, _res_list, _is_in_loop=False,
                                          _loop_stat_list: list = None):
            # init _loop_stat_list
            _loop_stat_list = [] if _loop_stat_list is None else _loop_stat_list
            # Multi lines SQL, lstrip the same ' '
            first_line_lstrip = len(f_lines[_line_id]) - len(f_lines[_line_id].lstrip())
            _line_content = _line_content.lstrip()

            this_line_res = []
            this_line_command = [_line_content]
            _line_id += 1

            if _line_content.strip() == "":
                return _line_id

            if _line_content.startswith(UNCHECK_FLAG):
                _line_content = _line_content[len(UNCHECK_FLAG):]

            if _line_content.startswith(SHELL_FLAG) or _line_content.startswith(FUNCTION_FLAG):
                # shell/function only support 1 line
                pass
            elif _is_in_loop and _line_content.startswith(CHECK_FLAG):
                # check statement in loop
                pass
            else:
                # SQL lines
                if _line_content.endswith(";"):
                    # SQL end with ;, also 1 line
                    pass
                else:
                    # SQL support lines, read the SQL lines
                    while _line_id < len(f_lines):
                        _line_content = re.sub(r"^ {%s}" % first_line_lstrip, '', f_lines[_line_id].rstrip("\n"))
                        _line_id += 1
                        if not _line_content.startswith("--"):
                            this_line_command.append(_line_content)
                            if _line_content.endswith(";"):
                                # not the end of SQL
                                break

            # SQL result
            if _line_id < len(f_lines) and f_lines[_line_id].lstrip().startswith(RESULT_FLAG):
                while _line_id < len(f_lines):
                    _line_content = re.sub(r"^ {%s}" % first_line_lstrip, '', f_lines[_line_id].rstrip("\n"))
                    this_line_res.append(_line_content)
                    _line_id += 1

                    if _line_content.startswith(RESULT_END_FLAT):
                        break

            if _is_in_loop:
                # loop list
                _loop_stat_list.append("\n".join(this_line_command))
            else:
                _stat_list.append("\n".join(this_line_command))
                _res_list.append("\n".join(this_line_res[1:-1] if len(this_line_res) > 0 else []))

            return _line_id

        with open(file, "r") as f:
            f_lines = f.readlines()

        file = os.path.abspath(file)[len(os.path.abspath(sr_sql_lib.root_path)):].lstrip("/")

        tools.assert_greater(len(f_lines), 0, "case file lines must not be empty: %s" % file)

        attr = os.environ.get("attr").split(",") if os.environ.get("attr") != "" else []
        no_attr = [at.lstrip("!") for at in filter(lambda at: at.startswith("!"), attr)]
        attr = list(filter(lambda at: not at.startswith("!"), attr))

        line_id = 0
        name = ""
        info = ""
        tags = []
        tmp_sql = []
        tmp_res = []
        tmp_loop_stat = []
        in_loop_flag = False

        tmp_con_stat = []

        while line_id < len(f_lines):
            line_content = f_lines[line_id].rstrip("\n")

            if line_content == "" or (line_content.startswith("--") and not line_content.startswith(NAME_FLAG)):
                # invalid line
                line_id += 1
                continue

            # line of case info: name/attrs...
            if line_content.startswith(NAME_FLAG):

                # check loop finished
                tools.ok_(not in_loop_flag, "LOOP FORMAT ERROR!")

                # save previous case
                if len(tmp_sql) > 0:
                    if case_regex is not None and not re.compile(case_regex).search(name):
                        # case name don't match regex
                        pass
                    elif (attr and any(each_attr not in tags for each_attr in attr)) \
                            or (no_attr and any(each_attr in tags for each_attr in no_attr)):
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
                tools.ok_(False, "case file illegal: file: %s, line: %s" % (file, line_id))
            elif line_content.startswith(LOOP_FLAG):
                l_loop_line = line_id

                # loop -- end loop
                if re.compile(f'{LOOP_FLAG}(\\s)*{{(\\s)*').fullmatch(line_content):
                    in_loop_flag = True
                else:
                    tools.ok_(False, "Case file loop struct illegal: file: %s, line: %s" % (file, line_id))

                line_id += 1
                tmp_loop_stat.clear()
                tmp_loop_prop = {}

                # fetch property json
                if f_lines[line_id].strip().startswith(PROPERTY_FLAG):
                    prop_str = f_lines[line_id].strip().lstrip(PROPERTY_FLAG)
                    try:
                        tmp_loop_prop = json.loads(prop_str)
                    except Exception:
                        raise AssertionError("Loop property's format must be json: %s" % prop_str)
                    tmp_loop_prop.setdefault("interval", 10)
                    tmp_loop_prop.setdefault("timeout", 60)
                    tmp_loop_prop.setdefault("desc", "LOOP STATEMENT")

                    line_id += 1

                while (line_id < len(f_lines)
                       and not re.compile(f'}}(\\s)*{END_LOOP_FLAG}').fullmatch(f_lines[line_id].strip())):
                    # read loop stats, unnecessary to record result
                    line_content = f_lines[line_id].strip()
                    line_id = __read_single_stat_and_result(line_content, line_id, tmp_sql, tmp_res, in_loop_flag,
                                                            tmp_loop_stat)
                tools.assert_less(line_id, len(f_lines), "LOOP FORMAT ERROR!")

                # reach the end loop line
                in_loop_flag = False
                r_loop_line = line_id

                tools.assert_greater(len(tmp_loop_stat), 0, "LOOP FORMAT ERROR(EMPTY)!")
                tmp_sql.append({
                    "type": LOOP_FLAG,
                    "stat": tmp_loop_stat,
                    "prop": tmp_loop_prop,
                    "ori": f_lines[l_loop_line: r_loop_line + 1]
                })
                tmp_res.append(None)
                line_id += 1

            elif line_content.startswith(CONCURRENCY_FLAG):
                # thread info list
                concurrency_t_list = []

                l_concurrency_line = line_id

                # concurrency -- end concurrency
                if not re.compile(f'{CONCURRENCY_FLAG}(\\s)*{{(\\s)*').fullmatch(line_content):
                    tools.ok_(False, "Concurrency struct illegal: file: %s, line: %s" % (file, line_content))

                line_id += 1
                tmp_con_stat.clear()
                tmp_con_prop = {}
                tmp_name_list = []

                # read concurrency struct
                t_info_regex = r"^-- [0-9a-zA-Z ]+(\([0-9]+\))?:$"
                while line_id < len(f_lines) and END_CONCURRENCY_FLAG not in f_lines[line_id]:

                    # skip null line
                    if f_lines[line_id].strip() == "":
                        line_id += 1
                        continue

                    _t_info_line = f_lines[line_id].rstrip()
                    _t_line = _t_info_line.lstrip()
                    tools.assert_regexp_matches(_t_line.lstrip(), t_info_regex, f"Missing thread info : {_t_line}")

                    # get thread name & thread count
                    _t_name, _, _t_count = re.findall(r"-- ([0-9a-zA-Z_\- ]+)(\(([0-9]+)\))?:", _t_line)[0]
                    _t_count = 1 if not _t_count else int(_t_count)

                    line_id += 1
                    # thread info
                    _thread_sql_list, _thread_res_list, _thread_ori_sql_list = [], [], []

                    while line_id < len(f_lines):
                        this_line = f_lines[line_id].rstrip().lstrip()
                        _old_sql_size = len(_thread_sql_list)

                        # read each cmd & res
                        next_line_id = __read_single_stat_and_result(this_line, line_id, _thread_sql_list, _thread_res_list)
                        if len(_thread_sql_list) > _old_sql_size:
                            # new record was recorded
                            _ori_lines = f_lines[line_id: next_line_id]
                            _thread_ori_sql_list.append("\n".join([_line.rstrip() for _line in _ori_lines]))
                        line_id = next_line_id

                        _next_line = f_lines[line_id].rstrip().lstrip()
                        if (re.match(r"^-- [0-9a-zA-Z ]+(\([0-9]+\))?:$", _next_line)
                                or END_CONCURRENCY_FLAG in _next_line):
                            break

                    # add previous thread info
                    concurrency_t_list.append({
                        "name": _t_name,
                        "count": _t_count,
                        "info": _t_info_line,
                        "ori": _thread_ori_sql_list,
                        "cmd": _thread_sql_list,
                        "res": _thread_res_list
                    })
                    tools.assert_not_in(_t_name, tmp_name_list, f"Thread name `{_t_name}` already exist!")
                    tmp_name_list.append(_t_name)

                tools.assert_in(END_CONCURRENCY_FLAG, f_lines[line_id], "`END CONCURRENCY` not found!")
                line_id += 1
                tmp_sql.append({
                    "type": CONCURRENCY_FLAG,
                    "thread": concurrency_t_list
                })
                tmp_res.append({
                    "type": CONCURRENCY_FLAG,
                    "thread": concurrency_t_list
                })

            else:
                # 1st line of command, SQL/SHELL/FUNCTION
                line_id = __read_single_stat_and_result(line_content, line_id, tmp_sql, tmp_res, in_loop_flag)

        tools.ok_(not in_loop_flag, "LOOP FORMAT ERROR!")
        if len(tmp_sql) > 0:
            if case_regex is not None and not re.compile(case_regex).search(name):
                # case name don't match regex
                pass
            elif (attr and any(each_attr not in tags for each_attr in attr)) \
                    or (no_attr and any(each_attr in tags for each_attr in no_attr)):
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

    run_info = f"""{'-' * 60}
[DIR]: {"DEFAULT" if confirm_case_dir is None else confirm_case_dir}
[Mode]: {"RECORD" if record_mode else "VALIDATE"}
[file regex]: {filename_regex}
[case regex]: {case_name_regex}
[attr]: {os.environ.get("attr")}
{'-' * 60}"""
    sr_sql_lib.self_print(run_info, color=ColorEnum.GREEN, bold=False, logout=True)

    cases = ChooseCase(confirm_case_dir, record_mode, filename_regex, case_name_regex)

    # log info: case list
    sr_sql_lib.self_print("case num: %s" % len(cases.case_list))

    for case in cases.case_list:
        log.info("%s:%s" % (case.file, case.name))

    return cases
