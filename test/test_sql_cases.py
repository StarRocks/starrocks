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
test_sql_cases
@Time : 2022/10/28 13:41
@Author : Brook.Ye
"""
import copy
import json
import os
import re
import sys
import threading
import time
import uuid
from typing import List

import pymysql
from nose import tools
from parameterized import parameterized
from cup import log

from lib import sr_sql_lib
from lib import choose_cases
from lib import sql_annotation
from lib.sr_sql_lib import self_print
from lib import *

# model: run case model, True => Record mode
#    - t: run sql and save result into r dir
#    - r: run sql and compare result with r
record_mode = os.environ.get("record_mode", "false") == "true"

case_list = choose_cases.choose_cases(record_mode).case_list

if len(case_list) == 0:
    self_print(f"{'-' * 60}\n** INFO: No case! **", ColorEnum.RED)
    sys.exit(0)


def doc_func(_, num, param):
    """doc func"""
    if param[0][0].file == "":
        return "%-5d: %s" % (num, param[0][0].name)
    else:
        return "%-5d: %s" % (num, param[0][0].file + ":" + param[0][0].name)


def name_func(testcase_func, param_num, param):
    """name func"""
    return parameterized.to_safe_name(param.args[0].name)


class TestSQLCases(sr_sql_lib.StarrocksSQLApiLib):
    """
    SQL Framework Run Test
    """

    _multiprocess_can_split_ = True

    def __init__(self, *args, **kwargs):
        """
        init
        """
        super(TestSQLCases, self).__init__(*args, **kwargs)
        self.case_info: choose_cases.ChooseCase.CaseTR
        self._check_db_unique()

    def setUp(self, *args, **kwargs):
        """set up"""
        super().setUp()
        self.connect_starrocks()
        self.create_starrocks_conn_pool()
        self._init_global_configs()

    def _init_global_configs(self):
        """
        Configs that are not ready for production but it can be used for testing.
        """
        default_configs = [
            "'mv_refresh_fail_on_filter_data' = 'true'",
            "'enable_mv_refresh_query_rewrite' = 'true'",
            # enlarge task run concurrency to speed up mv's refresh and find more potential bugs
            "'task_runs_concurrency' = '16'",
        ]

        for config in default_configs:
            sql = "ADMIN SET FRONTEND CONFIG (%s)" % config
            self.execute_sql(sql)

    @sql_annotation.ignore_timeout()
    def tearDown(self):
        """tear down"""
        super().tearDown()

        log.info("[TearDown begin]: %s" % self.case_info.name)

        for each_db in self.db:
            self.drop_database(each_db)

        for each_resource in self.resource:
            self.drop_resource(each_resource)

        res = None
        if record_mode:
            # save case result into db
            res = self.save_r_into_db(self.case_info.file, self.case_info.name, self.res_log, self.version)

        self.close_starrocks()
        # destroy connection pool
        while self.connection_pool and len(self.connection_pool._idle_cache) > 0:
            log.info(f"Teardown: freeze conn pool, size: {len(self.connection_pool._idle_cache)}")
            conn = self.connection_pool._idle_cache.pop()
            conn.close()

        self.close_trino()
        self.close_spark()
        self.close_hive()
        self.close_starrocks_arrow()

        if record_mode:
            tools.assert_true(res, "Save %s.%s result error" % (self.case_info.file, self.case_info.name))

        self_print(f"{'*' * 20} [FINISH] {self.case_info.name} {'*' * 20}", ColorEnum.GREEN, bold=True)

    def _init_data(self, sql_list: List) -> List:
        self.db = list()
        self.resource = list()

        sql_list = self._replace_uuid_variables(sql_list)

        for sql in sql_list:

            if isinstance(sql, str):
                db_name = self._get_db_name(sql)
                if len(db_name) > 0:
                    self.db.append(db_name)
                resource_name = self._get_resource_name(sql)
                if len(resource_name) > 0:
                    self.resource.append(resource_name)
            elif isinstance(sql, dict) and sql.get("type", "") == LOOP_FLAG:
                tools.assert_in("stat", sql, "LOOP STATEMENT FORMAT ERROR!")

                for each_sql in sql["stat"]:
                    db_name = self._get_db_name(each_sql)
                    if len(db_name) > 0:
                        self.db.append(db_name)
                    resource_name = self._get_resource_name(each_sql)
                    if len(resource_name) > 0:
                        self.resource.append(resource_name)
            elif isinstance(sql, dict) and sql.get("type", "") == CONCURRENCY_FLAG:
                tools.assert_in("thread", sql, "CONCURRENCY THREAD FORMAT ERROR!")

                for each_thread in sql["thread"]:
                    for each_cmd in each_thread["cmd"]:
                        db_name = self._get_db_name(each_cmd)
                        if len(db_name) > 0:
                            self.db.append(db_name)
                        resource_name = self._get_resource_name(each_cmd)
                        if len(resource_name) > 0:
                            self.resource.append(resource_name)
            else:
                tools.ok_(False, "Init data error!")

        self._clear_db_and_resource_if_exists()

        if len(self.db) == 0:
            self._create_and_use_db()

        return sql_list

    def _clear_db_and_resource_if_exists(self):
        for each_db in self.db:
            log.info("init drop db: %s" % each_db)
            self.drop_database(each_db)

        for each_resource in self.resource:
            log.info("init drop resource: %s" % each_resource)
            self.drop_resource(each_resource)

    def _create_and_use_db(self):
        db_name = "test_db_%s" % uuid.uuid4().hex
        self.db.append(db_name)
        self.execute_sql("CREATE DATABASE %s;" % db_name)
        self.execute_sql("USE %s;" % db_name)
        self_print("[SQL]: CREATE DATABASE %s;" % db_name)
        self_print("[SQL]: USE %s;" % db_name)

    def _check_db_unique(self):
        all_db_dict = dict()
        for case in case_list:
            sql_list = self._replace_uuid_variables(case.sql)

            for sql in sql_list:
                if isinstance(sql, str):
                    db_name = self._get_db_name(sql)
                    if len(db_name) > 0:
                        all_db_dict.setdefault(db_name, set()).add(case.name)
                elif isinstance(sql, dict) and sql.get("type", "") == LOOP_FLAG:
                    tools.assert_in("stat", sql, "LOOP STATEMENT FORMAT ERROR!")

                    for each_sql in sql["stat"]:
                        db_name = self._get_db_name(each_sql)
                        if len(db_name) > 0:
                            all_db_dict.setdefault(db_name, set()).add(case.name)
                elif isinstance(sql, dict) and sql.get("type", "") == CONCURRENCY_FLAG:
                    tools.assert_in("thread", sql, "CONCURRENCY THREAD FORMAT ERROR!")

                    for each_thread in sql["thread"]:
                        for each_cmd in each_thread["cmd"]:
                            db_name = self._get_db_name(each_cmd)
                            if len(db_name) > 0:
                                all_db_dict.setdefault(db_name, set()).add(case.name)
                else:
                    tools.ok_(False, "Check db uniqueness error!")

        error_info_dict = {db: list(cases) for db, cases in all_db_dict.items() if len(cases) > 1}
        tools.assert_true(len(error_info_dict) <= 0, "Pre Check Failed, Duplicate DBs: \n%s" % json.dumps(error_info_dict, indent=2))

    @staticmethod
    def _replace_uuid_variables(sql_list: List) -> List:
        ret = list()
        variable_dict = {}

        for sql in sql_list:

            if isinstance(sql, str):
                uuid_vars = re.findall(r"\${(uuid[0-9]*)}", sql)
                for each_uuid in uuid_vars:
                    if each_uuid not in variable_dict:
                        variable_dict[each_uuid] = uuid.uuid4().hex
            elif isinstance(sql, dict) and sql.get("type", "") == LOOP_FLAG:
                tools.assert_in("stat", sql, "LOOP STATEMENT FORMAT ERROR!")

                for each_sql in sql["stat"]:
                    uuid_vars = re.findall(r"\${(uuid[0-9]*)}", each_sql)
                    for each_uuid in uuid_vars:
                        if each_uuid not in variable_dict:
                            variable_dict[each_uuid] = uuid.uuid4().hex
            elif isinstance(sql, dict) and sql.get("type", "") == CONCURRENCY_FLAG:
                tools.assert_in("thread", sql, "CONCURRENCY THREAD FORMAT ERROR!")

                for each_thread in sql["thread"]:
                    for each_cmd in each_thread["cmd"]:
                        uuid_vars = re.findall(r"\${(uuid[0-9]*)}", each_cmd)
                        for each_uuid in uuid_vars:
                            if each_uuid not in variable_dict:
                                variable_dict[each_uuid] = uuid.uuid4().hex

            else:
                tools.ok_(False, "Replace uuid error!")

        for sql in sql_list:
            if isinstance(sql, str):
                for each_var in variable_dict:
                    sql = sql.replace("${%s}" % each_var, variable_dict[each_var])
                ret.append(sql)
            elif isinstance(sql, dict) and sql.get("type", "") == LOOP_FLAG:
                _sql = copy.deepcopy(sql)
                tmp_stat = []
                for each_sql in _sql["stat"]:
                    for each_var in variable_dict:
                        each_sql = each_sql.replace("${%s}" % each_var, variable_dict[each_var])
                    tmp_stat.append(each_sql)
                _sql["stat"] = tmp_stat
                ret.append(_sql)
            elif isinstance(sql, dict) and sql.get("type", "") == CONCURRENCY_FLAG:
                _sql = copy.deepcopy(sql)
                tools.assert_in("thread", sql, "CONCURRENCY THREAD FORMAT ERROR!")

                for each_thread in _sql["thread"]:
                    tmp_cmd = []
                    for each_cmd in each_thread["cmd"]:
                        for each_var in variable_dict:
                            each_cmd = each_cmd.replace("${%s}" % each_var, variable_dict[each_var])
                        tmp_cmd.append(each_cmd)
                    each_thread["cmd"] = tmp_cmd

                ret.append(_sql)

        return ret

    @staticmethod
    def _get_db_name(sql: str) -> str:
        db_name = ""
        if "CREATE DATABASE" in sql.upper():
            db_name = sql.rstrip(";").strip().split(" ")[-1]
        return db_name

    @staticmethod
    def _get_resource_name(sql: str) -> str:
        matches = list()
        if "CREATE EXTERNAL RESOURCE" in sql.upper():
            matches = re.findall(r'CREATE EXTERNAL RESOURCE \"?([a-zA-Z0-9_-]+)\"?', sql, flags=re.IGNORECASE)
        return matches[0] if len(matches) > 0 else ""

    # -------------------------------------------
    #         [CASE]
    # -------------------------------------------
    @parameterized.expand([[case_info] for case_info in case_list], doc_func=doc_func, name_func=name_func)
    @sql_annotation.timeout()
    def test_sql_basic(self, case_info: choose_cases.ChooseCase.CaseTR):
        """
        sql tester
        Args:
            case_info:
                name:       case name
                file:        case info {table: duplicate, file: A.csv ...}
                sql:        run sql
                result:     result
        """
        # -------------------------------------------
        #               [CASE EXECUTE]
        # -------------------------------------------

        # replace all db_name with each run
        self.case_info = case_info
        self_print("-" * 60, ColorEnum.GREEN, bold=True)
        self_print(f"[case name]: {case_info.name}", ColorEnum.GREEN, bold=True)
        self_print(f"[case file]: {case_info.file}", ColorEnum.GREEN, bold=True)
        self_print("-" * 60, ColorEnum.GREEN, bold=True)

        sql_list = self._init_data(case_info.sql)

        self_print(f"\t → case db: {self.db}")
        self_print(f"\t → case resource: {self.resource}")

        log.info(
            """
*********************************************
Start to run: %s
*********************************************"""
            % case_info.name
        )
        # record mode, init info
        if record_mode:
            self.res_log.append(case_info.info)

        for sql_id, sql in enumerate(sql_list):
            uncheck = False
            ori_sql = sql
            var = None

            if isinstance(sql, str):

                if record_mode:
                    self.res_log.append(case_info.ori_sql[sql_id])

                # uncheck flag, owns the highest priority
                if sql.startswith(sr_sql_lib.UNCHECK_FLAG):
                    uncheck = True
                    sql = sql[len(sr_sql_lib.UNCHECK_FLAG):]

                actual_res, actual_res_log, var, order = self.execute_single_statement(sql, sql_id, record_mode)

                if not record_mode and not uncheck:
                    # check mode only work in validating mode
                    # pretreatment expect res
                    expect_res = case_info.result[sql_id]
                    expect_res_for_log = expect_res if len(expect_res) < 1000 else expect_res[:1000] + "..."

                    log.info(f"""[{sql_id}.result]: 
    - [exp]: {expect_res_for_log}
    - [act]: {actual_res}""")

                    # -------------------------------------------
                    #               [CHECKER]
                    # -------------------------------------------
                    self.check(sql_id, sql, expect_res, actual_res, order, ori_sql)

            elif isinstance(sql, dict) and sql["type"] == sr_sql_lib.LOOP_FLAG:
                # loop statement

                # loop properties
                loop_timeout = sql["prop"]["timeout"]
                loop_interval = sql["prop"]["interval"]
                loop_desc = sql["prop"]["desc"]

                self_print(f"\n[LOOP] Start: {loop_desc}...", color=ColorEnum.BLUE, logout=True, bold=True)
                if record_mode:
                    self.res_log.append("".join(sql["ori"]))

                loop_check_res = self.execute_loop_statement(sql["stat"], sql_id, loop_timeout, loop_interval)

                self_print(f"[LOOP] Finish!\n", color=ColorEnum.BLUE, logout=True, bold=True)
                tools.ok_(loop_check_res, f"Loop checker fail: {''.join(sql['ori'])}!")

            elif isinstance(sql, dict) and sql["type"] == sr_sql_lib.CONCURRENCY_FLAG:
                # concurrency statement
                self_print(f"[CONCURRENCY] Start...", color=ColorEnum.CYAN, logout=True)
                if record_mode:
                    self.res_log.append("\n" + CONCURRENCY_FLAG + " {")

                t_info_list: List[dict] = sql["thread"]
                thread_list = []

                # get now db
                _outer_db = self.get_now_db()

                # thread group
                _t_conn_list = []
                for _t_info_id, _thread in enumerate(t_info_list):

                    # _thread prop: name, count, info, ori, cmd, res
                    _t_count = _thread["count"]
                    _t_name = _thread["name"]
                    _t_ori_cmd = _thread["ori"]
                    _t_cmd = _thread["cmd"]
                    _t_res = _thread["res"]

                    # thread exec, set count in (*)
                    for _t_exec_id in range(_t_count):
                        this_t_id = f'{_t_name}-{_t_info_id}-{_t_exec_id}'

                        # init a conn for thread
                        this_conn = self.connection_pool.connection()
                        _t_conn_list.append([this_conn, this_t_id])

                        t = threading.Thread(name=f"Thread-{this_t_id}",
                                             target=self.execute_thread,
                                             args=(this_t_id, _t_cmd, _t_res, _t_ori_cmd, record_mode, _outer_db, this_conn))
                        thread_list.append(t)

                threading.excepthook = self.custom_except_hook

                for thread in thread_list:
                    thread.start()

                for thread in thread_list:
                    thread.join()

                # release conn
                for _conn, _id in _t_conn_list:
                    self_print(f"[{_id}] Close connection...", color=ColorEnum.CYAN, logout=True)
                    _conn.close()

                if len(self.thread_res) != 0:
                    err_t_name = "\n\t- ".join(self.thread_res.keys())
                    err_msg = f"[CONCURRENCY] FAIL:\n\t- {err_t_name}"
                    self_print(err_msg, color=ColorEnum.RED, bold=True, logout=True)

                    # console detail
                    for _, t_err in self.thread_res.items():
                        self_print(t_err, bold=True, logout=True)

                    tools.ok_(False, err_msg)

                # record mode
                if record_mode:
                    for _t_info_id, _thread in enumerate(t_info_list):
                        _t_name = _thread["name"]
                        _t_count = _thread["count"]
                        _t_uid = f'{_t_name}-{_t_info_id}'

                        _t_info_line = _thread["info"]
                        self.res_log.append(_t_info_line)

                        # check thread result info
                        tools.assert_in(_t_uid, self.thread_res_log, f"Thread log of {_t_uid} is not found!")
                        tools.eq_(len(self.thread_res_log[_t_uid]), _t_count, f"Thread log size: {len(self.thread_res_log[_t_uid])} error, maybe you used the same thread name?")

                        s_thread_log = self.thread_res_log[_t_uid][0]
                        for exec_res_log in self.thread_res_log[_t_uid]:
                            if exec_res_log != s_thread_log:
                                self_print("Thread result of exec not equal: \n - %s\n - %s" % (exec_res_log, s_thread_log), color=ColorEnum.RED, logout=True, bold=True)

                        self.res_log.extend(s_thread_log)
                        self.res_log.append("")

                self_print(f"[CONCURRENCY] SUCCESS!", color=ColorEnum.CYAN, logout=True)
                if record_mode:
                    self.res_log.append("} " + END_CONCURRENCY_FLAG + "\n")

