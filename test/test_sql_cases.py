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
import json
import os
import re
import sys
import time
import uuid
from typing import List

from nose import tools
from parameterized import parameterized
from cup import log

from lib import sr_sql_lib
from lib import choose_cases
from lib import sql_annotation
from lib.sr_sql_lib import self_print

# model: run case model, True => Record mode
#    - t: run sql and save result into r dir
#    - r: run sql and compare result with r
record_mode = os.environ.get("record_mode", "false") == "true"

case_list = choose_cases.choose_cases(record_mode).case_list

if len(case_list) == 0:
    print("** INFO: No case! **")
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
        self.db = list()
        self.resource = list()
        self._check_db_unique()

    def setUp(self, *args, **kwargs):
        """set up"""
        super().setUp()
        self.connect_starrocks()

    def tearDown(self):
        """tear down"""
        super().tearDown()

        for each_db in self.db:
            self.drop_database(each_db)

        for each_resource in self.resource:
            self.drop_resource(each_resource)

        res = None
        if record_mode:
            # save case result into db
            res = self.save_r_into_db(self.case_info.file, self.case_info.name, self.res_log, self.version)

        self.close_starrocks()

        if record_mode:
            tools.assert_true(res, "Save %s.%s result error" % (self.case_info.file, self.case_info.name))

    # -------------------------------------------
    #         [CASE]
    # -------------------------------------------
    @parameterized.expand([[case_info] for case_info in case_list], doc_func=doc_func, name_func=name_func)
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
        sql_list = self._init_data(case_info.sql)
        print("[case name]: ", case_info.name)
        print("[case file]: ", case_info.file)
        print("[case db]: ", self.db)
        print("[case resource: ]: ", self.resource)

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
            order = False
            ori_sql = sql
            var = None

            if record_mode:
                self.res_log.append(case_info.ori_sql[sql_id])

            # uncheck flag, owns the highest priority
            if sql.startswith(sr_sql_lib.UNCHECK_FLAG):
                uncheck = True
                sql = sql[len(sr_sql_lib.UNCHECK_FLAG):]

            # execute command in files
            if sql.startswith(sr_sql_lib.SHELL_FLAG):
                sql = sql[len(sr_sql_lib.SHELL_FLAG):]

                # analyse var set
                var, sql = self.analyse_var(sql)
                self_print("[SHELL]: %s" % sql)

                log.info("[%s] SHELL: %s" % (sql_id, sql))
                actual_res = self.execute_shell(sql)

                if record_mode:
                    self.record_shell_res(sql, actual_res)

            elif sql.startswith(sr_sql_lib.FUNCTION_FLAG):
                # function invoke
                sql = sql[len(sr_sql_lib.FUNCTION_FLAG):]

                # analyse var set
                var, sql = self.analyse_var(sql)
                self_print("[FUNCTION]: %s" % sql)

                log.info("[%s] FUNCTION: %s" % (sql_id, sql))
                actual_res = eval("self.%s" % sql)

                if record_mode:
                    self.record_function_res(sql, actual_res)
            else:
                # sql
                log.info("[%s] SQL: %s" % (sql_id, sql))

                # order flag
                if sql.startswith(sr_sql_lib.ORDER_FLAG):
                    order = True
                    sql = sql[len(sr_sql_lib.ORDER_FLAG):]

                # analyse var set
                var, sql = self.analyse_var(sql)

                actual_res = self.execute_sql(sql)
                self_print("[SQL]: %s" % sql)

                if record_mode:
                    self.treatment_record_res(sql, actual_res)

                actual_res = actual_res["result"] if actual_res["status"] else "E: %s" % str(actual_res["msg"])

                # pretreatment actual res
                actual_res, actual_res_log = self.pretreatment_res(actual_res)

            if not record_mode and not uncheck:
                # check mode only work in validating mode
                # pretreatment expect res
                expect_res = case_info.result[sql_id]
                expect_res_for_log = expect_res if len(expect_res) < 1000 else expect_res[:1000] + "..."

                log.info("[%s.result]: \n\t- [exp]: %s\n\t- [act]: %s" % (sql_id, expect_res_for_log, actual_res))

                # -------------------------------------------
                #               [CHECKER]
                # -------------------------------------------
                self.check(sql_id, sql, expect_res, actual_res, order, ori_sql)

            # set variable dynamically
            if var:
                self.__setattr__(var, actual_res)

    def _init_data(self, sql_list: List[str]) -> List[str]:
        self.db = list()
        self.resource = list()

        sql_list = self._replace_uuid_variables(sql_list)

        for sql in sql_list:
            db_name = self._get_db_name(sql)
            if len(db_name) > 0:
                self.db.append(db_name)
            resource_name = self._get_resource_name(sql)
            if len(resource_name) > 0:
                self.resource.append(resource_name)

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
        db_name = "test_db_%s" % uuid.uuid1().hex
        self.db.append(db_name)
        self.execute_sql("CREATE DATABASE %s;" % db_name)
        self.execute_sql("USE %s;" % db_name)
        print("[SQL]: CREATE DATABASE %s;" % db_name)
        print("[SQL]: USE %s;" % db_name)

    def _check_db_unique(self):
        all_db_dict = dict()
        for case in case_list:
            sql_list = self._replace_uuid_variables(case.sql)
            for sql in sql_list:
                db_name = self._get_db_name(sql)
                if len(db_name) > 0:
                    all_db_dict.setdefault(db_name, set()).add(case.name)
        error_info_dict = {db: list(cases) for db, cases in all_db_dict.items() if len(cases) > 1}
        tools.assert_true(len(error_info_dict) <= 0, "Pre Check Failed, Duplicate DBs: \n%s" % json.dumps(error_info_dict, indent=2))

    @staticmethod
    def _replace_uuid_variables(sql_list: List[str]) -> List[str]:
        ret = list()
        variable_dict = dict()
        for sql in sql_list:
            uuid_vars = re.findall(r"\${(uuid[0-9]*)}", sql)
            for each_uuid in uuid_vars:
                if each_uuid not in variable_dict:
                    variable_dict[each_uuid] = uuid.uuid4().hex

        for sql in sql_list:
            for each_var in variable_dict:
                sql = sql.replace("${%s}" % each_var, variable_dict[each_var])
            ret.append(sql)
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
