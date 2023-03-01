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

from nose import tools
from parameterized import parameterized
from cup import log

from lib import sr_sql_lib
from lib import choose_cases
from lib import sql_annotation


# model: run case model, True => Record mode
#    - t: run sql and save result into r dir
#    - r: run sql and compare result with r
record_mode = os.environ.get("record_mode", "false") == "true"

case_list = choose_cases.choose_cases(record_mode).case_list


if len(case_list) == 0:
    print("** ERROR: No case! **")
    sys.exit(0)


def doc_func(_, num, param):
    """doc func"""
    if param[0][0].file == "":
        return "%-5d: %s" % (num, param[0][0].name)
    else:
        return "%-5d: %s" % (num, param[0][0].file + ":" + param[0][0].name)


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
        self.db = []
        self.resource = []

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
    @parameterized.expand(
        [[case_info] for case_info in case_list],
        doc_func=doc_func,
    )
    @sql_annotation.init(record_mode)
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
        print("DB: %s" % self.db)
        for sql_id, sql in enumerate(case_info.sql):
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
                print("[SHELL]: %s" % sql)

                log.info("[%s] SHELL: %s" % (sql_id, sql))
                actual_res = self.execute_shell(sql)

                if record_mode:
                    self.record_shell_res(sql, actual_res)

            elif sql.startswith(sr_sql_lib.FUNCTION_FLAG):
                # function invoke
                sql = sql[len(sr_sql_lib.FUNCTION_FLAG):]

                # analyse var set
                var, sql = self.analyse_var(sql)
                print("[FUNCTION]: %s" % sql)

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
                print("[SQL]: %s" % sql)

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
