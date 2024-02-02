#!/usr/bin/env python
# -- coding: utf-8 --
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
sr_sql_lib.py

sr api lib in this module
@Time : 2022/11/03 10:34
@Author : Brook.Ye
"""
import base64
import bz2
import configparser
import datetime
import json
import logging
import os
import re
import subprocess

import ast
import time
import unittest
import uuid

import pymysql as _mysql
from nose import tools
from cup import log

from lib import skip
from lib import data_delete_lib
from lib import data_insert_lib
from lib.mysql_lib import MysqlLib

lib_path = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.abspath(os.path.join(lib_path, "../"))
common_sql_path = os.path.join(root_path, "common/sql")
common_data_path = os.path.join(root_path, "common/data")
common_result_path = os.path.join(root_path, "common/result")


LOG_DIR = os.path.join(root_path, "log")
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)


class Filter(logging.Filter):
    """
    Msg filters by log levels
    """

    # pylint: disable= super-init-not-called
    def __init__(self, msg_level=logging.WARNING):
        super().__init__()
        self.msg_level = msg_level

    def filter(self, record):
        # replace secret infos
        for secret_k, secret_v in SECRET_INFOS.items():
            try:
                record.msg = record.msg.replace(secret_v, '${%s}' % secret_k)
            except Exception:
                record.msg = str(record.msg).replace(secret_v, '${%s}' % secret_k)

        if record.levelno >= self.msg_level:
            return False
        return True


def self_print(msg):
    # replace secret infos
    for secret_k, secret_v in SECRET_INFOS.items():
        msg = msg.replace(secret_v, '${%s}' % secret_k)

    print(msg)


__LOG_FILE = os.path.join(LOG_DIR, "sql_test.log")
log.init_comlog("sql", log.INFO, __LOG_FILE, log.ROTATION, 100 * 1024 * 1024, False)
logging.getLogger().addFilter(Filter())

T_R_DB = "t_r_db"
T_R_TABLE = "t_r_table"

RESULT_FLAG = "-- result:"
RESULT_END_FLAT = "-- !result"
SHELL_FLAG = "shell: "
FUNCTION_FLAG = "function: "
NAME_FLAG = "-- name: "
UNCHECK_FLAG = "[UC]"
ORDER_FLAG = "[ORDER]"
REGEX_FLAG = "[REGEX]"

SECRET_INFOS = {}


class StarrocksSQLApiLib(object):
    """api lib"""

    version = os.environ.get("version", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_path = root_path
        self.mysql_lib = MysqlLib()
        self.be_num = 0
        self.mysql_host = ""
        self.mysql_port = ""
        self.mysql_user = ""
        self.mysql_password = ""
        self.data_insert_lib = data_insert_lib.DataInsertLib()
        self.data_delete_lib = data_delete_lib.DataDeleteLib()

        # for t/r record
        self.log = []
        self.res_log = []

        config_path = os.environ.get("config_path")
        if config_path is None or config_path == "":
            self.read_conf("conf/sr.conf")
        else:
            self.read_conf(config_path)

    def __del__(self):
        pass

    def setUp(self):
        """set up:read cluster conf"""
        pass

    def tearDown(self):
        """tear down"""
        pass

    @classmethod
    def setUpClass(cls) -> None:
        pass

    def read_conf(self, path):
        """read conf"""
        config_parser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        config_parser.read("%s/%s" % (root_path, path))
        self.mysql_host = config_parser.get("mysql-client", "host")
        self.mysql_port = config_parser.get("mysql-client", "port")
        self.mysql_user = config_parser.get("mysql-client", "user")
        self.mysql_password = config_parser.get("mysql-client", "password")
        self.http_port = config_parser.get("mysql-client", "http_port")

        # read replace info
        for rep_key, rep_value in config_parser.items("replace"):
            self.__setattr__(rep_key, rep_value)

        # read env info
        for env_key, env_value in config_parser.items("env"):
            if not env_value:
                env_value = os.environ.get(env_key, "")
            else:
                # save secrets info
                if 'aws' in env_key:
                    SECRET_INFOS[env_key] = env_value

            self.__setattr__(env_key, env_value)

    def connect_starrocks(self):
        mysql_dict = {
            "host": self.mysql_host,
            "port": self.mysql_port,
            "user": self.mysql_user,
            "password": self.mysql_password,
        }
        self.mysql_lib.connect(mysql_dict)

    def close_starrocks(self):
        self.mysql_lib.close()

    def create_database(self, database_name, tolerate_exist=False):
        """
        create starrocks database if tolerate exist
        """
        if tolerate_exist:
            sql = "create database if not exists %s" % database_name
        else:
            sql = "create database %s" % database_name
        return self.execute_sql(sql)

    def use_database(self, db_name):
        return self.execute_sql("use %s" % db_name)

    def create_database_and_table(self, database_name, table_name, table_sql_path=None, tolerate_exist=False):
        """
        create database, use database and create table
        Args:
            database_name:    db
            table_name:       table
            table_sql_path:   sql dir
            tolerate_exist:   tolerate if not exists
        """
        # create database
        create_db_res = self.create_database(database_name, tolerate_exist=tolerate_exist)
        tools.assert_true(create_db_res["status"], "create database failed")

        # use database
        use_res = self.use_database(database_name)
        tools.assert_true(use_res["status"], "use database failed")

        # get sql content
        if table_sql_path:
            sql = self.get_sql_from_file("%s.sql" % table_name, common_sql_path + "/" + table_sql_path)
        else:
            sql = self.get_sql_from_file("%s.sql" % table_name)
        # replace tolerate exists
        check_if_exists = "IF NOT EXISTS"
        if tolerate_exist and check_if_exists not in sql and check_if_exists.lower() not in sql:
            sql.replace("CREATE TABLE", "CREATE TABLE %s " % check_if_exists)

        create_db_res = self.execute_sql(sql)
        tools.assert_true(create_db_res["status"], "create table failed")

    def drop_database(self, database):
        """
        清空集群环境，删除测试库
        """
        sql = "drop database %s" % database
        result = self.execute_sql(sql)
        return result

    def drop_resource(self, resource):
        """
        drop resource
        """
        sql = "drop resource %s" % resource
        result = self.execute_sql(sql, True)
        return result

    def insert_into(self, args_dict):
        """
        insert into table
        """
        self.data_insert_lib.set_table_schema(args_dict)
        sql = self.data_insert_lib.get_insert_schema()
        result = self.execute_sql(sql)
        return result

    @staticmethod
    def get_sql_from_file(file_name, dir_path=common_sql_path):
        """
        get sql from file
        """
        file_path = "%s/%s" % (dir_path, file_name)
        with open(file_path, "r") as f:
            sql = ""
            for line in f.readlines():
                if not line.startswith("--") and not line.startswith("#"):
                    sql = sql + " " + line.strip()
        return sql

    @staticmethod
    def get_common_data_files(path_name, dir_path=common_data_path):
        """
        get files path
        """
        data_path = os.path.join(dir_path, path_name)
        file_list = os.listdir(data_path)
        file_path_list = []
        for file_name in file_list:
            file_path_list.append(os.path.join(data_path, file_name))
        return file_path_list

    def execute_sql(self, sql, ori=False):
        """execute query"""
        try:
            with self.mysql_lib.connector.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                if isinstance(result, tuple):
                    index = 0
                    for res in result:
                        res = list(res)
                        # type to str
                        col_index = 0
                        for col_data in res:
                            if isinstance(col_data, bytes):
                                try:
                                    res[col_index] = col_data.decode()
                                except UnicodeDecodeError as e:
                                    log.info("decode sql result by utf-8 error, try str")
                                    res[col_index] = str(col_data)
                            col_index += 1

                        result = list(result)
                        result[index] = tuple(res)
                        result = tuple(result)
                        index += 1

                res_log = []

                if ori:
                    return {"status": True, "result": result, "msg": cursor._result.message}

                if isinstance(result, tuple):
                    if len(result) > 0:
                        if isinstance(result[0], tuple):
                            res_log.extend(["\t".join([str(y) for y in x]) for x in result])
                        else:
                            res_log.extend(["\t".join(str(x)) for x in result])
                elif isinstance(result, bytes):
                    if res_log != b"":
                        res_log.append(str(result).strip())
                elif result is not None and str(result).strip() != "":
                    log.info("execute sql not bytes or tuple")
                    res_log.append(str(result).strip())
                else:
                    log.info("execute sql empty result")
                    raise Exception("execute sql result type unknown")

                return {"status": True, "result": "\n".join(res_log), "msg": cursor._result.message}

        except _mysql.Error as e:
            return {"status": False, "msg": e.args}
        except Exception as e:
            print("unknown error", e)
            raise

    def delete_from(self, args_dict):
        """
        delete from table
        {
            "database_name" : "test_db_name",
            "table_name" : "test_table",
            "partition_desc" : ["p1",  "p2"]
            "table_name" : "test_table",
            "query" : "query sql",
        }
        """
        self.data_delete_lib.set_table_schema(args_dict)
        sql = self.data_delete_lib.get_delete_schema()
        result = self.execute_sql(sql, True)
        return result

    def delete_with_query(self, query, table_name, database_name):
        """
        delete from db with condition
        :param query: query conditionL
        :param table_name: table
        :param database_name: db
        :return: delete result
        """
        args = {"table_name": table_name, "database_name": database_name, "query": query}
        return self.delete_from(args)

    def treatment_record_res(self, sql, sql_res):
        if any(re.match(condition, sql) is not None for condition in skip.skip_res_cmd):
            return

        self.res_log.append(RESULT_FLAG)
        if not sql_res["status"]:
            self.res_log.append("E: %s" % str(sql_res["msg"]))
        else:
            # msg info no need to be checked
            sql_res = sql_res["result"] if "result" in sql_res else ""
            if isinstance(sql_res, tuple):
                if len(sql_res) > 0:
                    if isinstance(sql_res[0], tuple):
                        self.res_log.extend(["\t".join([str(y) for y in x]) for x in sql_res])
                    else:
                        self.res_log.extend(["\t".join(str(x)) for x in sql_res])
            elif isinstance(sql_res, bytes):
                if sql_res != b"":
                    self.res_log.append(str(sql_res).strip())
            elif sql_res is not None and str(sql_res).strip() != "":
                self.res_log.append(str(sql_res))
            else:
                log.info("SQL result: %s" % sql_res)

        self.res_log.append(RESULT_END_FLAT)

    def record_shell_res(self, shell, shell_res):
        """
        record shell res
        :param shell: command
        :param shell_res: shell result
        """
        if any(re.match(condition, shell) is not None for condition in skip.skip_res_cmd):
            return

        self.res_log.append(RESULT_FLAG)
        # return code
        self.res_log.append("%s" % shell_res[0])

        if shell.endswith("_stream_load"):
            self.res_log.append(
                json.dumps(
                    {"Status": json.loads(shell_res[1])["Status"], "Message": json.loads(shell_res[1])["Message"]},
                    indent="    ",
                )
            )
        else:
            self.res_log.append("%s" % shell_res[1])

        self.res_log.append(RESULT_END_FLAT)

    def record_function_res(self, function, func_res):
        """
        record function res
        :param function: function and args
        :param func_res: result
        """
        if any(re.match(condition, function) is not None for condition in skip.skip_res_cmd):
            return

        self.res_log.append(RESULT_FLAG)
        self.res_log.append("%s" % func_res)
        self.res_log.append(RESULT_END_FLAT)

    def execute_shell(self, shell: str):
        """execute shell"""

        log.info("shell cmd: %s" % shell)

        cmd_res = subprocess.run(
            shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=1800, shell=True
        )

        log.info("shell result: code: %s, stdout: %s" % (cmd_res.returncode, cmd_res.stdout))
        return [
            cmd_res.returncode,
            cmd_res.stdout.rstrip("\n") if cmd_res.returncode == 0 else cmd_res.stderr.rstrip("\n"),
        ]

    def meta_sync(self):
        return self.execute_sql("sync", True)

    def replace(self, cmd):
        """replace ${**} with self attrs"""
        match_words = re.compile("\\${([a-zA-Z0-9._-]+)}").findall(cmd)
        for match_word in match_words:
            cmd = cmd.replace("${%s}" % match_word, self.__getattribute__(match_word))

        return cmd

    def analyse_var(self, cmd):
        """
        analyse sql/function/shell, return variable name if marked to set
        """
        var = None

        regex = re.compile("[a-zA-Z0-9_-]+=")
        if regex.match(cmd):
            # set variable
            var = regex.match(cmd).group()
            cmd = cmd[len(var):]
            var = var[:-1]

        # replace variable dynamically, only replace right of '='
        match_words = re.compile("\\${([^}]*)}").findall(cmd)
        for each_word in match_words:
            cmd = cmd.replace("${%s}" % each_word, str(eval("self.%s" % each_word)))

        return var, cmd

    @staticmethod
    def pretreatment_res(res):
        """
        pretreatment result, more than 1 lines/contains null...
        Args:
            res: execute SQL/SHELL result

        Returns:
            res, result_for_log
        """

        res_for_log = res if len(res) < 1000 else res[:1000] + "..."
        # array pretreatment
        if res.startswith("["):
            # list result
            if "\n" in res:
                # many lines, replace null to None for Python
                res = res.replace("null", "None").replace("NULL", "None").split("\n")
            else:
                # only one line
                log.info("before: %s" % res)
                try:
                    res = ast.literal_eval(res.replace("null", "None"))
                except Exception as e:
                    log.warn("converse array error: %s, %s" % (res, e))

        return res, res_for_log

    @staticmethod
    def check(sql_id, sql, exp, act, order=False, ori_sql=None):
        """check sql result"""
        # judge if it needs to check
        if exp == "":
            if sql.startswith(SHELL_FLAG):
                # SHELL check
                log.info("[%s.check] only check with no Error" % sql_id)
                tools.assert_equal(0, act[0], "shell %s error: %s" % (sql, act))
            elif not sql.startswith(FUNCTION_FLAG):
                # Function, without error msg
                log.info("[%s.check] only check with no Error" % sql_id)
                tools.assert_false(str(act).startswith("E: "), "sql result not match: actual with E(%s)" % str(act))
            else:
                # SQL, with empty result
                exp = []
            return

        if any(re.compile(condition).search(sql) is not None for condition in skip.skip_res_cmd) or any(
            condition in sql for condition in skip.skip_res_cmd
        ):
            log.info("[%s.check] skip check" % sql_id)
            return

        tmp_ori_sql = ori_sql[len(UNCHECK_FLAG) :] if ori_sql.startswith(UNCHECK_FLAG) else ori_sql
        if tmp_ori_sql.startswith(SHELL_FLAG):
            tools.assert_equal(int(exp.split("\n")[0]), act[0], "shell %s error: %s" % (sql, act))

            exp_code = exp.split("\n")[0]
            exp_std = "\n".join(exp.split("\n")[1:])
            exp_std_is_json = exp_std.startswith("{")

            act_code = act[0]
            act_std = act[1]
            act_std_is_json = act_std.startswith("{")
            # check json/str match
            tools.assert_equal(exp_std_is_json, act_std_is_json)

            if exp_std_is_json:
                try:
                    exp_std = json.loads(exp_std)
                    act_std = json.loads(act_std)
                    # check all key,values in exp_std
                    tools.assert_true(
                        all(k in act_std and exp_std[k] == act_std[k] for k in exp_std),
                        "shell result json not match, \n[exp]: %s,\n[act]: %s" % (exp_std, act_std),
                    )
                    return

                except Exception as e:
                    log.debug("Try to treat res as json failed!\n:%s" % e)

                # If result can't be treated as json, cmp as str
                if exp_std == act_std:
                    return

                try:
                    tools.assert_true(re.match(exp_std, act_std, flags=re.S),
                                      "shell result str|re not match,\n[exp]: %s,\n [act]: %s" % (exp_std, act_std))
                except Exception as e:
                    log.warn("Try to treat res as regex, failed!\n:%s" % e)

                tools.assert_true(False, "shell result str|re not match,\n[exp]: %s,\n [act]: %s" % (exp_std, act_std))

            else:
                # str
                if exp_std != act_std and not re.match(exp_std, act_std, flags=re.S):
                    tools.assert_true(False, "shell result str not match,\n[exp]: %s,\n [act]: %s" % (exp_std, act_std))

        elif tmp_ori_sql.startswith(FUNCTION_FLAG):
            # only support str result
            tools.assert_equal(str(exp), str(act))
        else:
            if exp.startswith(REGEX_FLAG):
                log.info("[check regex]: %s" % exp[len(REGEX_FLAG) :])
                tools.assert_regexp_matches(
                    r"%s" % str(act),
                    exp[len(REGEX_FLAG) :],
                    "sql result not match regex:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                    % (sql, exp[len(REGEX_FLAG) :], act),
                )
                return

            try:
                if exp.startswith("["):
                    log.info("[check type]: List")
                    # list result
                    if "\n" in exp:
                        # many lines
                        expect_res = exp.replace("null", "None").split("\n")
                    else:
                        # only one line
                        try:
                            expect_res = ast.literal_eval(exp.replace("null", "None"))
                        except Exception as e:
                            log.warn("converse array error: %s, %s" % (exp, e))
                            expect_res = str(exp)

                    tools.assert_equal(type(expect_res), type(act), "exp and act results' type not match")

                    if order:
                        tools.assert_list_equal(
                            expect_res,
                            act,
                            "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                            % (sql, expect_res, act),
                        )
                    else:
                        tools.assert_count_equal(
                            expect_res,
                            act,
                            "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                            % (sql, expect_res, act),
                        )
                    return
                elif exp.startswith("{") and exp.endswith("}"):
                    log.info("[check type]: DICT")
                    tools.assert_equal(type(exp), type(act), "exp and act results' type not match")
                    # list result
                    tools.assert_dict_equal(
                        json.loads(exp),
                        json.loads(act),
                        "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---" % (sql, exp, act),
                    )
                    return
            except Exception as e:
                log.warn("analyse result before check error, %s" % e)

            # check str
            log.info("[check type]: Str")
            tools.assert_equal(type(exp), type(act), "exp and act results' type not match for %s" % sql)

            if exp.startswith("E:") and act.startswith("E:"):
                if "url:" in exp and "url:" in act:
                    # TODO. support url msg check in the future
                    log.info("Both Error msg with url, skip detail check")
                    return
                else:
                    # ERROR msg, regex check
                    tools.assert_equal(act, exp)

            exp = exp.split("\n") if isinstance(exp, str) else exp
            act = act.split("\n") if isinstance(act, str) else act

            if order:
                tools.assert_list_equal(
                    exp,
                    act,
                    "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---" % (sql, exp, act),
                )
            else:
                tools.assert_count_equal(
                    exp, act, "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---" % (sql, exp, act)
                )

    @staticmethod
    def compress(ori_str, c_round=10):
        """compress str"""
        new_b = ori_str.encode(encoding="utf-8")

        while c_round > 0:
            new_b = bz2.compress(new_b)
            c_round -= 1

        return base64.b64encode(new_b).decode()

    @staticmethod
    def decompress(target_str, c_round=10):
        """decompress str"""
        res_b = base64.b64decode(target_str.encode())

        while c_round > 0:
            res_b = bz2.decompress(res_b)
            c_round -= 1

        return res_b.decode()

    def save_r_into_db(self, test_filepath, case_name, case_log, version):
        """
        save r log into database
        Args:
            test_filepath: t/r path
            case_name:    case name
            case_log:     case log list
            version:      runtime record
        """
        # R file
        test_filepath = test_filepath.replace("/T/", "/R/")
        test_filepath = os.path.abspath(test_filepath).split(self.root_path)[1].lstrip("/")

        # create record table
        self.create_database_and_table(T_R_DB, T_R_TABLE, "sql_framework", True)

        # write record into db
        log.info("start insert...")
        # delete old info
        condition = "file='%s' AND log_type='R' AND name='%s' AND version='%s'" % (test_filepath, case_name, version)
        delete_res = self.delete_with_query(condition, T_R_TABLE, T_R_DB)
        tools.assert_true(delete_res["status"], delete_res["msg"])

        new_log = self.compress("\n".join(case_log))

        insert_round = 1
        while len(new_log) > 0:
            current_log = new_log[: min(len(new_log), 65533)]
            new_log = new_log[len(current_log) :]

            arg_dict = {
                "database_name": T_R_DB,
                "table_name": T_R_TABLE,
                "columns": ["file", "log_type", "name", "version", "log", "sequence"],
                "values": [[test_filepath, "R", case_name, version, current_log, insert_round]],
            }

            # write record into db
            insert_res = self.insert_into(arg_dict)
            log.info("insert case sql, round: %s, result: %s" % (insert_round, insert_res))

            if not insert_res["status"]:
                return False

            insert_round += 1

        log.info("end insert...")

        return True

    def save_r_into_file(self, part=False):
        """save r into file from db"""
        self.connect_starrocks()
        use_res = self.use_database(T_R_DB)
        tools.assert_true(use_res["status"], "use db: [%s] error" % T_R_DB)

        self.execute_sql("set group_concat_max_len = 1024000;", True)
        
        # get records
        query_sql = """
        select file, log_type, name, group_concat(log, ""), group_concat(hex(sequence), ",") 
        from (
            select * from %s.%s where version=\"%s\" and log_type="R" order by sequence
        ) a 
        group by file, log_type, name;
        """ % (
            T_R_DB,
            T_R_TABLE,
            self.version,
        )
        log.debug(query_sql)

        results = self.execute_sql(query_sql, True)
        tools.assert_true(results["status"], "select from table: [%s] error: %s" % (T_R_TABLE, results))
        results = results["result"]

        self.close_starrocks()

        # key: file_path, values: log[]
        file_dict = {}

        for each_record in results:
            file, log_type, name, case_log, sequence = each_record
            case_log = self.decompress(case_log)
            file_dict.setdefault(file, {})[name] = case_log

        # if part mode, merge the result w
        for file in file_dict.keys():
            logs = file_dict[file]
            file_dict[file] = self.merge_case_info(part, file, logs)

        for file, logs in file_dict.items():
            t_file = file.replace("/R/", "/T/")
            case_names = self._get_case_names(t_file)
            # write into file
            file_path = os.path.join(self.root_path, file)

            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))

            lines = list()
            for case_name in case_names:
                lines.append(logs.get(case_name, ""))
                if case_name in logs.keys():
                    logs.pop(case_name)

            if len(logs) > 0:
                log.info("% has case logs not write to R files: %s" % (file, logs))

            with open(file_path, "w") as f:
                f.write("\n".join(lines))

        # drop db
        self.connect_starrocks()
        self.drop_database(T_R_DB)
        self.close_starrocks()

    def merge_case_info(self, part, file, new_log_info: dict):
        """merge case info with ori content"""
        if not part:
            return new_log_info

        ori_log_info: dict = self.get_case_info_from_file(file)

        for k, v in new_log_info.items():
            ori_log_info[k] = v
            # new_log_info.update(ori_log_info)
        return ori_log_info

    def get_case_info_from_file(self, file):
        """case result files"""
        file_path = os.path.join(self.root_path, file)
        info_dict = {}

        if not os.path.exists(file_path):
            # new file, skip
            return info_dict

        with open(file_path, "r") as f:
            content = f.readlines()

        case_name = ""
        case_log = []
        line_id = 0
        while line_id < len(content):
            line = content[line_id]

            if line.startswith(NAME_FLAG):
                # skip first line, else save into dict
                if case_name != "":
                    info_dict[case_name] = "".join(case_log)

                # case first line
                case_name = re.compile("name: ([a-zA-Z0-9_-]+)").findall(line)[0]
                case_log = [line]

            else:
                case_log.append(line)

            line_id += 1

        if case_name != "":
            info_dict[case_name] = "".join(case_log)

        return info_dict

    def _get_case_names(self, t_file):
        file_path = os.path.join(self.root_path, t_file)

        case_names = list()
        if not os.path.exists(file_path):
            return case_names

        with open(file_path, "r") as f:
            contents = f.readlines()

        for line in contents:
            if not line.startswith(NAME_FLAG):
                continue
            case_names.append(re.compile("name: ([a-zA-Z0-9_-]+)").findall(line)[0])
        return case_names

    def show_schema_change_task(self):
        show_sql = "show alter table column"
        return self.execute_sql(show_sql, True)

    @staticmethod
    def TO_JSON(string):
        return json.loads(string)

    def wait_load_finish(self, label, time_out=300):
        times = 0
        while times < time_out:
            result = self.execute_sql('show load where label = "' + label + '"', True)
            log.info('show load where label = "' + label + '"')
            log.info(result)
            if len(result["result"]) > 0:
                load_state = result["result"][0][2]
                log.info(load_state)
                if load_state == "CANCELLED":
                    log.info(result)
                    return False
                elif load_state == "FINISHED":
                    log.info(result)
                    return True
            time.sleep(1)
            times += 1
        tools.assert_less(times, time_out, "load failed, timeout 300s")

    def show_routine_load(self, routine_load_task_name):
        show_sql = "show routine load for %s" % routine_load_task_name
        return self.execute_sql(show_sql, True)

    def check_routine_load_progress(self, sum_data_count, task_name):
        load_finished = False
        count = 0
        while count < 60:
            res = self.show_routine_load(task_name)
            tools.assert_true(res["status"])
            progress_dict = eval(res["result"][0][14])
            if "OFFSET_BEGINNING" in list(progress_dict.values()):
                time.sleep(5)
                count += 1
                continue
            for partition in list(progress_dict.keys()):
                if "OFFSET_ZERO" == progress_dict[partition]:
                    del progress_dict[partition]
            # The progress is counted from offset 0. So the actual number of message should +1.
            current_data_count = sum(int(progress_dict[key]) for key in progress_dict) + len(progress_dict.items())
            log.info(current_data_count)
            if current_data_count != sum_data_count:
                time.sleep(5)
            else:
                # Sleep for a little while to await the publishing finished.
                time.sleep(10)
                load_finished = True
                break
            count += 1
        tools.assert_true(load_finished)

    def check_index_progress(self):
        load_finished = False
        count = 0
        while count < 30:
            res = self.show_schema_change_task()
            tools.assert_true(res["status"], "show schema change task error")
            log.info("show schema result: %s" % res)
            number = 0
            for result in res["result"]:
                if result[9] == "FINISHED":
                    number += 1
                else:
                    continue
            if number == len(res["result"]):
                load_finished = True
                break
            else:
                time.sleep(10)
            count += 1
        tools.assert_true(load_finished, "show bitmap_index timeout")

    def wait_materialized_view_finish(self, check_count=60):
        """
        wait materialized view job finish and return status
        """
        status = ""
        show_sql = "SHOW ALTER MATERIALIZED VIEW"
        count = 0
        while count < check_count:
            res = self.execute_sql(show_sql, True)
            status = res["result"][-1][8]
            if status != "FINISHED":
                time.sleep(1)
            else:
                # sleep another 5s to avoid FE's async action.
                time.sleep(1)
                break
            count += 1
        tools.assert_equal("FINISHED", status, "wait alter table finish error")

    def wait_async_materialized_view_finish(self, mv_name, min_success_num = 1, check_count=60):
        """
        wait async materialized view job finish and return status
        """
        status = ""
        show_sql = "SHOW MATERIALIZED VIEWS WHERE name='" + mv_name + "'"
        count = 0
        success_num = 0
        while count < check_count and success_num < min_success_num:
            res = self.execute_sql(show_sql, True)
            status = res["result"][-1][12]
            if status != "SUCCESS":
                time.sleep(1)
            else:
                # sleep another 5s to avoid FE's async action.
                time.sleep(1)
                success_num += 1
            count += 1
        tools.assert_equal("SUCCESS", status, "wait aysnc materialized view finish error")

    def wait_for_pipe_finish(self, db_name, pipe_name, check_count=60):
        """
        wait pipe load finish
        """
        status = ""
        show_sql = "select state from information_schema.pipes where database_name='{}' and pipe_name='{}'".format(db_name, pipe_name)
        count = 0
        print("waiting for pipe {}.{} finish".format(db_name, pipe_name))
        while count < check_count:
            res = self.execute_sql(show_sql, True)
            print(res)
            status = res["result"][0][0]
            if status != "FINISHED":
                print("pipe status is " + status)
                time.sleep(1)
            else:
                # sleep another 5s to avoid FE's async action.
                time.sleep(1)
                break
            count += 1
        tools.assert_equal("FINISHED", status, "didn't wait pipe finish")


    def check_hit_materialized_view_plan(self, res, mv_name):
        """
        assert mv_name is hit in query
        """
        tools.assert_true(str(res).find(mv_name) > 0, "assert mv %s is not found" % (mv_name))

    def check_hit_materialized_view(self, query, *expects):
        """
        assert mv_name is hit in query
        """
        time.sleep(1)
        sql = "explain %s" % (query)
        res = self.execute_sql(sql, True)
        for expect in expects:
            tools.assert_true(str(res["result"]).find(expect) > 0, "assert expect %s is not found in plan" % (expect))

    def check_no_hit_materialized_view(self, query, mv_name):
        """
        assert mv_name is hit in query
        """
        time.sleep(1)
        sql = "explain %s" % (query)
        res = self.execute_sql(sql, True)
        tools.assert_false(str(res["result"]).find(mv_name) > 0, "assert mv %s is not found" % (mv_name))

    def wait_alter_table_finish(self, alter_type="COLUMN", off=9):
        """
        wait alter table job finish and return status
        """
        status = ""
        sleep_time = 0
        while True:
            res = self.execute_sql(
                "SHOW ALTER TABLE %s ORDER BY CreateTime DESC LIMIT 1" % alter_type,
                True,
            )
            if (not res["status"]) or len(res["result"]) <= 0:
                return ""

            status = res["result"][0][off]
            if status == "FINISHED" or status == "CANCELLED" or status == "":
                if sleep_time <= 1:
                    time.sleep(1)
                break
            time.sleep(0.5)
            sleep_time += 0.5
        tools.assert_equal("FINISHED", status, "wait alter table finish error")

    def wait_alter_table_not_pending(self, alter_type="COLUMN"):
        """
        wait until the status of the latest alter table job becomes from PNEDING to others
        """
        status = ""
        while True:
            res = self.execute_sql(
                "SHOW ALTER TABLE %s ORDER BY CreateTime DESC LIMIT 1" % alter_type,
                True,
            )
            if (not res["status"]) or len(res["result"]) <= 0:
                return None

            status = res["result"][0][9]
            if status != "PENDING":
                break
            time.sleep(0.5)
    
    def wait_optimize_table_finish(self, alter_type="OPTIMIZE", expect_status="FINISHED"):
        """
        wait alter table job finish and return status
        """
        status = ""
        while True:
            res = self.execute_sql(
                "SHOW ALTER TABLE %s ORDER BY CreateTime DESC LIMIT 1" % alter_type,
                True,
            )
            if (not res["status"]) or len(res["result"]) <= 0:
                return ""

            status = res["result"][0][6]
            if status == "FINISHED" or status == "CANCELLED" or status == "":
                break
            time.sleep(0.5)
        tools.assert_equal(expect_status, status, "wait alter table finish error")

    def wait_optimize_table_finish(self, alter_type="OPTIMIZE"):
        """
        wait alter table job finish and return status
        """
        status = ""
        while True:
            res = self.execute_sql(
                "SHOW ALTER TABLE %s ORDER BY CreateTime DESC LIMIT 1" % alter_type,
                True,
            )
            if (not res["status"]) or len(res["result"]) <= 0:
                return ""

            status = res["result"][0][6]
            if status == "FINISHED" or status == "CANCELLED" or status == "":
                break
            time.sleep(0.5)
        tools.assert_equal("FINISHED", status, "wait alter table finish error")

    def wait_global_dict_ready(self, column_name, table_name):
        """
        wait global dict ready
        """
        status = ""
        count = 0
        while True:
            if count > 60:
                tools.assert_true(False, "acquire dictionary timeout for 60s")
            sql = "explain costs select distinct %s from %s" % (column_name, table_name)
            res = self.execute_sql(sql, True)
            if not res["status"]:
                tools.assert_true(False, "acquire dictionary error")
            if str(res["result"]).find("Decode") > 0:
                return ""
            time.sleep(1)

    def assert_has_global_dict(self, column_name, table_name):
        """
        assert table_name:column_name has global dict
        """
        time.sleep(1)
        sql = "explain costs select distinct %s from %s" % (column_name, table_name)
        res = self.execute_sql(sql, True)
        tools.assert_true(str(res["result"]).find("Decode") > 0, "assert dictionary error")

    def assert_no_global_dict(self, column_name, table_name):
        """
        assert table_name:column_name has global dict
        """
        time.sleep(1)
        sql = "explain costs select distinct %s from %s" % (column_name, table_name)
        res = self.execute_sql(sql, True)
        tools.assert_true(str(res["result"]).find("Decode") <= 0, "assert dictionary error")

    def wait_submit_task_ready(self, task_name):
        """
        wait submit task ready
        """
        status = ""
        while True:
            sql = "select STATE from information_schema.task_runs where TASK_NAME = '%s'" % task_name
            res = self.execute_sql(sql, True)
            if not res["status"]:
                tools.assert_true(False, "acquire task state error")
            state = res["result"][0][0]
            if status != "RUNNING":
                return ""
            time.sleep(1)

    def check_es_table_metadata_ready(self, table_name):
        check_sql = "SELECT * FROM %s limit 1" % table_name
        count = 0
        log.info("==========begin check if es table metadata is ready==========")
        while count < 60:
            res = self.execute_sql(check_sql, True)
            if res["status"]:
                log.info("==========check success: es table metadata is ready==========")
                return
            else:
                if (
                    res["msg"][1].find("EsTable metadata has not been synced, Try it later") == -1
                    and res["msg"][1].find("metadata failure: null") == -1
                ):
                    log.info("==========check success: es table metadata is ready==========")
                    return
                else:
                    time.sleep(10)
            count += 1
        tools.assert_true(False, "check es table metadata 600s timeout")

    def _stream_load(self, label, database_name, table_name, filepath, headers=None, meta_sync=True):
        """ """
        url = (
            "http://"
            + self.mysql_host
            + ":"
            + self.http_port
            + "/api/"
            + database_name
            + "/"
            + table_name
            + "/_stream_load"
        )
        params = [
            "curl",
            "--location-trusted",
            "-u",
            "%s:%s" % (self.mysql_user, self.mysql_password),
            "-T",
            filepath,
            "-XPUT",
            "-H",
            "label:%s" % label,
        ]

        if headers:
            for k, v in headers.items():
                params.append("-H")
                params.append("%s:%s" % (k, v))

        params.append(url)
        stream_load_sql = " ".join(param for param in params)
        log.info(stream_load_sql)

        cmd_res = subprocess.run(
            params,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            timeout=1800,
        )

        log.info(cmd_res)
        if cmd_res.returncode != 0:
            return {"Status": "CommandFail", "Message": cmd_res.stderr}

        res = json.loads(cmd_res.stdout)

        if meta_sync:
            self.meta_sync()

        if res["Status"] == "Publish Timeout":
            cmd = "curl -s --location-trusted -u %s:%s http://%s:%s/api/%s/get_load_state?label=%s" % (
                self.mysql_user,
                self.mysql_password,
                self.mysql_host,
                self.http_port,
                database_name,
                label,
            )
            print(cmd)
            cmd = cmd.split(" ")
            for i in range(60):
                time.sleep(3)
                res = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    encoding="utf-8",
                    timeout=60,
                )

                if res.returncode != 0:
                    return {"Status": "CommandFail", "Message": res.stderr}

                res = json.loads(res.stdout)
                if res["state"] == "VISIBLE":
                    break
                else:
                    log.error(res)
                    res["Status"] = "Failed"
        return res

    def prepare_data(self, data_name, db):
        """ load data """
        tools.assert_in(data_name, ["ssb", "tpch", "tpcds"], "Unsupported data!")

        # create tables
        create_table_sqls = self.get_sql_from_file("create.sql", dir_path=os.path.join(common_sql_path, data_name))
        res = self.execute_sql(create_table_sqls, True)
        tools.assert_true(res["status"], "create %s table error, %s" % (data_name, res["msg"]))
        # load data
        data_files = self.get_common_data_files(data_name)
        for data in data_files:
            if ".gitkeep" in data:
                continue
            label = "%s_load_label_%s" % (data_name, uuid.uuid1().hex)
            file_name = data.split("/")[-1]
            table_name = file_name.split(".")[0]
            log.info("Load %s..." % table_name)
            headers = {"column_separator": "|"}

            # tpcds prepare
            if data_name == "tpcds":
                switch = {
                    "web_returns": "wr_returned_date_sk,wr_returned_time_sk,wr_item_sk,\
                                               wr_refunded_customer_sk,wr_refunded_cdemo_sk,wr_refunded_hdemo_sk,\
                                               wr_refunded_addr_sk,wr_returning_customer_sk,wr_returning_cdemo_sk,\
                                               wr_returning_hdemo_sk,wr_returning_addr_sk,wr_web_page_sk,wr_reason_sk,\
                                               wr_order_number,wr_return_quantity,wr_return_amt,wr_return_tax,\
                                               wr_return_amt_inc_tax,wr_fee,wr_return_ship_cost,wr_refunded_cash,\
                                               wr_reversed_charge,wr_account_credit,wr_net_loss",
                    "web_sales": "ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,\
                                             ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,\
                                             ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,\
                                             ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,\
                                             ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,\
                                             ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,\
                                             ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,\
                                             ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit",
                    "catalog_returns": "cr_returned_date_sk,cr_returned_time_sk,cr_item_sk,cr_refunded_customer_sk,\
                                                   cr_refunded_cdemo_sk,cr_refunded_hdemo_sk,cr_refunded_addr_sk,\
                                                   cr_returning_customer_sk,cr_returning_cdemo_sk,cr_returning_hdemo_sk,\
                                                   cr_returning_addr_sk,cr_call_center_sk,cr_catalog_page_sk,cr_ship_mode_sk,\
                                                   cr_warehouse_sk,cr_reason_sk,cr_order_number,cr_return_quantity,cr_return_amount,\
                                                   cr_return_tax,cr_return_amt_inc_tax,cr_fee,cr_return_ship_cost,cr_refunded_cash,\
                                                   cr_reversed_charge,cr_store_credit,cr_net_loss",
                    "catalog_sales": "cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,\
                                                 cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,\
                                                 cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,\
                                                 cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,\
                                                 cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,\
                                                 cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,\
                                                 cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit",
                    "store_returns": "sr_returned_date_sk,sr_return_time_sk,sr_item_sk,sr_customer_sk,sr_cdemo_sk,\
                                                 sr_hdemo_sk,sr_addr_sk,sr_store_sk,sr_reason_sk,sr_ticket_number,sr_return_quantity,\
                                                 sr_return_amt,sr_return_tax,sr_return_amt_inc_tax,sr_fee,sr_return_ship_cost,\
                                                 sr_refunded_cash,sr_reversed_charge,sr_store_credit,sr_net_loss",
                    "store_sales": "ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,\
                                               ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,\
                                               ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,\
                                               ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit",
                }
                if table_name in switch.keys():
                    headers["columns"] = switch[table_name]

            res = self._stream_load(label, db, table_name, data, headers)
            tools.assert_equal(res["Status"], "Success", "Prepare %s data error: %s" % (data_name, res["Message"]))

    def execute_cmd(self, exec_url):
        cmd_template = "curl -XPOST -u {user}:{passwd} '" + exec_url + "'"
        cmd = cmd_template.format(user=self.mysql_user, passwd=self.mysql_password)
        res = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=1800
        )
        return str(res)

    def manual_compact(self, database_name, table_name):
        sql = "show tablet from " + database_name + "." + table_name
        res = self.execute_sql(sql, "dml")
        tools.assert_true(res["status"], res["msg"])
        url = res["result"][0][20]

        pos = url.find("api")
        exec_url = url[0:pos] + "api/update_config?min_cumulative_compaction_num_singleton_deltas=0"
        res = self.execute_cmd(exec_url)
        print(res)

        exec_url = url[0:pos] + "api/update_config?base_compaction_interval_seconds_since_last_operation=0"
        res = self.execute_cmd(exec_url)
        print(res)

        exec_url = url[0:pos] + "api/update_config?cumulative_compaction_skip_window_seconds=1"
        res = self.execute_cmd(exec_url)
        print(res)

        exec_url = url.replace("compaction/show", "compact") + "&compaction_type=cumulative"
        res = self.execute_cmd(exec_url)
        print(res)

        exec_url = url.replace("compaction/show", "compact") + "&compaction_type=base"
        res = self.execute_cmd(exec_url)
        print(res)

    def wait_analyze_finish(self, database_name, table_name, sql):
        timeout = 300
        analyze_sql = "show analyze status where `Database` = 'default_catalog.%s'" % database_name
        res = self.execute_sql(analyze_sql, "dml")
        while timeout > 0:
            res = self.execute_sql(analyze_sql, "dml")
            if len(res["result"]) > 0:
                for table in res["result"]:
                    if table[2] == table_name and table[4] == "FULL" and table[6] == "SUCCESS":
                        break
                break
            else:
                time.sleep(1)
                timeout -= 1
        else:
            tools.assert_true(False, "analyze timeout")

        finished = False
        counter = 0
        while True:
            res = self.execute_sql(sql, "dml")
            tools.assert_true(res["status"], res["msg"])
            if str(res["result"]).find("Decode") > 0:
                finished = True
                break
            time.sleep(5)
            if counter > 10:
                break
            counter = counter + 1

        tools.assert_true(finished, "analyze timeout")
    def assert_table_cardinality(self, sql, rows):
        """
        assert table with an expected row counts
        """
        res = self.execute_sql(sql, True)
        expect = r"cardinality=" + rows
        match = re.search(expect, str(res["result"]))
        print(expect)
        tools.assert_true(match, "expected cardinality: " + rows + ". but found: " + str(res["result"]))
