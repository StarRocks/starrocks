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
import copy
import datetime
import json
import logging
import sys
import threading
import traceback

import pymysql
import trino
import pyhive

import mysql.connector
import os
import re
import subprocess

import ast
import time
import unittest
import uuid
from typing import List, Dict

from fuzzywuzzy import fuzz
import pymysql as _mysql
import requests
from cup import shell
from nose import tools
from cup import log
from cup.util import conf as configparser
from requests.auth import HTTPBasicAuth
from timeout_decorator import timeout, TimeoutError
from dbutils.pooled_db import PooledDB


from lib import skip
from lib import data_delete_lib
from lib import data_insert_lib
from lib.github_issue import GitHubApi
from lib.mysql_lib import MysqlLib
from lib.trino_lib import TrinoLib
from lib.spark_lib import SparkLib
from lib.hive_lib import HiveLib
from lib import *

lib_path = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.abspath(os.path.join(lib_path, "../"))
common_sql_path = os.path.join(root_path, "common/sql")
common_data_path = os.path.join(root_path, "common/data")
common_result_path = os.path.join(root_path, "common/result")

LOG_DIR = os.path.join(root_path, "log")
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)
CRASH_DIR = os.path.join(root_path, "crash_logs")
if not os.path.exists(CRASH_DIR):
    os.mkdir(CRASH_DIR)

LOG_LEVEL = logging.INFO
QUERY_TIMEOUT = int(os.environ.get("QUERY_TIMEOUT", 60))


class Filter(logging.Filter):
    """
    Msg filters by log levels
    """

    # pylint: disable= super-init-not-called
    def __init__(self, msg_level=LOG_LEVEL):
        super().__init__()
        self.msg_level = msg_level

    def filter(self, record):
        # replace secret infos
        for secret_k, secret_v in SECRET_INFOS.items():
            try:
                record.msg = record.msg.replace(secret_v, "${%s}" % secret_k)
            except Exception:
                record.msg = str(record.msg).replace(secret_v, "${%s}" % secret_k)

        if record.levelno < self.msg_level:
            return False
        return True


def self_print(msg, color: ColorEnum = None, bold=False, logout=False, need_print=True):
    # replace secret infos
    for secret_k, secret_v in SECRET_INFOS.items():
        msg = msg.replace(secret_v, "${%s}" % secret_k)

    if need_print:
        if color:
            bold_str = "1;" if bold else ""
            print(f"\033[{bold_str}{color.value}m{msg} \033[0m")

            if logout:
                log.info(f"\033[{bold_str}{color.value}m{msg} \033[0m")
        else:
            print(msg)

            if logout:
                log.info(msg)

    return msg


__LOG_FILE = os.path.join(LOG_DIR, "sql_test.log")
log.init_comlog("sql", LOG_LEVEL, __LOG_FILE, log.ROTATION, 100 * 1024 * 1024, bprint_console=False, gen_wf=False)
logging.getLogger().addFilter(Filter())

T_R_DB = "t_r_db"
T_R_TABLE = "t_r_table"

SECRET_INFOS = {}


class StarrocksSQLApiLib(object):
    """api lib"""

    version = os.environ.get("version", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    _instance = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_path = root_path
        self.data_path = common_data_path
        self.db = list()
        self.resource = list()
        self.mysql_lib = MysqlLib()
        self.trino_lib = TrinoLib()
        self.spark_lib = SparkLib()
        self.hive_lib = HiveLib()
        self.be_num = 0
        self.mysql_host = ""
        self.mysql_port = ""
        self.mysql_user = ""
        self.mysql_password = ""
        self.http_port = ""
        self.host_user = ""
        self.host_password = ""
        self.cluster_path = ""
        self.data_insert_lib = data_insert_lib.DataInsertLib()
        self.data_delete_lib = data_delete_lib.DataDeleteLib()

        # connection pool
        self.connection_pool = None
        # thread
        self.thread_res = {}
        self.thread_var = {}
        self.thread_res_log = {}

        self.component_status = {}
        self.data_status = {}

        # trino client config
        self.trino_host = ""
        self.trino_port = ""
        self.trino_user = ""

        # spark client config
        self.spark_host = ""
        self.spark_port = ""
        self.spark_user = ""

        # hive client config
        self.hive_host = ""
        self.hive_port = ""
        self.hive_user = ""

        # for t/r record
        self.case_info = None
        self.log = []
        self.res_log = []

        # check data dir
        self.check_data_dir()

        # read conf
        config_path = os.environ.get("config_path")
        if config_path is None or config_path == "":
            self.read_conf("conf/sr.conf")
        else:
            self.read_conf(config_path)

        if os.environ.get("keep_alive") == "True":
            self.keep_alive = True
        else:
            self.keep_alive = False
        self.run_info = os.environ.get("run_info", "")

    def __del__(self):
        pass

    def setUp(self):
        """set up:read cluster conf"""
        pass

    def tearDown(self):
        """tear down"""
        self.keep_cluster_alive()

    def keep_cluster_alive(self):
        """
        check crash msg in case logs, and recover alive
        """
        if not self.keep_alive:
            return

        # check if case result contains crash msg: "StarRocks process failed"
        crash_msg_list = ["StarRocks process failed"]
        contains_crash_msg = self.check_case_result_contains_crash(crash_msg_list, self.res_log)
        # wait util be exit
        if contains_crash_msg:
            self.wait_until_be_exit()

        # if cluster status is abnormal, get crash log
        cluster_status_dict = self.get_cluster_status()

        if isinstance(cluster_status_dict, str):
            if cluster_status_dict == "abnormal":
                log.error("FE status is abnormal!")

            return

        if cluster_status_dict["status"] != "abnormal":
            log.info("Cluster status OK!")
            return

        # TODO: 判断是不是巡检，不然不需要创建issue

        # analyse be crash info
        log.warning("Cluster status is abnormal, begin to get crash log...")
        be_crash_log = self.get_crash_log(cluster_status_dict["ip"][0])

        if be_crash_log != "":
            crash_similarity = self.get_crash_log_similarity(be_crash_log)

            if crash_similarity >= 90:
                log.info("Crash log is similarity, skip create issue")
            else:
                log.info("Max similarity is %f, create new issue" % crash_similarity)
                cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                with open(f"{CRASH_DIR}/crash_{cur_time}.log", "w") as f:
                    f.writelines(be_crash_log)

                be_crash_case = self.case_info.name

                title = f"[{self.run_info}] SQL-Tester crash"
                run_link = os.environ.get("WORKFLOW_URL", "")
                body = (
                        """```\nTest Case:\n    %s\n```\n\n ```\nCrash Log: \n%s\n```\n\n```\nSR Version: %s\nBE: %s\nURL: %s\n\n```"""
                        % (
                            be_crash_case,
                            be_crash_log,
                            cluster_status_dict["version"],
                            cluster_status_dict["ip"][0],
                            run_link,
                        )
                )
                assignee = os.environ.get("ISSUE_AUTHOR")
                repo = os.environ.get("GITHUB_REPOSITORY")
                label = os.environ.get("ISSUE_LABEL")
                create_issue_res = GitHubApi.create_issue(title, body, label, assignee)
                log.info(create_issue_res)
        else:
            log.warning("Crash log is empty, please check. cluster status is %s" % cluster_status_dict)

        # after create issue, restart crash be
        start_be_status = "success"
        for crash_be_ip in cluster_status_dict["ip"]:
            res = self.start_be(crash_be_ip)
            if res != 0:
                start_be_status = "fail"
                print("BE start failed, please check, ip: %s" % crash_be_ip)
                break
        time.sleep(20)
        cluster_dict = self.get_cluster_status()
        if len(cluster_dict["ip"]) != 0:
            log.error("BE start failed, please check, ip: %s" % cluster_dict["ip"])

    def start_be(self, ip):
        # backup be.out
        cur_time = datetime.datetime.now().strftime("%H_%M")
        cmd = "cd %s/be/log/; mv be.out be.out.%s" % (self.cluster_path, cur_time)
        backup_res = shell.expect.go_ex(ip, self.host_user, self.host_password, cmd, timeout=20, b_print_stdout=True)
        if backup_res["exitstatus"] != 0 or backup_res["remote_exitstatus"] != 0:
            log.error("Backup be.out error in host: %s, msg: %s" % (ip, backup_res))

        # be status is not alive and the process exit failed
        timeout = 300
        while timeout >= 0:
            cmd = "ps -ef | grep starrocks_be | grep -v grep"
            stop_res = shell.expect.go_ex(ip, self.host_user, self.host_password, cmd, timeout=20, b_print_stdout=True)
            if stop_res["remote_exitstatus"] == 1:
                break

            time.sleep(5)
            timeout -= 5

        if timeout < 0:
            print("BE exit timeout for 300s after crash, ip: %s" % ip)
            return -1

        time.sleep(10)
        cmd = f". ~/.bash_profile; cd {self.cluster_path}/be; ulimit -c unlimited; export ASAN_OPTIONS=abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1;sh bin/start_be.sh --daemon"
        start_res = shell.expect.go_ex(ip, self.host_user, self.host_password, cmd, timeout=20, b_print_stdout=True)
        if start_res["exitstatus"] != 0 or start_res["remote_exitstatus"] != 0:
            log.error("Start be error, msg: %s" % start_res)
            return -1

        return 0

    @staticmethod
    def get_crash_log_similarity(target_crash_log):
        files_list = os.listdir(CRASH_DIR)
        crash_log_list = []
        similarity = 0
        for crash_file in files_list:
            with open("%s/%s" % (CRASH_DIR, crash_file), "r") as f:
                crash_log = "\n".join(f.readlines())
                crash_log_list.append(crash_log)

        for crash_log in crash_log_list:
            cur_similarity = fuzz.ratio(crash_log, target_crash_log)
            similarity = cur_similarity if cur_similarity > similarity else similarity

        return similarity

    def get_crash_log(self, ip):
        log.warning("Get crash log from %s" % ip)
        cmd = f'cd {self.cluster_path}/be/log/; grep -A10000 "*** Check failure stack trace: ***\|ERROR: AddressSanitizer:" be.out'
        crash_log = shell.expect.go_ex(ip, self.host_user, self.host_password, cmd, timeout=20, b_print_stdout=False)
        return crash_log["result"]

    def wait_until_be_exit(self):
        """
        wait until be was exited
        """
        log.warning("Wait be exit...")
        _timeout = 60
        while _timeout > 0:
            status_dict = self.get_cluster_status()

            if status_dict == "abnormal":
                # fe abnormal
                return

            elif status_dict["status"] == "abnormal":
                return

            else:
                time.sleep(5)
                _timeout -= 1

        return

    def get_cluster_status(self):
        cmd = f"curl http://root:@{self.mysql_host}:{self.http_port}/api/show_proc?path=/backends"
        res = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=30, shell=True
        )
        if res.returncode != 0:
            log.warning("Show backends cmd execute failed, cmd: %s, err_msg: %s" % (cmd, res.stderr))
            return "abnormal"

        status_dict = {"ip": [], "status": "normal"}
        for be_info_dict in json.loads(res.stdout):
            status_dict["version"] = be_info_dict["Version"]
            if be_info_dict["Alive"] == "false":
                status_dict["ip"].append(be_info_dict["IP"])
                status_dict["status"] = "abnormal"

        return status_dict

    @staticmethod
    def check_case_result_contains_crash(crash_msg_list, case_result_logs):
        """
        scan case result logs, return whether contain crash msg
        """
        case_result_logs_str = "\n".join(case_result_logs)

        for crash_msg in crash_msg_list:
            if crash_msg in case_result_logs_str:
                return True

        return False

    @classmethod
    def setUpClass(cls) -> None:
        pass

    def check_data_dir(self):

        def _sort_by_len_and_alph(n):
            return

        for _data_path in os.listdir(self.data_path):
            # check data files exist
            _this_data_path = os.path.join(self.data_path, _data_path)

            # check dirs not empty
            _files = os.listdir(_this_data_path)
            if all(_file.startswith(".") for _file in _files):
                self.data_status[_data_path] = False
                continue
            else:
                self.data_status[_data_path] = True

        if not StarrocksSQLApiLib._instance:
            self_print(f"\n{'-' * 30}\n[Data check]\n{'-' * 30}", color=ColorEnum.BLUE, logout=True, bold=True)
            _data_names = sorted(list(self.data_status.keys()))

            for _data_name in _data_names:
                if self.data_status[_data_name]:
                    self_print("▶ %-20s ... YES" % str(_data_name).upper(), color=ColorEnum.CYAN, bold=True)
                else:
                    self_print("▶ %-20s ... NO" % str(_data_name).upper(), color=ColorEnum.YELLOW, bold=True)
            self_print(f"{'-' * 30}", color=ColorEnum.BLUE, logout=True, bold=True)

    def read_conf(self, path):
        """read conf"""

        def _get_value(_conf_dict, *args):
            _tmp_conf = _conf_dict
            _index = 0
            _now_conf_key = ""

            for arg in args:
                _now_conf_key = ".".join(args[:_index + 1])
                if arg not in _tmp_conf:
                    if not StarrocksSQLApiLib._instance:
                        self_print(f"[Miss config] {_now_conf_key}", color=ColorEnum.YELLOW)

                    return ""

                _tmp_conf = _tmp_conf.get(arg)
                _index += 1

            if isinstance(_tmp_conf, str):
                if _tmp_conf == "":
                    if not StarrocksSQLApiLib._instance:
                        log.warning(f"[Null config] {_now_conf_key}")

                return _tmp_conf
            else:
                return _tmp_conf

        # read conf file to dict
        config_parser = configparser.Configure2Dict(path, separator="=").get_dict()
        config_parser_str = json.dumps(config_parser)

        # replace ${} in conf
        var_strs = set(re.findall(r"\${([a-zA-Z._-]+)}", config_parser_str))
        for var_str in var_strs:
            var_str_path = "['" + var_str.replace(".", "']['") + "']"
            try:
                var_value = eval(f'config_parser{var_str_path}')
            except Exception as e:
                self_print(f"[ERROR] config: {var_str} is incorrect!", color=ColorEnum.RED, bold=True)
                sys.exit(1)
            config_parser_str = config_parser_str.replace(f"${{{var_str}}}", var_value)

        config_parser = json.loads(config_parser_str)

        # update dependency component status dict
        component_list = list(_get_value(config_parser, "env").keys())
        component_list.extend(list(_get_value(config_parser, "client").keys()))

        if not StarrocksSQLApiLib._instance:
            self_print(f"\n{'-' * 30}\n[Component check]\n{'-' * 30}", color=ColorEnum.BLUE, logout=True, bold=True)

        for each_comp in component_list:
            if each_comp == "others":
                continue

            if each_comp in _get_value(config_parser, "env"):
                comp_conf_dict = _get_value(config_parser, "env", each_comp)
            else:
                comp_conf_dict = _get_value(config_parser, "client", each_comp)

            self.component_status[each_comp] = {
                "status": None,
                "keys": list(comp_conf_dict.keys())
            }

            for com_k, com_v in comp_conf_dict.items():
                if str(com_v).strip() == "" and "passwd" not in com_k.lower() and "passwd" not in com_k.lower():
                    self.component_status[each_comp]["status"] = False
                    if not StarrocksSQLApiLib._instance:
                        self_print("▶ %-20s ... NO" % str(each_comp).upper(), color=ColorEnum.YELLOW, bold=True)
                    break

            if self.component_status[each_comp]["status"] is None:
                self.component_status[each_comp]["status"] = True
                if not StarrocksSQLApiLib._instance:
                    self_print("▶ %-20s ... YES" % str(each_comp).upper(), color=ColorEnum.BLUE, bold=True)

        if not StarrocksSQLApiLib._instance:
            self_print(f"{'-' * 30}", color=ColorEnum.BLUE, logout=True, bold=True)

        cluster_conf = _get_value(config_parser, "cluster")
        self.mysql_host = _get_value(cluster_conf, "host")
        self.mysql_port = _get_value(cluster_conf, "port")
        self.mysql_user = _get_value(cluster_conf, "user")
        self.mysql_password = _get_value(cluster_conf, "password")
        self.http_port = _get_value(cluster_conf, "http_port")
        self.host_user = _get_value(cluster_conf, "host_user")
        self.host_password = _get_value(cluster_conf, "host_password")
        self.cluster_path = _get_value(cluster_conf, "cluster_path")

        # client
        client_conf = _get_value(config_parser, "client")
        # parse trino config
        self.trino_host = _get_value(client_conf, "trino-client", "host")
        self.trino_port = _get_value(client_conf, "trino-client", "port")
        self.trino_user = _get_value(client_conf, "trino-client", "user")
        # parse spark config
        self.spark_host = _get_value(client_conf, "spark-client", "host")
        self.spark_port = _get_value(client_conf, "spark-client", "port")
        self.spark_user = _get_value(client_conf, "spark-client", "user")
        # parse hive config
        self.hive_host = _get_value(client_conf, "hive-client", "host")
        self.hive_port = _get_value(client_conf, "hive-client", "port")
        self.hive_user = _get_value(client_conf, "hive-client", "user")

        # read replace info
        for rep_key, rep_value in _get_value(config_parser, "replace").items():
            self.__setattr__(rep_key, rep_value)

        # read env info
        for env_key, env_value in _get_value(config_parser, "env").items():
            for each_env_key, each_env_value in env_value.items():
                if not each_env_value:
                    each_env_value = os.environ.get(each_env_key, "")
                else:
                    # save secrets info
                    if 'aws' in each_env_key or 'oss_' in each_env_key:
                        SECRET_INFOS[each_env_key] = each_env_value

                self.__setattr__(each_env_key, each_env_value)

        StarrocksSQLApiLib._instance = True

    def connect_starrocks(self):
        mysql_dict = {
            "host": self.mysql_host,
            "port": self.mysql_port,
            "user": self.mysql_user,
            "password": self.mysql_password,
        }
        self.mysql_lib.connect(mysql_dict)

    def create_starrocks_conn_pool(self):
        self.connection_pool = PooledDB(
            creator=pymysql,
            mincached=3,
            blocking=True,
            host=self.mysql_host,
            port=int(self.mysql_port),
            user=self.mysql_user,
            password=self.mysql_password,
        )

    def connect_trino(self):
        trino_dict = {
            "host": self.trino_host,
            "port": self.trino_port,
            "user": self.trino_user,
        }
        self.trino_lib.connect(trino_dict)

    def connect_spark(self):
        spark_dict = {
            "host": self.spark_host,
            "port": self.spark_port,
            "user": self.spark_user
        }
        self.spark_lib.connect(spark_dict)

    def connect_hive(self):
        hive_dict = {
            "host": self.hive_host,
            "port": self.hive_port,
            "user": self.hive_user
        }
        self.hive_lib.connect(hive_dict)

    def close_starrocks(self):
        self.mysql_lib.close()

    def close_trino(self):
        self.trino_lib.close()

    def close_spark(self):
        self.spark_lib.close()

    def close_hive(self):
        self.hive_lib.close()

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

    def execute_sql(self, sql, ori=False, conn=None):
        """execute query"""
        try:
            if conn is None:
                conn = self.mysql_lib.connector

            with conn.cursor() as cursor:
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
                    return {"status": True, "result": result, "msg": cursor._result.message, "desc": cursor.description}

                if isinstance(result, tuple) or isinstance(result, list):
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

    def get_now_db(self, conn=None):
        """execute query"""
        db = None
        try:
            if conn is None:
                conn = self.mysql_lib.connector

            with conn.cursor() as cursor:
                cursor.execute("SELECT DATABASE()")
                result = cursor.fetchall()
                db = result[0][0]

        except _mysql.Error as e:
            print("Get now db with _mysql.Error!", e.args)
        except Exception as e:
            print("Get now db with unknown error!", e)

        return db

    @timeout(
        QUERY_TIMEOUT,
        timeout_exception=AssertionError,
        exception_message=f"Query TimeoutException(TRINO/SPARK/HIVE): {QUERY_TIMEOUT}s!"
    )
    def conn_execute_sql(self, conn, sql):
        try:
            cursor = conn.cursor()
            if sql.endswith(";"):
                sql = sql[:-1]
            cursor.execute(sql)
            result = cursor.fetchall()

            for i in range(len(result)):
                row = [str(item) for item in result[i]]
                result[i] = "\t".join(row)

            return {"status": True, "result": "\n".join(result), "msg": "OK"}

        except trino.exceptions.TrinoQueryError as e:
            return {"status": False, "msg": e.message}
        except pyhive.exc.OperationalError as e:
            return {"status": False, "msg": e.args}
        except Exception as e:
            print("unknown error", e)
            raise

    def trino_execute_sql(self, sql):
        """trino execute query"""
        self.connect_trino()
        return self.conn_execute_sql(self.trino_lib.connector, sql)

    def spark_execute_sql(self, sql):
        """spark execute query"""
        self.connect_spark()
        return self.conn_execute_sql(self.spark_lib.connector, sql)

    def hive_execute_sql(self, sql):
        """hive execute query"""
        self.connect_hive()
        return self.conn_execute_sql(self.hive_lib.connector, sql)

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

    def treatment_record_res(self, sql, sql_res, res_container: list = None):

        res_container = res_container if res_container is not None else self.res_log

        if any(re.match(condition, sql) is not None for condition in skip.skip_res_cmd):
            return

        res_container.append(RESULT_FLAG)
        if not sql_res["status"]:
            res_container.append("E: %s" % str(sql_res["msg"]))
        else:
            # msg info no need to be checked
            sql_res = sql_res["result"] if "result" in sql_res else ""
            if isinstance(sql_res, tuple):
                if len(sql_res) > 0:
                    if isinstance(sql_res[0], tuple):
                        res_container.extend(["\t".join([str(y) for y in x]) for x in sql_res])
                    else:
                        res_container.extend(["\t".join(str(x)) for x in sql_res])
            elif isinstance(sql_res, bytes):
                if sql_res != b"":
                    res_container.append(str(sql_res).strip())
            elif sql_res is not None and str(sql_res).strip() != "":
                res_container.append(str(sql_res))
            else:
                log.info("SQL result: %s" % sql_res)

        res_container.append(RESULT_END_FLAT)

    def record_shell_res(self, shell_cmd, shell_res, record_container: list = None):
        """
        record shell res
        :param shell_cmd: command
        :param shell_res: shell result
        :param record_container: record container
        """

        record_container = record_container if record_container is not None else self.res_log

        if any(re.match(condition, shell_cmd) is not None for condition in skip.skip_res_cmd):
            return

        record_container.append(RESULT_FLAG)
        # return code
        record_container.append("%s" % shell_res[0])

        if shell_cmd.endswith("_stream_load"):
            self_print(shell_res[1])
            record_container.append(
                json.dumps(
                    {"Status": json.loads(shell_res[1])["Status"], "Message": json.loads(shell_res[1])["Message"]},
                    indent="    ",
                )
            )
        else:
            record_container.append("%s" % shell_res[1])

        record_container.append(RESULT_END_FLAT)

    def record_function_res(self, function, func_res, result_container: list = None):
        """
        record function res
        :param function: function and args
        :param func_res: function result
        :param result_container: result container
        """

        result_container = result_container if result_container is not None else self.res_log

        if any(re.match(condition, function) is not None for condition in skip.skip_res_cmd):
            return

        result_container.append(RESULT_FLAG)
        result_container.append("%s" % func_res)
        result_container.append(RESULT_END_FLAT)

    @staticmethod
    def execute_shell(shell_cmd: str):
        """execute shell"""
        shell_cmd = shell_cmd.replace("curl", "curl -sS")
        log.info("shell cmd: %s" % shell_cmd)

        cmd_res = subprocess.run(
            shell_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=120, shell=True
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

    def analyse_var(self, cmd, unfold=True, thread_key: str = None):
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

        match_words: list = re.compile("\\${([^}]*)}").findall(cmd)

        # replace thread variable dynamically
        if thread_key:
            tools.assert_in(thread_key, self.thread_var, f"Thread{thread_key} var dict is not found!")

            for each_word in copy.copy(match_words):
                # each_word type: xyz | xyz[*]...
                # each_word: db[0]  match_keywords: [ db ] each_keyword: db
                match_keywords: list = re.compile(r"^([a-zA-Z][a-zA-Z0-9_\-]*)").findall(cmd)
                tools.eq_(1, len(match_keywords), f"Var num in {match_keywords} != 1")
                each_keyword = match_keywords[0]

                # only replace the first keywords
                if each_keyword in self.thread_var[thread_key]:
                    each_keyword_value = each_word.replace(each_keyword,
                                                           f"self.thread_var[{thread_key}][{each_keyword}]")

                    if unfold:
                        each_keyword_value = str(eval(each_keyword_value))
                    else:
                        pass

                    cmd = cmd.replace(f"${{{each_word}}}", each_keyword_value)
                    log.info(f'Replace {each_keyword} → {each_keyword_value}, {cmd}')

                    match_words.remove(each_word)

        # replace variable dynamically, only replace right of '='
        for each_word in copy.copy(match_words):

            # each_word type: xyz | xyz[*]...
            # each_word: db[0]  match_keywords: [ db ] each_keyword: db
            match_keywords: list = re.compile(r"^([a-zA-Z][a-zA-Z0-9_\-]*)").findall(each_word)
            tools.eq_(1, len(match_keywords), f"Var num in {match_keywords} != 1")
            each_keyword = match_keywords[0]

            if each_keyword in self.__dict__:
                each_keyword_value = each_word.replace(each_keyword, f"self.{each_keyword}")

                if unfold:
                    each_keyword_value = str(eval(each_keyword_value))
                else:
                    pass

                cmd = cmd.replace("${%s}" % each_word, each_keyword_value)
                log.info(f'Replace {each_keyword} → {each_keyword_value}, {cmd}')

                match_words.remove(each_word)

        if len(match_words) > 0:
            error_msg = f"[{self.case_info.name}] Some vars not been resolved yet: %s" % ",".join(match_words)
            self_print(error_msg, color=ColorEnum.YELLOW, bold=True, logout=True)

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
                    log.warning("converse array error: %s, %s" % (res, e))

        return res, res_for_log

    def custom_except_hook(self, args: threading.ExceptHookArgs):

        # console exception
        trace_stack = f'{"*" * 50} {args.thread.name} {"*" * 50}\n'
        for ex in traceback.TracebackException(type(args.exc_type), args.exc_value, args.exc_traceback).format():
            trace_stack += str(ex)

        # print(trace_stack, file=sys.stderr, end="")
        self.thread_res[args.thread.name] = trace_stack
        # tools.ok_(False, "Thread exit with exception!")

    def execute_thread(self, exec_id, cmd_list, res_list, ori_cmd_list, record_mode, init_db, conn):

        this_res = []
        self.thread_var[exec_id] = {}
        _t_info = exec_id[:str(exec_id).rindex("-")]
        _t_exec_id = exec_id.split("-")[-1]

        if init_db:
            self_print(f"Init conn's db: {init_db}", color=ColorEnum.CYAN, logout=True)
            self.execute_sql("use %s" % init_db, conn=conn)

        for _cmd_id, _each_cmd in enumerate(cmd_list):
            uncheck = False
            _cmd_id_str = f'Thread-{exec_id}.{_cmd_id}'
            this_res.append(ori_cmd_list[_cmd_id])
            prefix_s_count = len(ori_cmd_list[_cmd_id]) - len(ori_cmd_list[_cmd_id].lstrip())

            # uncheck flag, owns the highest priority
            if _each_cmd.startswith(UNCHECK_FLAG):
                uncheck = True
                _each_cmd = _each_cmd[len(UNCHECK_FLAG):]

            old_this_res_len = len(this_res)
            actual_res, actual_res_log, var, order = self.execute_single_statement(_each_cmd, _cmd_id_str, record_mode,
                                                                                   this_res, var_key=exec_id, conn=conn)

            if record_mode:
                for new_res_lino in range(old_this_res_len, len(this_res)):
                    # add prefix ' '
                    _old_res = this_res[new_res_lino]
                    if "\n" in _old_res and _old_res.startswith("{") and _old_res.endswith("}"):
                        try:
                            json.loads(_old_res)
                            _tmp_old_json = _old_res.split("\n")
                            for json_line_id in range(len(_tmp_old_json)):
                                _tmp_old_json[json_line_id] = " " * prefix_s_count + _tmp_old_json[json_line_id]
                            this_res[new_res_lino] = "\n".join(_tmp_old_json)
                            continue
                        except Exception:
                            pass
                    this_res[new_res_lino] = " " * prefix_s_count + _old_res

            if not record_mode and not uncheck:
                # check mode only work in validating mode
                # pretreatment expect res
                expect_res = res_list[_cmd_id]
                expect_res_for_log = expect_res if len(expect_res) < 1000 else expect_res[:1000] + "..."

                log.info(f"""[{_cmd_id_str}.result]: \n\t- [exp]: {expect_res_for_log}\n\t- [act]: {actual_res}""")

                # -------------------------------------------
                #               [CHECKER]
                # -------------------------------------------
                self.check(_cmd_id_str, _each_cmd, expect_res, actual_res, order, ori_sql=cmd_list[_cmd_id].lstrip())

        if record_mode and len(this_res) > 0:
            self.thread_res_log.setdefault(_t_info, [])
            self.thread_res_log[_t_info].append(this_res)

    def execute_single_statement(self, statement, sql_id, record_mode, res_container: list = None, var_key: str = None, conn: any = None):
        """
        execute single statement and return result
        """

        order = False
        res_container = res_container if res_container is not None else self.res_log

        if statement.startswith(TRINO_FLAG):
            statement = statement[len(TRINO_FLAG):]
            # analyse var set
            var, statement = self.analyse_var(statement, thread_key=var_key)

            self_print("[TRINO]: %s" % statement)
            log.info("[%s] TRINO: %s" % (sql_id, statement))
            actual_res = self.trino_execute_sql(statement)

            if record_mode:
                self.treatment_record_res(statement, actual_res, res_container)

            actual_res = actual_res["result"] if actual_res["status"] else "E: %s" % str(actual_res["msg"])

            # pretreatment actual res
            actual_res, actual_res_log = self.pretreatment_res(actual_res)

        elif statement.startswith(SPARK_FLAG):
            statement = statement[len(SPARK_FLAG):]
            # analyse var set
            var, statement = self.analyse_var(statement, thread_key=var_key)

            self_print("[SPARK]: %s" % statement)
            log.info("[%s] SPARK: %s" % (sql_id, statement))
            actual_res = self.spark_execute_sql(statement)

            if record_mode:
                self.treatment_record_res(statement, actual_res, res_container)

            actual_res = actual_res["result"] if actual_res["status"] else "E: %s" % str(actual_res["msg"])

            # pretreatment actual res
            actual_res, actual_res_log = self.pretreatment_res(actual_res)

        elif statement.startswith(HIVE_FLAG):
            statement = statement[len(HIVE_FLAG):]
            # analyse var set
            var, statement = self.analyse_var(statement, thread_key=var_key)

            self_print("[HIVE]: %s" % statement)
            log.info("[%s] HIVE: %s" % (sql_id, statement))
            actual_res = self.hive_execute_sql(statement)

            if record_mode:
                self.treatment_record_res(statement, actual_res, res_container)

            actual_res = actual_res["result"] if actual_res["status"] else "E: %s" % str(actual_res["msg"])

            # pretreatment actual res
            actual_res, actual_res_log = self.pretreatment_res(actual_res)

        # execute command in files
        elif statement.startswith(SHELL_FLAG):
            statement = statement[len(SHELL_FLAG):]

            # analyse var set
            var, statement = self.analyse_var(statement, thread_key=var_key)
            self_print("[SHELL]: %s" % statement)
            log.info("[%s] SHELL: %s" % (sql_id, statement))
            actual_res = self.execute_shell(statement)

            if record_mode:
                self.record_shell_res(statement, actual_res, res_container)

            actual_res_log = ""

        elif statement.startswith(FUNCTION_FLAG):
            # function invoke
            sql = statement[len(FUNCTION_FLAG):]

            # analyse var set
            var, sql = self.analyse_var(sql, thread_key=var_key)
            self_print("[FUNCTION]: %s" % sql)
            log.info("[%s] FUNCTION: %s" % (sql_id, sql))
            actual_res = eval("self.%s" % sql)

            if record_mode:
                self.record_function_res(sql, actual_res, res_container)

            actual_res_log = ""
        else:
            # sql
            log.info("[%s] SQL: %s" % (sql_id, statement))

            # order flag
            if statement.startswith(ORDER_FLAG):
                order = True
                statement = statement[len(ORDER_FLAG):]

            # analyse var set
            var, statement = self.analyse_var(statement, thread_key=var_key)

            actual_res = self.execute_sql(statement, conn=conn)
            self_print(statement)

            if record_mode:
                self.treatment_record_res(statement, actual_res, res_container)

            actual_res = actual_res["result"] if actual_res["status"] else "E: %s" % str(actual_res["msg"])

            # pretreatment actual res
            actual_res, actual_res_log = self.pretreatment_res(actual_res)

        # set variable dynamically
        if var:
            if var_key is None:
                _tmp_log = f"case attr: {var} → {actual_res}"
                log.info(f"Update {_tmp_log}" if var in self.__dict__ else f"New {_tmp_log}")
                self.__setattr__(var, actual_res)
            else:
                # thread variable
                _tmp_log = f"thread({var_key}) attr: {var} → {actual_res}"
                log.info(f"Update {_tmp_log}" if var in self.thread_var[var_key] else f"New {_tmp_log}")

                self.thread_var[var_key][var] = actual_res

        return actual_res, actual_res_log, var, order

    def execute_loop_statement(self, _loop_stat_list, _loop_id, _timeout, _interval):
        """
        execute loop statement
        """
        loop_begin_time = int(time.time())
        retry_count = 0
        loop_check_res = False
        while (time.time() - loop_begin_time) < _timeout:

            self_print(f"→ ROUND: {retry_count}...", color=ColorEnum.MAGENTA, logout=True)
            retry_count += 1

            for each_stat_id, each_statement in enumerate(_loop_stat_list):

                sql_id_str = f"{_loop_id}-{each_stat_id}"

                # uncheck flag, owns the highest priority
                if each_statement.startswith(UNCHECK_FLAG):
                    tools.ok_(False, f"Loop statement does not support UNCHECK flag!")

                # check statement
                if each_statement.startswith(CHECK_FLAG):
                    each_statement = each_statement[len(CHECK_FLAG):].strip()

                    # analyse var set
                    var, each_statement_new = self.analyse_var(each_statement, unfold=False)

                    try:
                        check_result = eval(each_statement_new)
                    except Exception as e:
                        self_print(f"[LOOP] Exception: {each_statement}, {e}!", ColorEnum.YELLOW, logout=True)
                        for self_k in re.findall(r'self.([0-9a-zA-Z_-]+)', each_statement_new):
                            self_print(
                                f"    ▶ {self_k}: %s" % eval(f'self.{self_k}') if self_k in self.__dict__ else "")
                        loop_check_res = False
                        break

                    if check_result is True:
                        self_print(f"[LOOP CHECK] SUCCESS: {each_statement}!", color=ColorEnum.GREEN, logout=True)
                        loop_check_res = True
                    else:
                        self_print(f"[LOOP CHECK] FAILURE: {each_statement}, result: {check_result}!",
                                   color=ColorEnum.YELLOW, logout=True)
                        # print variables in each_statement_new
                        for self_k in re.findall(r'self.([0-9a-zA-Z_-]+)', each_statement_new):
                            self_print(
                                f"    ▶ {self_k}: %s" % eval(f'self.{self_k}') if self_k in self.__dict__ else "")
                        loop_check_res = False
                        break

                else:
                    # normal statement, loop statement no need to record the result
                    actual_res, actual_res_log, var, _ = self.execute_single_statement(each_statement, each_stat_id,
                                                                                       False)
                    log.info("[%s.result]: %s" % (sql_id_str, actual_res))

            if loop_check_res:
                break

            time.sleep(_interval)

        return loop_check_res

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

        tmp_ori_sql = ori_sql[len(UNCHECK_FLAG):] if ori_sql.startswith(UNCHECK_FLAG) else ori_sql
        if tmp_ori_sql.startswith(SHELL_FLAG):
            tools.assert_equal(int(exp.split("\n")[0]), act[0],
                               f"[SHELL ERROR]\n\t- cmd : {sql}\n\t- code: {act[0]}\n\t- msg : {act[1]}")

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
                    tools.assert_true(
                        re.match(exp_std, act_std, flags=re.S),
                        "shell result str|re not match,\n[exp]: %s,\n [act]: %s" % (exp_std, act_std),
                    )
                except Exception as e:
                    log.warning("Try to treat res as regex, failed!\n:%s" % e)

                tools.assert_true(False, "shell result str|re not match,\n[exp]: %s,\n[act]: %s" % (exp_std, act_std))

            else:
                # str
                if exp_std != act_std and not re.match(exp_std, act_std, flags=re.S):
                    tools.assert_true(False, "shell result str not match,\n[exp]: %s\n[act]: %s" % (exp_std, act_std))

        elif tmp_ori_sql.startswith(FUNCTION_FLAG):
            # only support str result
            tools.assert_equal(str(exp), str(act))
        else:
            if exp.startswith(REGEX_FLAG):
                log.info("[check regex]: %s" % exp[len(REGEX_FLAG):])
                tools.assert_regexp_matches(
                    r"%s" % str(act),
                    exp[len(REGEX_FLAG):],
                    "sql result not match regex:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                    % (self_print(sql, need_print=False), exp[len(REGEX_FLAG):], act),
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
                            log.warning("converse array error: %s, %s" % (exp, e))
                            expect_res = str(exp)

                    tools.assert_equal(type(expect_res), type(act), "exp and act results' type not match")

                    if order:
                        tools.assert_list_equal(
                            expect_res,
                            act,
                            "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                            % (self_print(sql, need_print=False), expect_res, act),
                        )
                    else:
                        tools.assert_count_equal(
                            expect_res,
                            act,
                            "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                            % (self_print(sql, need_print=False), expect_res, act),
                        )
                    return
                elif exp.startswith("{") and exp.endswith("}"):
                    log.info("[check type]: DICT")
                    tools.assert_equal(type(exp), type(act), "exp and act results' type not match")
                    # list result
                    tools.assert_dict_equal(
                        json.loads(exp),
                        json.loads(act),
                        "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                        % (self_print(sql, need_print=False), exp, act),
                    )
                    return
            except Exception as e:
                log.warning("analyse result before check error, %s" % e)

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
                    "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                    % (self_print(sql, need_print=False), exp, act),
                )
            else:
                tools.assert_count_equal(
                    exp,
                    act,
                    "sql result not match:\n- [SQL]: %s\n- [exp]: %s\n- [act]: %s\n---"
                    % (self_print(sql, need_print=False), exp, act),
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
        try:
            self.create_database_and_table(T_R_DB, T_R_TABLE, "sql_framework", True)
        except AssertionError:
            self_print(f"Failed to create DB/Table to save the results, please check the env!", ColorEnum.RED)
            sys.exit(1)

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
            new_log = new_log[len(current_log):]

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
        load_state = ""
        while times < time_out:
            result = self.execute_sql('show load where label = "' + label + '"', True)
            log.info('show load where label = "' + label + '"')
            log.info(result)
            if len(result["result"]) > 0:
                load_state = result["result"][0][2]
                log.info(load_state)
                if load_state == "CANCELLED":
                    log.info(result)
                    break
                elif load_state == "FINISHED":
                    log.info(result)
                    break
            time.sleep(1)
            times += 1
        tools.assert_true(load_state in ("FINISHED", "CANCELLED"), "wait load finish error, timeout 300s")

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

    def wait_materialized_view_cancel(self, check_count=60):
        """
        wait materialized view job cancel and return status
        """
        status = ""
        show_sql = "SHOW ALTER MATERIALIZED VIEW"
        count = 0
        while count < check_count:
            res = self.execute_sql(show_sql, True)
            status = res["result"][-1][8]
            if status != "CANCELLED":
                time.sleep(1)
            else:
                # sleep another 5s to avoid FE's async action.
                time.sleep(1)
                break
            count += 1
        tools.assert_equal("CANCELLED", status, "wait alter table cancel error")

    def wait_async_materialized_view_finish(self, current_db, mv_name, check_count=None):
        """
        wait async materialized view job finish and return status
        """

        # show materialized veiws result
        def is_all_finished1():
            sql = "SHOW MATERIALIZED VIEWS WHERE database_name='{}' AND NAME='{}'".format(current_db, mv_name)
            print(sql)
            result = self.execute_sql(sql, True)
            if not result["status"]:
                tools.assert_true(False, "show mv state error")
            results = result["result"]
            for _res in results:
                last_refresh_state = _res[12]
                if last_refresh_state != "SUCCESS" and last_refresh_state != "MERGED":
                    return False
            return True

        def is_all_finished2():
            sql = f"""select STATE from information_schema.task_runs a
                join information_schema.materialized_views b 
                on a.task_name=b.task_name 
                where b.table_name='{mv_name}' 
                    and a.`database`='{current_db}'
            """
            self_print(sql)
            result = self.execute_sql(sql, True)
            if not result["status"]:
                tools.assert_true(False, "show mv state error")
            results = result["result"]
            for _res in results:
                if _res[0] != "SUCCESS" and _res[0] != "MERGED":
                    return False
            return True

        # information_schema.task_runs result
        def get_success_count(results):
            cnt = 0
            for _res in results:
                if _res[0] == "SUCCESS" or _res[0] == "MERGED":
                    cnt += 1
            return cnt

        max_loop_count = 180
        is_all_ok = False
        count = 0
        if check_count is None:
            while count < max_loop_count:
                is_all_ok = is_all_finished1() and is_all_finished2()
                if is_all_ok:
                    time.sleep(1)
                    break
                time.sleep(1)
                count += 1
        else:
            show_sql = f"""select STATE from information_schema.task_runs a 
                join information_schema.materialized_views b 
                on a.task_name=b.task_name 
                where b.table_name='{mv_name}' 
                    and a.`database`='{current_db}'"""
            while count < max_loop_count:
                print(show_sql)
                res = self.execute_sql(show_sql, True)
                if not res["status"]:
                    tools.assert_true(False, "show mv state error")

                success_cnt = get_success_count(res["result"])
                if success_cnt >= check_count:
                    is_all_ok = True
                    # sleep to avoid FE's async action.
                    time.sleep(1)
                    break
                time.sleep(1)
                count += 1
        tools.assert_equal(True, is_all_ok, "wait async materialized view finish error")

    def wait_mv_refresh_count(self, db_name, mv_name, expect_count):
        show_sql = """select count(*) from information_schema.materialized_views 
            join information_schema.task_runs using(task_name)
            where table_schema='{}' and table_name='{}' and (state = 'SUCCESS' or state = 'MERGED')
        """.format(
            db_name, mv_name
        )
        print(show_sql)

        cnt = 1
        refresh_count = 0
        while cnt < 60:
            res = self.execute_sql(show_sql, True)
            print(res)
            refresh_count = res["result"][0][0]
            if refresh_count >= expect_count:
                return
            else:
                print("current refresh count is {}, expect is {}".format(refresh_count, expect_count))
                time.sleep(1)
            cnt += 1

        tools.assert_equal(expect_count, refresh_count, "wait too long for the refresh count")

    def wait_for_pipe_finish(self, db_name, pipe_name, check_count=60):
        """
        wait pipe load finish
        """
        state = ""
        show_sql = """select state, load_status, last_error 
            from information_schema.pipes 
            where database_name='{}' and pipe_name='{}'
        """.format(
            db_name, pipe_name
        )
        count = 0
        print("waiting for pipe {}.{} finish".format(db_name, pipe_name))
        while count < check_count:
            res = self.execute_sql(show_sql, True)
            print(res)
            state = res["result"][0][0]
            if state == "RUNNING":
                print("pipe state is " + state)
                time.sleep(1)
            else:
                break
            count += 1
        tools.assert_equal("FINISHED", state, "didn't wait for the pipe to finish")

    @staticmethod
    def check_hit_materialized_view_plan(res, mv_name):
        """
        assert mv_name is hit in query
        """
        tools.assert_true(str(res).find(mv_name) > 0, "assert mv %s is not found" % mv_name)

    def check_hit_materialized_view(self, query, *expects):
        """
        assert mv_name is hit in query
        """
        time.sleep(1)
        sql = "explain %s" % (query)
        res = self.execute_sql(sql, True)
        if not res["status"]:
            print(res)
        tools.assert_true(res["status"])
        plan = str(res["result"])
        for expect in expects:
            tools.assert_true(plan.find(expect) > 0, "assert expect %s is not found in plan" % (expect))

    def print_hit_materialized_view(self, query, *expects) -> bool:
        """
        assert mv_name is hit in query
        """
        time.sleep(1)
        sql = "explain %s" % (query)
        res = self.execute_sql(sql, True)
        if not res["status"]:
            print(res)
            return False
        plan = str(res["result"])
        for expect in expects:
            if plan.find(expect) > 0:
                return True
        return False

    def assert_equal_result(self, *sqls):
        if len(sqls) < 2:
            return

        res_list = []
        # could be faster if making this loop parallel
        for sql in sqls:
            if sql.startswith(TRINO_FLAG):
                sql = sql[len(TRINO_FLAG):]
                res = self.trino_execute_sql(sql)
            elif sql.startswith(SPARK_FLAG):
                sql = sql[len(SPARK_FLAG):]
                res = self.spark_execute_sql(sql)
            elif sql.startswith(HIVE_FLAG):
                sql = sql[len(HIVE_FLAG):]
                res = self.hive_execute_sql(sql)
            else:
                res = self.execute_sql(sql)

            tools.assert_true(res["status"])
            res_list.append(res["result"])

        # assert equal result
        for i in range(1, len(res_list)):
            tools.assert_equal(res_list[0], res_list[i])

    def check_no_hit_materialized_view(self, query, *expects):
        """
        assert mv_name is hit in query
        """
        time.sleep(1)
        sql = "explain %s" % (query)
        res = self.execute_sql(sql, True)
        if not res["status"]:
            print(res)
        tools.assert_true(res["status"])
        for expect in expects:
            tools.assert_false(str(res["result"]).find(expect) > 0, "assert expect %s should not be found" % (expect))

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

    def try_collect_dict_N_times(self, column_name, table_name, N):
        """
        try to collect dictionary for N times
        """
        for i in range(N):
            sql = "explain costs select distinct %s from %s" % (column_name, table_name)
            res = self.execute_sql(sql, True)
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

    def assert_never_collect_dicts(self, column_name, table_name, db_name):
        """
        assert table_name:column_name has global dict
        """
        sql = """
admin execute on frontend '
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.catalog.ColumnId;

var tid = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(\"{db}\").getTable(\"{tb}\").getId();
var dictMgr = CacheDictManager.getInstance();
var columnId = new ColumnId("{col}");
var cid = new ColumnIdentifier(tid, columnId)

out.append("${{dictMgr.NO_DICT_STRING_COLUMNS.contains(cid)}}")
';      """.format(
            db=db_name, tb=table_name, col=column_name
        )
        res = self.execute_sql(sql, True)
        print("scirpt output:" + str(res))
        tools.assert_true(str(res["result"][0][0]).strip() == "true", "column still could collect dictionary")

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
            timeout=120,
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
        """load data"""
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

    def post_http_request(self, exec_url) -> str:
        """Sends a POST request.

        Returns:
            the response content.
        """
        res = requests.post(exec_url, auth=HTTPBasicAuth(self.mysql_user, self.mysql_password))
        tools.assert_equal(200, res.status_code, f"failed to post http request [res={res}] [url={exec_url}]")
        return res.content.decode("utf-8")

    def manual_compact(self, database_name, table_name):
        sql = "show tablet from " + database_name + "." + table_name
        res = self.execute_sql(sql, True)
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
        res = self.execute_sql(analyze_sql, True)
        while timeout > 0:
            res = self.execute_sql(analyze_sql, True)
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
            res = self.execute_sql(sql, True)
            tools.assert_true(res["status"], res["msg"])
            if str(res["result"]).find("Decode") > 0:
                finished = True
                break
            time.sleep(5)
            if counter > 10:
                break
            counter = counter + 1

        tools.assert_true(finished, "analyze timeout")

    def wait_compaction_finish(self, table_name: str, expected_num_segments: int):
        timeout = 300
        scan_table_sql = (
            f"SELECT /*+SET_VAR(enable_profile=true,enable_async_profile=false)*/ COUNT(1) FROM {table_name}"
        )
        fetch_segments_sql = r"""
            with profile as (
                select unnest as line from (values(1))t(v) join unnest(split(get_query_profile(last_query_id()), "\n"))
            )
            select regexp_extract(line, ".*- SegmentsReadCount: (?:.*\\()?(\\d+)\\)?", 1) as value 
            from profile 
            where line like "%- SegmentsReadCount%"
        """

        while timeout > 0:
            res = self.execute_sql(scan_table_sql)
            tools.assert_true(res["status"], f'Fail to execute scan_table_sql, error=[{res["msg"]}]')

            res = self.execute_sql(fetch_segments_sql)
            tools.assert_true(res["status"], f'Fail to execute fetch_segments_sql, error=[{res["msg"]}]')

            if res["result"] == str(expected_num_segments):
                break

            time.sleep(1)
            timeout -= 1
        else:
            tools.assert_true(False, "wait compaction timeout")

    def _get_backend_http_endpoints(self) -> List[Dict]:
        """Get the http host and port of all the backends.

        Returns:
            a dict list, each of which contains the key "host" and "host" of a backend.
        """
        res = self.execute_sql("show backends;", ori=True)
        tools.assert_true(res["status"], res["msg"])

        backends = []
        for row in res["result"]:
            backends.append(
                {
                    "host": row[1],
                    "port": row[4],
                }
            )

        return backends

    def update_be_config(self, key, value):
        """Update the config to all the backends."""
        backends = self._get_backend_http_endpoints()
        for backend in backends:
            exec_url = f"http://{backend['host']}:{backend['port']}/api/update_config?{key}={value}"
            print(f"post {exec_url}")
            res = self.post_http_request(exec_url)

            res_json = json.loads(res)
            tools.assert_dict_contains_subset(
                {"status": "OK"}, res_json, f"failed to update be config [response={res}] [url={exec_url}]"
            )

    def get_resource_group_id(self, name):
        res = self.execute_sql(f"show resource group {name};", ori=True)
        tools.assert_true(res["status"], res["msg"])
        return res["result"][0][1]

    def get_backend_cpu_cores(self):
        res = self.execute_sql("show backends;", ori=True)
        tools.assert_true(res["status"], res["msg"])
        for i, col_info in enumerate(res["desc"]):
            if col_info[0] == "CpuCores":
                return res["result"][0][i]

        tools.assert_true(False, f"failed to get backend cpu cores [res={res}]")

    def assert_table_cardinality(self, sql, rows):
        """
        assert table with an expected row counts
        """
        res = self.execute_sql(sql, True)
        expect = r"cardinality=" + rows
        match = re.search(expect, str(res["result"]))
        print(expect)
        tools.assert_true(match, "expected cardinality: " + rows + ". but found: " + str(res["result"]))

    def wait_refresh_dictionary_finish(self, name, check_status):
        """
        wait dictionary refresh job finish and return status
        """
        status = ""
        while True:
            res = self.execute_sql(
                "SHOW DICTIONARY %s" % name,
                True,
            )

            status = res["result"][0][7]
            if status != "REFRESHING" and status != "COMMITTING":
                break
            time.sleep(0.5)
        tools.assert_equal(check_status, status, "wait refresh dictionary finish error")

    def set_first_tablet_bad_and_recover(self, table_name):
        """
        set table first tablet as bad replica and recover until success
        """
        res = self.execute_sql(
            "SHOW TABLET FROM %s" % table_name,
            True,
        )

        tablet_id = res["result"][0][0]
        backend_id = res["result"][0][2]

        res = self.execute_sql(
            "ADMIN SET REPLICA STATUS PROPERTIES('tablet_id' = '%s', 'backend_id' = '%s', 'status' = 'bad')"
            % (tablet_id, backend_id),
            True,
        )

        time.sleep(20)

        while True:
            res = self.execute_sql(
                "SHOW TABLET FROM %s" % table_name,
                True,
            )

            if len(res["result"]) != 2:
                time.sleep(0.5)
            else:
                break

    def assert_explain_contains(self, query, *expects):
        """
        assert explain result contains expect string
        """
        sql = "explain %s" % query
        res = self.execute_sql(sql, True)
        for expect in expects:
            tools.assert_true(
                str(res["result"]).find(expect) > 0,
                "assert expect {} is not found in plan {}".format(expect, res["result"]),
            )

    def assert_explain_not_contains(self, query, *expects):
        """
        assert explain result contains expect string
        """
        sql = "explain %s" % query
        res = self.execute_sql(sql, True)
        for expect in expects:
            tools.assert_true(str(res["result"]).find(expect) == -1, "assert expect %s is found in plan" % (expect))

    def assert_explain_verbose_contains(self, query, *expects):
        """
        assert explain verbose result contains expect string
        """
        sql = "explain verbose %s" % (query)
        res = self.execute_sql(sql, True)
        tools.assert_true(res["status"], res['msg'])
        for expect in expects:
            plan_string = "\n".join(item[0] for item in res["result"])
            tools.assert_true(plan_string.find(expect) > 0, "assert expect %s is not found in plan: %s" % (expect, plan_string))

    def assert_explain_costs_contains(self, query, *expects):
        """
        assert explain costs result contains expect string
        """
        sql = "explain costs %s" % query
        res = self.execute_sql(sql, True)
        for expect in expects:
            tools.assert_true(str(res["result"]).find(expect) > 0, "assert expect %s is not found in plan" % (expect))

    def assert_trace_values_contains(self, query, *expects):
        """
        assert trace values result contains expect string
        """
        sql = "trace values %s" % query
        res = self.execute_sql(sql, True)
        for expect in expects:
            tools.assert_true(
                str(res["result"]).find(expect) > 0,
                "assert expect %s is not found in plan, error msg is %s" % (expect, str(res["result"])),
            )

    def assert_prepare_execute(self, db, query, params=()):
        conn = mysql.connector.connect(
            host=self.mysql_host, user=self.mysql_user, password="", port=self.mysql_port, database=db
        )
        cursor = conn.cursor(prepared=True)

        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            cursor.fetchall()
        except mysql.connector.Error as e:
            tools.assert_true(1 == 0, e)

        finally:
            cursor.close()
            conn.close()

    def assert_trace_times_contains(self, query, *expects):
        """
        assert trace times result contains expect string
        """
        sql = "trace times %s" % query
        res = self.execute_sql(sql, True)
        for expect in expects:
            tools.assert_true(
                str(res["result"]).find(expect) > 0,
                "assert expect %s is not found in plan, error msg is %s" % (expect, str(res["result"])),
            )

    def assert_clear_stale_stats(self, query, expect_num):
        timeout = 300
        num = 0
        while timeout > 0:
            res = self.execute_sql(query)
            num = res["result"]
            if int(num) < expect_num:
                break
            time.sleep(10)
            timeout -= 10
        else:
            tools.assert_true(False, "clear stale column stats timeout. The number of stale column stats is %s" % num)

    def assert_table_partitions_num(self, table_name, expect_num):
        res = self.execute_sql("SHOW PARTITIONS FROM %s" % table_name, True)
        tools.assert_true(res["status"], "show schema change task error")
        ans = res["result"]
        tools.assert_true(len(ans) == expect_num, "The number of partitions is %s" % len(ans))

    def wait_table_rowcount_not_empty(self, table, max_times=300):
        times = 0
        rc = 0
        sql = 'show partitions from ' + table
        while times < max_times:
            result = self.execute_sql(sql, True)
            if len(result["result"]) > 0:
                rc = int(result["result"][0][-4])
                log.info(rc)
                if rc > 0:
                    break
            time.sleep(1)
            times += 1
        tools.assert_true(True, "wait row count > 0 error, max_times:" + str(max_times))

    def assert_cache_select_is_success(self, query):
        """
        Check cache select is success, make sure that read_cache_size + write_cache_size > 0
        """
        res = self.execute_sql(query, True, )
        result = res["result"][0]
        # remove unit
        read_cache_size = int(result[0].replace("B", "").replace("KB", ""))
        write_cache_size = int(result[1].replace("B", "").replace("KB", ""))
        tools.assert_true(read_cache_size + write_cache_size > 0, "cache select is failed, read_cache_size + write_cache_size must larger than 0 bytes")

    @staticmethod
    def regex_match(check_str: str, pattern: str):
        if re.fullmatch(pattern, check_str):
            return True

        return False
