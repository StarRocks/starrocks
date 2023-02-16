# !/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
#
# Copyright (c) 2020, Dingshi Inc.
# All rights reserved.
#
###########################################################################

"""
sql_annotation
@Time : 2022/11/2 13:43
@Author : Brook.Ye
"""
from functools import wraps

from cup import log
from nose import tools


def init(record_mode=False):
    """init"""

    def receive(func):
        """init decorator"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            """wrapper"""
            name = args[1].name
            args[0].db = list(args[1].db)
            args[0].case_info = args[1]
            args[0].resource = args[1].resource

            log.info(
                """
*********************************************
Start to run: %s
*********************************************"""
                % name
            )
            # record mode, init info
            if record_mode:
                args[0].res_log.append(args[1].info)

            # drop database
            for each_db in args[0].db:
                # drop database in init
                log.info("init drop db: %s" % each_db)
                args[0].drop_database(each_db)

            # drop resource
            for each_resource in args[0].resource:
                # drop resource in init
                log.info("init drop resource: %s" % each_resource)
                args[0].drop_resource(each_resource)

            for each_cmd in args[1].init_cmd:
                log.info("[INIT] SQL: %s" % each_cmd)
                res = args[0].execute_sql(each_cmd)
                tools.assert_true(res["status"], "init cmd execute error, %s" % res)

            return func(*args, **kwargs)

        return wrapper

    return receive
