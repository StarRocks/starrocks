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
            args[0].db = args[1].db
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

            return func(*args, **kwargs)

        return wrapper

    return receive
