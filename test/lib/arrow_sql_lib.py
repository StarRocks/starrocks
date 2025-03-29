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
arrow_sql_lib.py

@Time : 2024/11/13
@Author : liubotao
"""

import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql


class ArrowSqlLib(object):
    """ArrowSqlLib class"""

    def __init__(self):
        self.connector = None

    def connect(self, query_dict):
        self.connector = flight_sql.connect(uri=f"grpc://{query_dict['host']}:{int(query_dict['arrow_port'])}",
                                            db_kwargs={
                                                adbc_driver_manager.DatabaseOptions.USERNAME.value: query_dict["user"],
                                                adbc_driver_manager.DatabaseOptions.PASSWORD.value: query_dict[
                                                    "password"],
                                            })

    def close(self):
        if self.connector:
            self.connector.close()
            self.connector = None
