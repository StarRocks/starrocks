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
data_insert_lib
@Time : 2022/11/14 16:11
@Author : Brook.Ye
"""


class DataInsertLib(object):
    """handle data inserting"""

    def __init__(self):
        self.sql = ""
        self.args = {}

    def set_table_schema(self, args_dict):
        self.args = args_dict

    def get_insert_schema(self):
        """return insert sql"""
        sql = "insert into "
        if "database_name" in self.args:
            sql += "%s." % (self.args["database_name"])
        if "table_name" in self.args:
            sql += "%s " % (self.args["table_name"])
        if "partition_desc" in self.args:
            sql += "partition (%s)" % (", ".join(self.args["partition_desc"]["partition_list"]))
        if "with_label" in self.args:
            sql += "WITH LABEL %s " % (self.args["with_label"])
        if "columns" in self.args:
            sql += "(%s)" % (", ".join(self.args["columns"]))
        if "values" in self.args:
            values = "values "
            for v_one in self.args["values"]:
                values += "("
                for v in v_one:
                    if isinstance(v, tuple):  # use tuple indicate expression
                        if v[0] == "default":
                            values += "default, "
                        else:
                            values += "%s, " % v[0]
                    elif isinstance(v, str):
                        values += "'" + str(v) + "', "
                    elif v is None:
                        values += "null, "
                    else:
                        values += str(v) + ", "
                values = values[:-2] + "), "
            sql += values
            sql = sql[:-2]
        if "query" in self.args:
            sql += self.args["query"]

        sql += ";"
        return sql
