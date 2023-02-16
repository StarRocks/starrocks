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
skip
@Time : 2022/11/2 17:11
@Author : Brook.Ye
"""

skip_res_cmd = [
    "show backends",
    "show backends;",
    "select connection_id()",
    "select connection_id();",
    ".*explain costs select.*",
    "EXPLAIN SELECT.*",
    "rand()",
    "show stats meta",
    "SHOW RESOURCES",
    "show alter table column",
    "select db_id, table_id, column_name,.* from _statistics_.column_statistics.*",
    "SELECT \\* FROM .* LIMIT 1.*",
    "explain .*",
    "select \\* from t2 where c1 = \\(select c1 from t2 limit 1\\).*",
    "SHOW ALTER TABLE COLUMN ORDER BY CreateTime DESC LIMIT 1.*",
    "show load.*",
    "SELECT `v2` FROM `test_except_with_only_one_tablet`.*",
    "SHOW PARTITIONS.*",
    "SHOW REPLICA",
    "show routine load",
    "SELECT DISTINCT k1 FROM aggregate_tbl LIMIT 1",
    "SELECT DISTINCT k1 FROM aggregate_par_tbl LIMIT 1",
    "select.* db_id.*",
    "select.* table_id.*",
    "SELECT * FROM unnest_es_external_table limit 1",
    "select last_query_id",
    "select uuid",
    "select unix_timestamp",
    "select utc_timestamp",
    "select now\\(\\)",
    "select current_timestamp",
    "select localtime",
    "select curtime\\(\\)",
    "select current_time\\(\\)",
    "select curdate\\(\\)",
    "select current_date\\(\\)",
]
