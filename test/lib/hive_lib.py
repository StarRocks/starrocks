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

from pyhive import hive
from cup import log

from lib import close_conn


class HiveLib(object):
    """HiveLib class"""

    def __init__(self):
        self.connector = None

    def connect(self, query_dict):
        if self.connector is None:
            self.connector = hive.Connection(
                host=query_dict["host"],
                port=query_dict["port"],
                username=query_dict["user"]
            )

    def close(self):
        if self.connector is not None:
            close_conn(self.connector, "hive")
            self.connector = None
