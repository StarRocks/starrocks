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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union, Optional

from dbutils.pooled_db import PooledDB


@dataclass
class SQLRawResult:
    status: bool
    msg: str
    result: Optional[Union[list, tuple]] = None
    desc: Optional[str] = None

class BaseConnectionLib(ABC):
    @abstractmethod
    def connect(self, query_dict):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def execute(self, sql) -> SQLRawResult:
        pass

    @abstractmethod
    def wrapper(self, conn):
        pass

    def create_pool(self, host: str, mysql_port: int, arrow_port: int, user: str, password: str) -> PooledDB:
        pass