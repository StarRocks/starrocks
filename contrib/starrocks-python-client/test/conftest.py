#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from sqlalchemy.dialects import registry


registry.register("starrocks+pymysql", "starrocks.dialect", "StarRocksDialect")
registry.register("starrocks", "starrocks.dialect", "StarRocksDialect")

# It's just used to suppress a spurious warning from pytest.
pytest.register_assert_rewrite("sqlalchemy.testing.assertions")


# from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402,F403,I001

# Load StarRocks-specific test helpers and fixtures
pytest_plugins = ("test.conftest_sr",)
