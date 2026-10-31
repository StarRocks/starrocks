# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The conftest guard that fails collection for tests the SQLAlchemy plugin would drop without a word.

The objects it is given are built inside the combinations, so this module holds no such test itself.
"""

import enum
import unittest

import pytest
from sqlalchemy import testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises_message

from test.conftest import _refuse_tests_the_plugin_would_drop


def _a_test_method(self):
    pass


class CollectionGuardTest(fixtures.TestBase):
    @testing.combinations(
        ("class_named_like_a_test", "ReflectionTest", type("ReflectionTest", (), {"test_x": _a_test_method})),
        ("class_named_otherwise", "ReflectionChecks", type("ReflectionChecks", (), {"test_x": _a_test_method})),
        ("unittest_case", "ReflectionCase", type("ReflectionCase", (unittest.TestCase,), {"test_x": _a_test_method})),
        ("module_level_function", "test_reflection", lambda: None),
        id_="iaa",
        argnames="name, obj",
    )
    def test_refuses_what_the_plugin_would_drop(self, request, name, obj):
        with expect_raises_message(pytest.Collector.CollectError, name):
            _refuse_tests_the_plugin_would_drop(request.node.getparent(pytest.Module), name, obj)

    @testing.combinations(
        ("testbase", "ReflectionTest", type("ReflectionTest", (fixtures.TestBase,), {"test_x": _a_test_method})),
        ("underscore_mixin", "_ReflectionChecks", type("_ReflectionChecks", (), {"test_x": _a_test_method})),
        (
            "test_false",
            "ReflectionChecks",
            type("ReflectionChecks", (), {"test_x": _a_test_method, "__test__": False}),
        ),
        ("no_test_methods", "Colour", enum.Enum("Colour", "RED GREEN")),
        ("data_named_like_a_test", "ReflectionChecks", type("ReflectionChecks", (), {"test_values": (1, 2)})),
        ("helper_function", "_reflect", lambda: None),
        id_="iaa",
        argnames="name, obj",
    )
    def test_leaves_alone_what_is_not_a_dropped_test(self, request, name, obj):
        _refuse_tests_the_plugin_would_drop(request.node.getparent(pytest.Module), name, obj)
