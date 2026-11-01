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
import functools
import types
import unittest

import pytest
from sqlalchemy import testing
from sqlalchemy.engine import Connection
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises_message

from test.conftest import _refuse_tests_the_plugin_would_drop


def _named_test_x(test: types.FunctionType) -> types.FunctionType:
    """Name a test ``test_x``, as the plugin wants a test's own ``__name__``, under a name pytest doesn't collect."""
    test.__name__ = "test_x"
    return test


@_named_test_x
def _a_test_method(self: object) -> None:
    pass


def _wrapped_without_wraps(test: types.FunctionType) -> types.FunctionType:
    def wrapper(self: object) -> None:
        test(self)

    return wrapper


def _wrapped_with_wraps(test: types.FunctionType) -> types.FunctionType:
    @functools.wraps(test)
    def wrapper(self: object) -> None:
        test(self)

    return wrapper


@_named_test_x
def _a_test_returning_none(self: object) -> None:
    pass


@_named_test_x
def _a_test_taking_a_connection(self: object, connection: Connection):
    pass


@_named_test_x
def _a_test_taking_a_connection_by_name(self: object, connection: "Connection"):
    pass


@_named_test_x
def _a_test_taking_an_int(self: object, value: int = 1):
    pass


@_named_test_x
def _a_test_without_annotations(self, value=1):
    pass


@_named_test_x
def _a_test_taking_a_list_of_ints(self: object, value: list[int] = None):
    pass


@_named_test_x
def _a_test_taking_an_optional_int(self: object, value: int | None = None):
    pass


@_named_test_x
def _a_test_taking_a_list_of_connections(self: object, value: list[Connection] = None):
    pass


@_named_test_x
def _a_test_defaulting_to_an_object(self: object, value=object()):
    pass


@_named_test_x
def _a_test_defaulting_to_a_class(self: object, value=int):
    pass


@_named_test_x
def _a_test_defaulting_to_a_dict_of_objects(self: object, value={"key": object()}):
    pass


@_named_test_x
def _a_test_defaulting_to_a_dict_of_literals(self: object, value={"key": (1, "two", None)}):
    pass


@_named_test_x
def _a_test_defaulting_to_an_enum_member(self: object, value=enum.Enum("Color", "red").red):
    pass


@_named_test_x
def _a_test_defaulting_to_a_keyword_only_object(self: object, *, value=object()):
    pass


def _under_a_mark(test: types.FunctionType) -> types.FunctionType:
    return testing.skip_if(lambda config: False, "never skips")(test)


class _CallableTest:
    def __call__(self, test_self: object) -> None:
        pass


class _BaseKeptFromRunning(fixtures.TestBase):
    __test__ = False

    test_x = _a_test_method


def _nested_class(outer_base: type, inner_base: type) -> type:
    """A class named Outer, deriving from ``outer_base``, with a class named Inner nested in it, deriving from
    ``inner_base`` and holding a test.
    """

    class Outer(outer_base):
        class Inner(inner_base):
            def test_x(self) -> None:
                pass

    return Outer


class CollectionGuardTest(fixtures.TestBase):
    @testing.combinations(
        ("class_named_like_a_test", "ReflectionTest", type("ReflectionTest", (), {"test_x": _a_test_method})),
        ("class_named_otherwise", "ReflectionChecks", type("ReflectionChecks", (), {"test_x": _a_test_method})),
        ("class_named_tests", "ReflectionTests", type("ReflectionTests", (), {"test_x": _a_test_method})),
        ("unittest_case", "ReflectionCase", type("ReflectionCase", (unittest.TestCase,), {"test_x": _a_test_method})),
        (
            "testbase_misnamed_method",
            "ReflectionTest",
            type("ReflectionTest", (fixtures.TestBase,), {"testx": _a_test_method}),
        ),
        ("module_level_function", "test_reflection", lambda: None),
        id_="iaa",
        argnames="name, obj",
    )
    def test_refuses_what_the_plugin_would_drop(self, request, name, obj):
        with expect_raises_message(pytest.Collector.CollectError, name):
            _refuse_tests_the_plugin_would_drop(request.node.getparent(pytest.Module), name, obj)

    @testing.combinations(
        (
            "testbase_classmethod",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": classmethod(_a_test_method)}),
            "ReflectionTest.test_x is a classmethod",
        ),
        (
            "testbase_staticmethod",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": staticmethod(_a_test_method)}),
            "ReflectionTest.test_x is a staticmethod",
        ),
        (
            "plain_class_with_a_classmethod_test",
            type("ReflectionChecks", (), {"test_x": classmethod(_a_test_method)}),
            "ReflectionChecks does not derive from fixtures.TestBase",
        ),
        (
            "plain_class_in_testbase",
            _nested_class(fixtures.TestBase, object),
            "Outer.Inner does not derive from fixtures.TestBase",
        ),
        (
            "testbase_in_plain_class",
            _nested_class(object, fixtures.TestBase),
            "Outer does not derive from fixtures.TestBase",
        ),
        (
            "testbase_test_wrapped_without_wraps",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": _wrapped_without_wraps(_a_test_method)}),
            "ReflectionTest.test_x is a function named wrapper",
        ),
        (
            "testbase_callable_object",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": _CallableTest()}),
            "ReflectionTest.test_x is a _CallableTest, not a function",
        ),
        (
            "testbase_partialmethod",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": functools.partialmethod(_a_test_method)}),
            "ReflectionTest.test_x is a partialmethod, not a function",
        ),
        (
            "testbase_function_without_a_module",
            type(
                "ReflectionTest",
                (fixtures.TestBase,),
                {"test_x": types.FunctionType(_a_test_method.__code__, {}, "test_x")},
            ),
            "ReflectionTest.test_x has no __module__",
        ),
        (
            "subclass_inheriting_test_false",
            type("ReflectionTest", (_BaseKeptFromRunning,), {}),
            "ReflectionTest inherits __test__ = False from _BaseKeptFromRunning",
        ),
        id_="iaa",
        argnames="obj, message",
    )
    def test_refuses_tests_the_plugin_would_drop_and_says_why(self, request, obj, message):
        with expect_raises_message(pytest.Collector.CollectError, message):
            _refuse_tests_the_plugin_would_drop(request.node.getparent(pytest.Module), obj.__name__, obj)

    @testing.combinations(
        ("testbase", "ReflectionTest", type("ReflectionTest", (fixtures.TestBase,), {"test_x": _a_test_method})),
        ("underscore_mixin", "_ReflectionChecks", type("_ReflectionChecks", (), {"test_x": _a_test_method})),
        (
            "test_false",
            "ReflectionChecks",
            type("ReflectionChecks", (), {"test_x": _a_test_method, "__test__": False}),
        ),
        ("no_test_methods", "Colour", enum.Enum("Colour", "RED GREEN")),
        ("helper_function", "_reflect", lambda: None),
        ("testbase_in_testbase", "Outer", _nested_class(fixtures.TestBase, fixtures.TestBase)),
        (
            "class_attribute_naming_a_mixin",
            "ReflectionTest",
            type(
                "ReflectionTest",
                (fixtures.TestBase,),
                {"checks": type("_ReflectionChecks", (), {"test_x": _a_test_method})},
            ),
        ),
        (
            "testbase_test_wrapped_with_wraps",
            "ReflectionTest",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": _wrapped_with_wraps(_a_test_method)}),
        ),
        (
            "testbase_data_named_like_a_test",
            "ReflectionTest",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": _a_test_method, "test_values": (1, 2)}),
        ),
        (
            "testbase_test_false",
            "ReflectionTest",
            type("ReflectionTest", (fixtures.TestBase,), {"test_x": _a_test_method, "__test__": False}),
        ),
        (
            "subclass_setting_test_true",
            "ReflectionTest",
            type("ReflectionTest", (_BaseKeptFromRunning,), {"__test__": True}),
        ),
        id_="iaa",
        argnames="name, obj",
    )
    def test_leaves_alone_what_is_not_a_dropped_test(self, request, name, obj):
        _refuse_tests_the_plugin_would_drop(request.node.getparent(pytest.Module), name, obj)

    @testing.combinations(
        ("mark_over_a_return_annotation", _under_a_mark, _a_test_returning_none, True),
        ("mark_over_a_class_annotation", _under_a_mark, _a_test_taking_a_connection, True),
        ("provide_metadata_over_a_return_annotation", testing.provide_metadata, _a_test_returning_none, True),
        ("mark_over_a_builtin_annotation", _under_a_mark, _a_test_taking_an_int, False),
        ("mark_over_a_string_annotation", _under_a_mark, _a_test_taking_a_connection_by_name, False),
        ("mark_over_no_annotation", _under_a_mark, _a_test_without_annotations, False),
        ("wraps_over_a_return_annotation", _wrapped_with_wraps, _a_test_returning_none, False),
        ("mark_over_a_list_of_builtins", _under_a_mark, _a_test_taking_a_list_of_ints, False),
        ("mark_over_a_union_of_builtins", _under_a_mark, _a_test_taking_an_optional_int, False),
        ("mark_over_a_list_of_a_class", _under_a_mark, _a_test_taking_a_list_of_connections, True),
        ("mark_over_an_object_default", _under_a_mark, _a_test_defaulting_to_an_object, True),
        ("mark_over_a_class_default", _under_a_mark, _a_test_defaulting_to_a_class, True),
        ("mark_over_a_dict_of_objects_default", _under_a_mark, _a_test_defaulting_to_a_dict_of_objects, True),
        ("mark_over_a_keyword_only_object_default", _under_a_mark, _a_test_defaulting_to_a_keyword_only_object, True),
        ("mark_over_a_dict_of_literals_default", _under_a_mark, _a_test_defaulting_to_a_dict_of_literals, False),
        ("mark_over_an_enum_member_default", _under_a_mark, _a_test_defaulting_to_an_enum_member, False),
        ("wraps_over_an_object_default", _wrapped_with_wraps, _a_test_defaulting_to_an_object, False),
        id_="iaaa",
        argnames="decorate, test, refused",
    )
    @testing.requires.sqlalchemy_2
    def test_refuses_a_marked_test_whose_annotations_sqlalchemy_1_4_cannot_rebuild(
        self, request, decorate, test, refused
    ):
        """SQLAlchemy 1.4 writes a mark's wrapper as source and fails to import such a test's module, so 2.0 refuses
        it too. On 1.4 the test is skipped: the mark would fail before the guard runs.
        """
        cls = type("ReflectionTest", (fixtures.TestBase,), {"test_x": decorate(test)})
        module = request.node.getparent(pytest.Module)
        if refused:
            with expect_raises_message(pytest.Collector.CollectError, "ReflectionTest.test_x is under a mark"):
                _refuse_tests_the_plugin_would_drop(module, "ReflectionTest", cls)
        else:
            _refuse_tests_the_plugin_would_drop(module, "ReflectionTest", cls)

    @testing.combinations(
        ("return_annotation", _a_test_returning_none, True),
        ("object_default", _a_test_defaulting_to_an_object, True),
        ("builtin_annotation", _a_test_taking_an_int, False),
        id_="iaa",
        argnames="helper, refused",
    )
    @testing.requires.sqlalchemy_2
    def test_refuses_a_marked_helper_as_it_refuses_a_marked_test(self, request, helper, refused):
        """SQLAlchemy 1.4 wraps a helper under a mark as it wraps a test, whatever the helper is named."""
        cls = type("ReflectionTest", (fixtures.TestBase,), {"build_the_table": _under_a_mark(helper)})
        module = request.node.getparent(pytest.Module)
        if refused:
            with expect_raises_message(pytest.Collector.CollectError, "ReflectionTest.build_the_table is under a mark"):
                _refuse_tests_the_plugin_would_drop(module, "ReflectionTest", cls)
        else:
            _refuse_tests_the_plugin_would_drop(module, "ReflectionTest", cls)
