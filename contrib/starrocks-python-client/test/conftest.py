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

"""Test bootstrap: the SQLAlchemy testing plugin drives every test, including the Alembic suite.

The database URL comes from, in order of precedence: ``--dburi``/``--db``, the ``STARROCKS_URL`` environment
variable, a git-ignored ``test.cfg`` ``[db]`` section, then ``setup.cfg``'s ``[db] default``. Run pytest from
this client directory: Alembic stages its migration scripts under ``scratch`` in the working directory.
"""

import ast
import builtins
import functools
import inspect
import os
from pathlib import Path
import types
from typing import Optional, get_args
import unittest

from _pytest.fixtures import getfixturemarker
import pytest
from sqlalchemy.dialects import registry


registry.register("starrocks+pymysql", "starrocks.dialect", "StarRocksDialect")
registry.register("starrocks", "starrocks.dialect", "StarRocksDialect")

# It's just used to suppress a spurious warning from pytest.
pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin import plugin_base, pytestplugin  # noqa: E402
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402,F403,I001

import starrocks.alembic  # noqa: E402,F401


CLIENT_DIRECTORY = Path(__file__).resolve().parent.parent


def pytest_configure(config: pytest.Config) -> None:
    _read_config_files_from_client_directory()
    _use_starrocks_url_when_no_dburi(config)
    pytestplugin.pytest_configure(config)


def pytest_pycollect_makeitem(collector: pytest.Collector, name: str, obj: object) -> object:
    _refuse_tests_the_plugin_would_drop(collector, name, obj)
    return pytestplugin.pytest_pycollect_makeitem(collector, name, obj)


def _read_config_files_from_client_directory() -> None:
    """Read ``setup.cfg`` and ``test.cfg`` from this client directory, whatever the working directory.

    SQLAlchemy 1.4 reads them relative to the working directory when it adds its options, so a run started
    elsewhere found no ``[db]`` section. 2.0 reads them later, from pytest's rootdir, which is this directory,
    and has no file config yet at this point.
    """
    if plugin_base.file_config is not None:
        plugin_base.file_config.read([CLIENT_DIRECTORY / "setup.cfg", CLIENT_DIRECTORY / "test.cfg"])


def _use_starrocks_url_when_no_dburi(config: pytest.Config) -> None:
    """Map the ``STARROCKS_URL`` environment variable onto ``--dburi`` unless a URL was given."""
    starrocks_url = os.environ.get("STARROCKS_URL")
    if starrocks_url and not config.option.dburi and not config.option.db:
        config.option.dburi = [starrocks_url]


def _refuse_tests_the_plugin_would_drop(collector: pytest.Collector, name: str, obj: object) -> None:
    """Fail collection for tests that the SQLAlchemy plugin would skip without a message.

    The plugin runs only plain ``test_*`` methods of ``fixtures.TestBase`` subclasses, so these would vanish
    from the run:

    - a module-level test function;
    - a ``testfoo`` method of a ``TestBase`` subclass, or a test that is a classmethod or staticmethod;
    - a test that is not a function, such as a callable object or a ``functools.partialmethod``;
    - a test function whose own ``__name__`` is not ``test_*``, as a decorator without ``functools.wraps``
      leaves it;
    - a class with test methods that does not derive from ``TestBase``, a ``unittest.TestCase`` among them;
    - a class nested in another class: the plugin collects it only when both derive from ``TestBase``;
    - a class with tests that inherits ``__test__ = False`` instead of setting it, which pytest skips.

    That covers a class of any name: pytest's own class name filter does not apply, since the plugin collects a
    ``TestBase`` subclass whatever its name. Classes that are not meant to run are left alone: ``__test__ =
    False`` set in the class itself, a ``_`` prefix, or no test methods at all, as with enums and named tuples.
    """
    if not isinstance(collector, pytest.Module) or getattr(obj, "__module__", None) != collector.module.__name__:
        return
    if inspect.isfunction(obj) and collector.funcnamefilter(name):
        raise collector.CollectError(
            f"{name} is a module-level test function; move it into a fixtures.TestBase subclass"
        )
    if inspect.isclass(obj):
        _refuse_tests_of_a_class_the_plugin_would_drop(collector, name, name, obj)


def _refuse_tests_of_a_class_the_plugin_would_drop(collector: pytest.Module, path: str, name: str, cls: type) -> None:
    """Fail collection for a class's tests that the plugin would drop, and for those of the classes nested in it.

    ``name`` is the attribute the class is found under, and ``path`` names it from the module, as ``Outer.Inner``.
    ``fixtures`` is imported here because it can only be imported once the plugin is configured.
    """
    from sqlalchemy.testing import fixtures

    if name.startswith("_"):
        return
    if not getattr(cls, "__test__", True):
        _refuse_an_inherited_test_false(collector, path, cls)
        return
    if not issubclass(cls, fixtures.TestBase):
        if _holds_tests(collector, cls):
            raise collector.CollectError(
                f"{path} does not derive from fixtures.TestBase, so the SQLAlchemy plugin would never run its tests; "
                "derive from it, or give a mixin a name starting with _"
            )
        return
    for attr, method in _test_methods(collector, cls).items():
        reason = _why_the_plugin_would_never_run(collector, cls, attr, method)
        if reason is not None:
            raise collector.CollectError(f"{path}.{attr} {reason}, so the SQLAlchemy plugin would never run it")
    _refuse_a_marked_function_sqlalchemy_1_4_cannot_import(collector, path, cls)
    for nested_name, nested in _nested_classes(collector, cls).items():
        _refuse_tests_of_a_class_the_plugin_would_drop(collector, f"{path}.{nested_name}", nested_name, nested)


def _refuse_an_inherited_test_false(collector: pytest.Module, path: str, cls: type) -> None:
    """Fail collection for a class with tests that inherits ``__test__ = False`` rather than setting it.

    pytest skips any class whose ``__test__`` is false, so a subclass of a base kept from running would drop its
    tests too. A class that sets ``__test__ = False`` itself is meant not to run, and is left alone.
    """
    if "__test__" in vars(cls) or not _holds_tests(collector, cls):
        return
    base = next(base for base in cls.__mro__ if "__test__" in vars(base))
    raise collector.CollectError(
        f"{path} inherits __test__ = False from {base.__name__}, so pytest would never run its tests; "
        "set __test__ = True in it, or give it a name starting with _"
    )


def _why_the_plugin_would_never_run(collector: pytest.Module, cls: type, attr: str, method: object) -> Optional[str]:
    """Why the plugin would drop a test of a ``TestBase`` subclass, or None when it runs it.

    The plugin runs a function whose own ``__name__`` is ``test_*`` and that has a ``__module__``, and drops any
    other object under a test name. Only a test defined in a class of this module is checked beyond its name,
    so the tests inherited from SQLAlchemy's and Alembic's suites are taken as they are.
    """
    if isinstance(method, (classmethod, staticmethod)):
        return f"is a {type(method).__name__}"
    if not attr.startswith("test_"):
        return "is not named test_*"
    if not _defined_in_this_module(collector, cls, attr):
        return None
    if not inspect.isfunction(method):
        return f"is a {type(method).__name__}, not a function"
    if not method.__name__.startswith("test_"):
        return f"is a function named {method.__name__}, as a decorator without functools.wraps leaves it"
    if method.__module__ is None:
        return "has no __module__"
    return None


def _refuse_a_marked_function_sqlalchemy_1_4_cannot_import(collector: pytest.Module, path: str, cls: type) -> None:
    """Fail collection for a function of the class, a test or a helper, that SQLAlchemy 1.4 would fail to import.

    A helper under a mark or ``testing.provide_metadata`` is wrapped as a test is, and a function that a base
    in this module defines is checked as well, whatever its name.
    """
    for attr in dir(cls):
        method = inspect.getattr_static(cls, attr)
        if isinstance(method, (classmethod, staticmethod)):
            method = method.__func__
        if not inspect.isfunction(method) or not _defined_in_this_module(collector, cls, attr):
            continue
        reason = _why_sqlalchemy_1_4_cannot_rebuild_its_signature(method)
        if reason is not None:
            raise collector.CollectError(
                f"{path}.{attr} is under a mark and {reason}, so SQLAlchemy 1.4 would fail to import the module; "
                "drop it"
            )


def _why_sqlalchemy_1_4_cannot_rebuild_its_signature(method: object) -> Optional[str]:
    """Why SQLAlchemy 1.4 would fail to import a module, rebuilding the function's signature for a mark, or None.

    Every mark (``skip_if``, ``fails_if``, ``testing.requires.*`` and the marks in ``test/util.py``), and
    decorators such as ``testing.provide_metadata``, wrap a function through SQLAlchemy's ``util.decorator``, which
    writes the wrapper's signature as source and runs it. 1.4 cuts the last character of a return annotation, a
    ``SyntaxError``, and writes a type from outside builtins under a name the source doesn't define, a
    ``NameError``. It writes a default value whose type is a builtin as its ``repr``, which fails when that isn't
    source, as for ``object()``, a class or a dict holding such objects. Any other default is passed by name. On 2.0
    the wrapper is built from source too, so its code has no file; a mark also sets ``_sa_exclusion_extend`` on it.
    """
    rebuilt = False
    original = method
    while hasattr(original, "__wrapped__"):
        rebuilt = rebuilt or _built_by_sqlalchemys_decorator(original)
        original = original.__wrapped__
    if not rebuilt:
        return None
    if any(
        name == "return" or not _sqlalchemy_1_4_writes_back(annotation)
        for name, annotation in getattr(original, "__annotations__", {}).items()
    ):
        return "has a return annotation, or a parameter annotation that is not a plain builtin class"
    defaults = [
        *(getattr(original, "__defaults__", None) or ()),
        *(getattr(original, "__kwdefaults__", None) or {}).values(),
    ]
    if not all(_sqlalchemy_1_4_writes_back_default(default) for default in defaults):
        return "has a default value whose repr is not source"
    return None


def _sqlalchemy_1_4_writes_back_default(default: object) -> bool:
    """Whether 1.4 can write a default as source: not a builtin type's, or a ``repr`` of builtin names and literals."""
    if type(default).__module__ != "builtins":
        return True
    try:
        tree = ast.parse(repr(default), mode="eval")
    except SyntaxError:
        return False
    return all(hasattr(builtins, node.id) for node in ast.walk(tree) if isinstance(node, ast.Name))


def _built_by_sqlalchemys_decorator(wrapper: object) -> bool:
    """Whether a wrapper is a mark's, or was otherwise written as source by SQLAlchemy's ``util.decorator``."""
    code = getattr(wrapper, "__code__", None)
    return hasattr(wrapper, "_sa_exclusion_extend") or (code is not None and code.co_filename == "<string>")


def _sqlalchemy_1_4_writes_back(annotation: object) -> bool:
    """Whether 1.4 writes a parameter annotation as source it can run.

    That is a string, None, a builtin class, and ``list[int]`` or ``int | None`` made of those.
    """
    if isinstance(annotation, (types.GenericAlias, types.UnionType)):
        return all(_sqlalchemy_1_4_writes_back(argument) for argument in get_args(annotation))
    return (
        annotation is None
        or isinstance(annotation, str)
        or (isinstance(annotation, type) and annotation.__module__ == "builtins")
    )


def _test_methods(collector: pytest.Module, cls: type) -> dict[str, object]:
    """What pytest's name filter takes for a class's tests, inherited ones included, as the class holds them.

    These are its functions, classmethods and staticmethods, and the other callables and
    ``functools.partialmethod`` objects defined in a class of this module, pytest fixtures left out.
    """
    methods = {}
    for attr in dir(cls):
        if collector.funcnamefilter(attr):
            method = inspect.getattr_static(cls, attr)
            if inspect.isfunction(method) or isinstance(method, (classmethod, staticmethod)):
                methods[attr] = method
            elif (
                (callable(method) or isinstance(method, functools.partialmethod))
                and getfixturemarker(method) is None
                and _defined_in_this_module(collector, cls, attr)
            ):
                methods[attr] = method
    return methods


def _defined_in_this_module(collector: pytest.Module, cls: type, attr: str) -> bool:
    """Whether the class that defines a class's attribute, the class itself or a base, is in this module."""
    owner = next((klass for klass in cls.__mro__ if attr in vars(klass)), None)
    return owner is not None and owner.__module__ == collector.module.__name__


def _nested_classes(collector: pytest.Module, cls: type) -> dict[str, type]:
    """The classes defined in the body of a class, leaving out a class attribute that names a class from
    elsewhere.
    """
    return {
        attr: value
        for attr, value in vars(cls).items()
        if inspect.isclass(value)
        and value.__module__ == collector.module.__name__
        and value.__qualname__ == f"{cls.__qualname__}.{attr}"
    }


def _holds_tests(collector: pytest.Module, cls: type) -> bool:
    """Whether a class is a ``unittest.TestCase``, has test methods, or has a nested class meant to run that
    holds tests.
    """
    if issubclass(cls, unittest.TestCase) or _test_methods(collector, cls):
        return True
    return any(
        not nested_name.startswith("_") and getattr(nested, "__test__", True) and _holds_tests(collector, nested)
        for nested_name, nested in _nested_classes(collector, cls).items()
    )
