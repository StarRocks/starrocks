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

The system tests, and the integration modules in ``collect_ignore``, are left out of collection until they
are ported to the plugin.
"""

import inspect
import os
from pathlib import Path
import unittest

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
collect_ignore = [
    "integration/test_autogenerate_alter_table.py",
    "integration/test_autogenerate_columns.py",
    "integration/test_autogenerate_mvs.py",
    "integration/test_autogenerate_views.py",
    "integration/test_combine_column_alters.py",
    "integration/test_combine_column_alters_range_key.py",
    "integration/test_reflection_columns.py",
    "integration/test_reflection_data_types.py",
    "integration/test_reflection_indexes.py",
    "integration/test_reflection_mv.py",
    "integration/test_reflection_table.py",
    "integration/test_reflection_view.py",
    "integration/test_result_processor.py",
    "integration/test_run_mode.py",
    "integration/test_schema_change_wait.py",
    "system",
]


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

    The plugin runs only the ``test_*`` methods of ``fixtures.TestBase`` subclasses, so a module-level test
    function would vanish from the run, and so would a class with test methods that does not derive from
    ``TestBase``, a ``unittest.TestCase`` among them. A class of any name is checked, since the plugin collects a
    ``TestBase`` subclass whatever its name. A class not meant to run is left alone: one named with a leading
    ``_``, one with ``__test__ = False``, and one without test methods, such as an enum or a named tuple.
    ``fixtures`` is imported here because it can only be imported once the plugin is configured.
    """
    from sqlalchemy.testing import fixtures

    if not isinstance(collector, pytest.Module) or getattr(obj, "__module__", None) != collector.module.__name__:
        return
    if inspect.isfunction(obj) and collector.funcnamefilter(name):
        raise collector.CollectError(
            f"{name} is a module-level test function; move it into a fixtures.TestBase subclass"
        )
    if (
        inspect.isclass(obj)
        and not name.startswith("_")
        and getattr(obj, "__test__", True)
        and not issubclass(obj, fixtures.TestBase)
        and _holds_tests(collector, obj)
    ):
        raise collector.CollectError(
            f"{name} does not derive from fixtures.TestBase, so the SQLAlchemy plugin would never run its tests; "
            "derive from it, or give a mixin a name starting with _"
        )


def _holds_tests(collector: pytest.Module, cls: type) -> bool:
    """Whether a class is a ``unittest.TestCase``, or has a method that pytest's name filter takes for a test."""
    return issubclass(cls, unittest.TestCase) or any(
        collector.funcnamefilter(attr) and callable(getattr(cls, attr, None)) for attr in dir(cls)
    )
