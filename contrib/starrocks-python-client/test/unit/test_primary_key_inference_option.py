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

"""How ``starrocks_infer_primary_key`` reaches the dialect, which leaves it off unless it is given."""

import io

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, Integer, String, create_engine, engine_from_config, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from starrocks.dialect import StarRocksDialect


class PrimaryKeyInferenceOptionTest(fixtures.TestBase):
    def test_the_inference_is_off_unless_it_is_given(self):
        eq_(StarRocksDialect().infer_primary_key, False)
        eq_(create_engine("starrocks://root@localhost:9030/db").dialect.infer_primary_key, False)

    @testing.combinations(
        ("on", True, True),
        ("off", False, False),
        ("text_on", "true", True),
        ("text_off", "false", False),
        argnames="value,infers",
        id_="iaa",
    )
    def test_create_engine_passes_the_option_to_the_dialect(self, value, infers):
        engine = create_engine("starrocks://root@localhost:9030/db", starrocks_infer_primary_key=value)
        eq_(engine.dialect.infer_primary_key, infers)

    def test_engine_from_config_passes_the_option_as_text(self):
        configuration = {
            "sqlalchemy.url": "starrocks://root@localhost:9030/db",
            "sqlalchemy.starrocks_infer_primary_key": "true",
        }
        eq_(engine_from_config(configuration).dialect.infer_primary_key, True)

    @testing.combinations(
        ("dialect_name", {"dialect_name": "starrocks"}),
        ("url", {"url": "starrocks://root@localhost:9030/db"}),
        argnames="target",
        id_="ia",
    )
    @testing.combinations(
        ("on", {"starrocks_infer_primary_key": True}, "PRIMARY KEY (id)"),
        ("off", {}, None),
        argnames="option,key_clause",
        id_="iaa",
    )
    def test_offline_migrations_take_the_option_in_dialect_opts(self, target, option, key_clause):
        """Offline, Alembic makes the dialect from ``dialect_opts``; an online engine is made by ``env.py``."""
        buffer = io.StringIO()
        context = MigrationContext.configure(
            **target,
            opts={"as_sql": True, "output_buffer": buffer},
            dialect_opts={"paramstyle": "named", **option},
        )
        Operations(context).create_table("users", Column("id", Integer, primary_key=True), Column("name", String(20)))
        script = buffer.getvalue()
        assert "CREATE TABLE users" in script, script
        if key_clause is None:
            assert "KEY (" not in script, script
        else:
            assert key_clause in script, script
