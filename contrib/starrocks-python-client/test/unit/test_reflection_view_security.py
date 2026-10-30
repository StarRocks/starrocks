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

"""Unit tests for reading a view's SECURITY clause from the SHOW CREATE VIEW statement."""

from sqlalchemy import testing
from sqlalchemy.testing import eq_, fixtures

from starrocks.reflection import StarRocksTableDefinitionParser


def _security_of(create_view: str):
    parser = StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)
    return parser._parse_sql_security_from_create_view(create_view)


class ViewSecurityReflectionTest(fixtures.TestBase):
    @testing.combinations(
        ("clause", "CREATE VIEW `v` (`id`) SECURITY INVOKER AS SELECT id FROM t;", "INVOKER"),
        ("with_comment", 'CREATE VIEW `v` (`id`) COMMENT "plain" SECURITY DEFINER AS SELECT id FROM t;', "DEFINER"),
        ("comment_says_none", 'CREATE VIEW `v` (`id`) COMMENT "x SECURITY NONE" SECURITY INVOKER AS SELECT id FROM t;', "INVOKER"),
        ("comment_says_it_escaped", 'CREATE VIEW `v` (`id`) COMMENT "a \\" SECURITY NONE" SECURITY DEFINER AS SELECT 1;', "DEFINER"),
        ("column_comment_says_it", 'CREATE VIEW `v` (`id` COMMENT " SECURITY NONE") SECURITY INVOKER AS SELECT id FROM t;', "INVOKER"),
        ("definition_says_it", "CREATE VIEW `v` (`id`) SECURITY INVOKER AS SELECT ' SECURITY NONE' AS id;", "INVOKER"),
        ("no_clause_comment_says_it", 'CREATE VIEW `v` (`id`) COMMENT "x SECURITY NONE" AS SELECT id FROM t;', None),
        ("no_clause_definition_says_it", "CREATE VIEW `v` AS SELECT ' SECURITY NONE' AS id;", None),
        argnames="create_view,security",
        id_="iaa",
    )
    def test_the_clause_is_read_and_a_lookalike_in_a_comment_or_definition_is_not(self, create_view, security):
        eq_(_security_of(create_view), security)
