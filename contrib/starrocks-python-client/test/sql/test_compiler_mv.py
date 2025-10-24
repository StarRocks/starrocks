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

import logging

from sqlalchemy.dialects import registry
from sqlalchemy.schema import MetaData

from starrocks.sql.ddl import CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import MaterializedView
from test.unit.test_utils import normalize_sql


class TestMaterializedViewCompiler:
    @classmethod
    def setup_class(cls):
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()
        cls.metadata = MetaData()

    def test_create_materialized_view(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = "CREATE MATERIALIZED VIEW my_mv AS SELECT * FROM my_table"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_with_properties(self):
        properties = {"replication_num": "1"}
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", properties=properties, metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = 'PROPERTIES ("replication_num" = "1")'
        assert expected in sql

    def test_drop_materialized_view(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", metadata=self.metadata)
        sql = str(DropMaterializedView(mv).compile(dialect=self.dialect))
        expected = "DROP MATERIALIZED VIEW my_mv"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_drop_materialized_view_if_exists(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", metadata=self.metadata)
        sql = str(DropMaterializedView(mv, if_exists=True).compile(dialect=self.dialect))
        expected = "DROP MATERIALIZED VIEW IF EXISTS my_mv"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_with_schema(self):
        """Test CREATE MATERIALIZED VIEW with schema qualification."""
        mv = MaterializedView("my_mv", "SELECT id, name FROM users", metadata=self.metadata, schema="test_db")
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = "CREATE MATERIALIZED VIEW test_db.my_mv AS SELECT id, name FROM users"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_if_not_exists(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", metadata=self.metadata)
        sql = str(CreateMaterializedView(mv, if_not_exists=True).compile(dialect=self.dialect))
        expected = "CREATE MATERIALIZED VIEW IF NOT EXISTS my_mv AS SELECT * FROM my_table"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_complex_query(self):
        """Test CREATE MATERIALIZED VIEW with complex SELECT query."""
        complex_query = """
        SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = true
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 0
        ORDER BY total_amount DESC
        """
        mv = MaterializedView("user_order_summary", complex_query, metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = f"CREATE MATERIALIZED VIEW user_order_summary AS {complex_query.strip()}"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_with_aggregation(self):
        """Test CREATE MATERIALIZED VIEW with aggregation functions."""
        agg_query = """
        SELECT
            category_id,
            COUNT(*) as product_count,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            SUM(stock_quantity) as total_stock
        FROM products
        GROUP BY category_id
        """
        mv = MaterializedView("category_summary", agg_query, metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = f"CREATE MATERIALIZED VIEW category_summary AS {agg_query.strip()}"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_with_window_functions(self):
        """Test CREATE MATERIALIZED VIEW with window functions."""
        window_query = """
        SELECT
            user_id,
            order_date,
            amount,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) as rn,
            SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total
        FROM orders
        """
        mv = MaterializedView("user_order_window", window_query, metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = f"CREATE MATERIALIZED VIEW user_order_window AS {window_query.strip()}"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_with_multiple_properties(self):
        """Test CREATE MATERIALIZED VIEW with multiple properties."""
        properties = {
            "replication_num": "3",
            "storage_medium": "SSD",
            "storage_cooldown_time": "2025-12-31 23:59:59"
        }
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", properties=properties, metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))

        # Check that all properties are present
        assert '"replication_num" = "3"' in sql
        assert '"storage_medium" = "SSD"' in sql
        assert '"storage_cooldown_time" = "2025-12-31 23:59:59"' in sql
        assert "PROPERTIES" in sql

    def test_create_materialized_view_with_special_chars(self):
        """Test CREATE MATERIALIZED VIEW with special characters in identifiers."""
        special_query = """
        SELECT
            `user-id` as user_id,
            `user name` as user_name,
            `email@domain.com` as email
        FROM `user-table`
        WHERE `status` = 'active'
        """
        mv = MaterializedView("special_chars_mv", special_query, metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = f"CREATE MATERIALIZED VIEW special_chars_mv AS {special_query.strip()}"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_drop_materialized_view_with_schema(self):
        """Test DROP MATERIALIZED VIEW with schema qualification."""
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", metadata=self.metadata, schema="test_db")
        sql = str(DropMaterializedView(mv, if_exists=True).compile(dialect=self.dialect))
        expected = "DROP MATERIALIZED VIEW IF EXISTS test_db.my_mv"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_materialized_view_with_cte(self):
        """Test CREATE MATERIALIZED VIEW with Common Table Expressions (CTEs)."""
        cte_query = """
        WITH monthly_stats AS (
            SELECT user_id, COUNT(*) as monthly_orders
            FROM orders
            WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
            GROUP BY user_id
        )
        SELECT u.name, COALESCE(ms.monthly_orders, 0) as orders
        FROM users u
        LEFT JOIN monthly_stats ms ON u.id = ms.user_id
        """
        mv = MaterializedView("user_monthly_stats", cte_query, metadata=self.metadata)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = f"CREATE MATERIALIZED VIEW user_monthly_stats AS {cte_query.strip()}"
        assert normalize_sql(sql) == normalize_sql(expected)


