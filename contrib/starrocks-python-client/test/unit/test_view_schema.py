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

"""Unit tests for View schema definition and column handling."""

from sqlalchemy import Column, Integer, MetaData, Table, column, select, table, testing, text
from sqlalchemy.testing import expect_raises_message, fixtures

from starrocks.alembic.ops import AlterViewOp, CreateViewOp, DropViewOp
from starrocks.datatype import STRING
from starrocks.sql.schema import MaterializedView, View


class ViewColumnDefinitionsTest(fixtures.TestBase):
    """Test different ways to define columns in View."""

    __requires__ = ("sqlalchemy_2",)

    def test_view_with_column_objects(self):
        """Test View with Column objects via *args."""
        metadata = MetaData()
        view = View(
            'v1',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            definition='SELECT id, name FROM users',
            comment='User view'
        )

        assert view.name == 'v1'
        assert view.comment == 'User view'
        assert view.definition == 'SELECT id, name FROM users'
        assert len(view.columns) == 2
        assert view.columns['id'].name == 'id'
        assert view.columns['name'].comment == 'User name'

    def test_view_with_string_columns(self):
        """Test View with string column definitions."""
        metadata = MetaData()
        view = View(
            'v2',
            metadata,
            definition='SELECT id, name FROM users',
            columns=['id', 'name']
        )

        assert view.name == 'v2'
        assert len(view.columns) == 2
        assert view.columns['id'].name == 'id'
        assert view.columns['name'].name == 'name'

    def test_view_with_dict_columns(self):
        """Test View with dict column definitions."""
        metadata = MetaData()
        view = View(
            'v3',
            metadata,
            definition='SELECT id, name, email FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'User email'}
            ]
        )

        assert view.name == 'v3'
        assert len(view.columns) == 3
        assert view.columns['id'].name == 'id'
        assert view.columns['id'].comment is None
        assert view.columns['name'].comment == 'User name'
        assert view.columns['email'].comment == 'User email'

    def test_view_with_mixed_columns(self):
        """Test View with mixed column definitions."""
        metadata = MetaData()
        view = View(
            'v4',
            metadata,
            Column('id', STRING()),  # Column object via *args
            definition='SELECT id, name, email FROM users',
            columns=[  # Additional columns via columns parameter
                'name',
                {'name': 'email', 'comment': 'User email'}
            ]
        )

        assert view.name == 'v4'
        assert len(view.columns) == 3
        assert view.columns['id'].name == 'id'
        assert view.columns['name'].name == 'name'
        assert view.columns['email'].comment == 'User email'

    def test_view_without_columns(self):
        """Test View without explicit columns."""
        metadata = MetaData()
        view = View(
            'v5',
            metadata,
            definition='SELECT * FROM users'
        )

        assert view.name == 'v5'
        assert view.definition == 'SELECT * FROM users'
        assert len(view.columns) == 0

    def test_view_columns_with_invalid_dict(self):
        """Test that invalid dict column definition raises error."""
        metadata = MetaData()
        with expect_raises_message(ValueError, "Invalid column definition"):
            View(
                'v_invalid',
                metadata,
                definition='SELECT 1',
                columns=[{'comment': 'Missing name'}]
            )

    def test_view_columns_with_invalid_type(self):
        """Test that invalid column type raises error."""
        metadata = MetaData()
        with expect_raises_message(ValueError, "Invalid column definitions"):
            View(
                'v_invalid',
                metadata,
                definition='SELECT 1',
                columns=[123]  # Invalid type
            )


class ViewOpsWithColumnsTest(fixtures.TestBase):
    """Test View operations with columns."""

    @testing.requires.sqlalchemy_2
    def test_create_view_op_from_view_with_columns(self):
        """Test creating CreateViewOp from View object with columns."""
        metadata = MetaData()
        view = View(
            'v1',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            Column('email', STRING()),
            definition='SELECT id, name, email FROM users',
            comment='User view'
        )

        op = CreateViewOp.from_view(view)

        assert op.view_name == 'v1'
        assert op.definition == 'SELECT id, name, email FROM users'
        assert op.comment == 'User view'
        assert op.columns is not None
        assert len(op.columns) == 3
        assert op.columns[0] == {'name': 'id', 'comment': None}
        assert op.columns[1] == {'name': 'name', 'comment': 'User name'}
        assert op.columns[2] == {'name': 'email', 'comment': None}

    @testing.requires.sqlalchemy_2
    def test_create_view_op_reverse(self):
        """Test CreateViewOp reverse operation."""
        metadata = MetaData()
        view = View(
            'v2',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            definition='SELECT id, name FROM users',
            comment='User view'
        )

        create_op = CreateViewOp.from_view(view)
        assert create_op.definition is not None
        drop_op = create_op.reverse()

        assert drop_op.view_name == 'v2'

    @testing.requires.sqlalchemy_2
    def test_drop_view_op_from_view_with_columns(self):
        """Test creating DropViewOp from View object with columns."""
        metadata = MetaData()
        view = View(
            'v4',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            definition='SELECT id, name FROM users'
        )

        drop_op = DropViewOp.from_view(view)

        assert drop_op.view_name == 'v4'
        assert drop_op.existing_columns is not None
        assert len(drop_op.existing_columns) == 2

        # Test reverse
        create_op = drop_op.reverse()
        assert create_op.columns is not None
        assert len(create_op.columns) == 2

    def test_create_view_op_manual_with_columns(self):
        """Test manually creating CreateViewOp with columns."""
        op = CreateViewOp(
            'v3',
            'SELECT id, name FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ],
            comment='Manual view'
        )

        assert op.view_name == 'v3'
        assert op.columns is not None
        assert len(op.columns) == 2

    @testing.requires.sqlalchemy_2
    def test_view_op_without_columns(self):
        """Test View op without columns."""
        metadata = MetaData()
        view = View(
            'v5',
            metadata,
            definition='SELECT * FROM users'
        )

        op = CreateViewOp.from_view(view)
        assert op.columns is None


class AlterViewOpsWithColumnsTest(fixtures.TestBase):
    """Test AlterView operations with columns."""

    def test_alter_view_op_with_columns(self):
        """Test creating AlterViewOp with columns."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name, email FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'Email address'}
            ],
            comment='Updated view'
        )

        assert op.view_name == 'v1'
        assert op.definition == 'SELECT id, name, email FROM users'
        assert op.comment == 'Updated view'
        assert op.columns is not None
        assert len(op.columns) == 3
        assert op.columns[0] == {'name': 'id'}
        assert op.columns[1] == {'name': 'name', 'comment': 'User name'}
        assert op.columns[2] == {'name': 'email', 'comment': 'Email address'}

    def test_alter_view_op_reverse_with_columns(self):
        """Test AlterViewOp reverse operation preserves columns."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name FROM users',
            columns=[
                {'name': 'id', 'comment': None},
                {'name': 'name', 'comment': 'New comment'}
            ],
            comment='New view',
            existing_definition='SELECT id FROM users',
            existing_columns=[
                {'name': 'id', 'comment': None}
            ],
            existing_comment='Old view'
        )

        # Test forward operation
        assert op.view_name == 'v1'
        assert op.definition == 'SELECT id, name FROM users'
        assert len(op.columns) == 2
        assert op.columns[1]['comment'] == 'New comment'

        # Test reverse
        reverse_op = op.reverse()
        assert reverse_op.view_name == 'v1'
        assert reverse_op.definition == 'SELECT id FROM users'
        assert reverse_op.columns is not None
        assert len(reverse_op.columns) == 1
        assert reverse_op.columns[0]['name'] == 'id'
        assert reverse_op.comment == 'Old view'

        # Test reverse of reverse (should restore original)
        re_reverse_op = reverse_op.reverse()
        assert re_reverse_op.definition == 'SELECT id, name FROM users'
        assert len(re_reverse_op.columns) == 2
        assert re_reverse_op.columns[1]['comment'] == 'New comment'

    def test_alter_view_op_with_security_and_columns(self):
        """Test AlterViewOp with both security and columns."""
        op = AlterViewOp(
            'secure_view',
            'SELECT id, sensitive_data FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'sensitive_data', 'comment': 'Protected data'}
            ],
            security='INVOKER',
            comment='Secure view with columns'
        )

        assert op.view_name == 'secure_view'
        assert op.security == 'INVOKER'
        assert op.columns is not None
        assert len(op.columns) == 2
        assert op.columns[1]['comment'] == 'Protected data'

    def test_alter_view_op_without_columns(self):
        """Test AlterViewOp without explicit columns."""
        op = AlterViewOp(
            'v2',
            'SELECT * FROM users',
            comment='Simple view'
        )

        assert op.view_name == 'v2'
        assert op.definition == 'SELECT * FROM users'
        assert op.columns is None

    def test_alter_view_op_change_column_order(self):
        """Test AlterViewOp with different column order."""
        op = AlterViewOp(
            'v1',
            'SELECT name, id FROM users',  # Changed order
            columns=[
                {'name': 'name', 'comment': 'User name'},
                {'name': 'id'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ]
        )

        # Columns should reflect the new order
        assert op.columns[0]['name'] == 'name'
        assert op.columns[1]['name'] == 'id'

        # Reverse should have original order
        reverse_op = op.reverse()
        assert reverse_op.columns[0]['name'] == 'id'
        assert reverse_op.columns[1]['name'] == 'name'

    def test_alter_view_op_add_column(self):
        """Test AlterViewOp that adds a column."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name, email FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'New column'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ]
        )

        # Forward: 3 columns
        assert len(op.columns) == 3
        assert op.columns[2]['name'] == 'email'

        # Reverse: 2 columns
        reverse_op = op.reverse()
        assert len(reverse_op.columns) == 2
        assert 'email' not in [col['name'] for col in reverse_op.columns]

    def test_alter_view_op_remove_column(self):
        """Test AlterViewOp that removes a column."""
        op = AlterViewOp(
            'v1',
            'SELECT id FROM users',
            columns=[
                {'name': 'id'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ]
        )

        # Forward: 1 column
        assert len(op.columns) == 1
        assert op.columns[0]['name'] == 'id'

        # Reverse: 2 columns
        reverse_op = op.reverse()
        assert len(reverse_op.columns) == 2
        assert reverse_op.columns[1]['name'] == 'name'
        assert reverse_op.columns[1]['comment'] == 'User name'

    def test_alter_view_op_change_column_comment(self):
        """Test AlterViewOp that only changes column comment."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'Updated comment'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'Original comment'}
            ]
        )

        # Forward
        assert op.columns[1]['comment'] == 'Updated comment'

        # Reverse
        reverse_op = op.reverse()
        assert reverse_op.columns[1]['comment'] == 'Original comment'


class ViewCreationOrderTest(fixtures.TestBase):
    """``MetaData.sorted_tables`` puts a view after the tables it reads, whatever their names."""

    __requires__ = ("sqlalchemy_2",)

    @staticmethod
    def _base(metadata):
        return Table("z_orders", metadata, Column("id", Integer))

    @staticmethod
    def _order(metadata):
        return [table.name for table in metadata.sorted_tables]

    @testing.combinations(
        ("bare", "SELECT id FROM z_orders"),
        ("backticked_and_qualified", "SELECT id FROM `shop`.`z_orders`"),
        ("upper_case", "SELECT ID FROM Z_ORDERS"),
        argnames="definition",
        id_="ia",
    )
    def test_string_definition(self, definition):
        metadata = MetaData()
        self._base(metadata)
        View("a_order_view", metadata, definition=definition)
        assert self._order(metadata) == ["z_orders", "a_order_view"]

    def test_selectable_definition(self):
        metadata = MetaData()
        orders = self._base(metadata)
        View("a_order_view", metadata, definition=select(orders.c.id))
        assert self._order(metadata) == ["z_orders", "a_order_view"]

    @testing.combinations(
        ("text", lambda: text("SELECT id FROM z_orders")),
        ("textual_select", lambda: text("SELECT id FROM z_orders").columns(column("id"))),
        ("select_from_text", lambda: select(text("id")).select_from(text("z_orders"))),
        ("select_from_table_clause", lambda: select(column("id")).select_from(table("z_orders"))),
        argnames="build_definition",
        id_="ia",
    )
    def test_definition_naming_no_table_objects(self, build_definition):
        """A ``text()`` or ``table()`` names no Table to SQLAlchemy, so the definition's SQL is read for them."""
        metadata = MetaData()
        View("a_order_view", metadata, definition=build_definition())
        self._base(metadata)
        assert self._order(metadata) == ["z_orders", "a_order_view"]

    @testing.combinations(
        ("join", "SELECT o.id FROM a_customers c JOIN z_orders o ON o.id = c.id", ["a_customers", "z_orders"]),
        ("comma_list", "SELECT 1 FROM a_customers c, z_orders", ["a_customers", "z_orders"]),
        ("subquery", "SELECT * FROM (SELECT id FROM z_orders) t", ["z_orders"]),
        ("in_subquery", "SELECT 1 FROM a_customers WHERE id IN (SELECT id FROM z_orders)", ["a_customers", "z_orders"]),
        ("alias_only", "SELECT id AS z_orders FROM a_customers", ["a_customers"]),
        ("comma_after_on", "SELECT 1 FROM a_customers c JOIN m_x x ON c.id = x.id, z_orders", ["a_customers", "z_orders"]),
        ("join_hint", "SELECT 1 FROM a_customers c INNER JOIN [shuffle] z_orders o ON c.id = o.id", ["a_customers", "z_orders"]),
        ("commented_name", "SELECT 1 FROM a_customers /* , z_orders */ -- , z_orders", ["a_customers"]),
        ("cte_name", "WITH z_orders AS (SELECT id FROM a_customers) SELECT id FROM z_orders", ["a_customers"]),
        ("table_function", "SELECT 1 FROM a_customers, z_orders(1) AS u(x)", ["a_customers"]),
        ("extract_from_a_column", "SELECT extract(year FROM z_orders) AS y FROM a_customers", ["a_customers"]),
        ("trim_from_a_column", "SELECT trim(BOTH ' ' FROM z_orders) AS t FROM a_customers", ["a_customers"]),
        (
            "parenthesised_join",
            "SELECT 1 FROM (a_customers c JOIN z_orders o ON c.id = o.id)",
            ["a_customers", "z_orders"],
        ),
        (
            "nested_parenthesised_join",
            "SELECT 1 FROM ((a_customers c JOIN m_x x ON c.id = x.id) JOIN z_orders o ON c.id = o.id)",
            ["a_customers", "z_orders"],
        ),
        ("subquery_select_list", "SELECT * FROM (SELECT id, a_customers FROM z_orders) t", ["z_orders"]),
        ("doubly_parenthesised_subquery", "SELECT * FROM ((SELECT id FROM z_orders)) t", ["z_orders"]),
        ("values", "SELECT * FROM (VALUES (1), (2)) AS a_customers(id), z_orders", ["z_orders"]),
        id_="iaa",
        argnames="definition,read",
    )
    def test_only_table_references_count(self, definition, read):
        metadata = MetaData()
        Table("a_customers", metadata, Column("id", Integer))
        self._base(metadata)
        view = View("m_view", metadata, definition=definition)
        assert sorted(table.name for table in view._tables_read_by_definition()) == read

    def test_column_aliases_naming_each_other_make_no_cycle(self):
        """Two views whose column aliases are each other's names read only the base table."""
        metadata = MetaData()
        self._base(metadata)
        View("clicks", metadata, definition="SELECT id, id AS views FROM z_orders")
        View("views", metadata, definition="SELECT id, id AS clicks FROM z_orders")
        assert self._order(metadata)[0] == "z_orders"

    def test_views_named_alike_in_two_schemas_read_the_qualified_table(self):
        """Two views named orders, in reporting and archive, both read staging.orders, not each other."""
        metadata = MetaData()
        Table("orders", metadata, Column("id", Integer), schema="staging")
        View("orders", metadata, definition="SELECT id FROM staging.orders", schema="reporting")
        View("orders", metadata, definition="SELECT id FROM staging.orders", schema="archive")
        assert [table.fullname for table in metadata.sorted_tables][0] == "staging.orders"

    def test_an_unqualified_name_reads_the_views_own_schema_or_the_default(self):
        metadata = MetaData()
        own = Table("orders", metadata, Column("id", Integer), schema="reporting")
        default = Table("orders", metadata, Column("id", Integer))
        Table("orders", metadata, Column("id", Integer), schema="archive")
        view = View("v", metadata, definition="SELECT id FROM orders", schema="reporting")
        assert view._tables_read_by_definition() == {own, default}

    def test_a_cte_named_like_another_view_makes_no_cycle(self):
        metadata = MetaData()
        self._base(metadata)
        View("v_a", metadata, definition="WITH v_b AS (SELECT id FROM z_orders) SELECT id FROM v_b")
        View("v_b", metadata, definition="SELECT id FROM v_a")
        assert self._order(metadata) == ["z_orders", "v_a", "v_b"]

    def test_a_column_read_by_trim_makes_no_cycle(self):
        """``detail`` reads ``summary``, which trims a column also named ``detail``."""
        metadata = MetaData()
        self._base(metadata)
        View("summary", metadata, definition="SELECT trim(BOTH ' ' FROM detail) AS d FROM z_orders")
        View("detail", metadata, definition="SELECT d FROM summary")
        assert self._order(metadata) == ["z_orders", "summary", "detail"]

    def test_names_starting_with_a_digit_or_holding_other_letters(self):
        """StarRocks takes ``2024_sales`` and ``données`` unquoted as table names."""
        metadata = MetaData()
        sales = Table("2024_sales", metadata, Column("id", Integer))
        data = Table("données", metadata, Column("id", Integer))
        view = View("1_view", metadata, definition="SELECT id FROM 2024_sales JOIN données USING (id)")
        assert view._tables_read_by_definition() == {sales, data}
        assert [table.name for table in metadata.sorted_tables][-1] == "1_view"

    def test_name_inside_a_string_literal_is_not_a_dependency(self):
        metadata = MetaData()
        self._base(metadata)
        View("a_label_view", metadata, definition="SELECT 'z_orders' AS label")
        assert self._order(metadata) == ["a_label_view", "z_orders"]

    def test_explicit_dependency(self):
        metadata = MetaData()
        orders = self._base(metadata)
        view = View("a_constant_view", metadata, definition="SELECT 1 AS one")
        view.add_is_dependent_on(orders)
        assert self._order(metadata) == ["z_orders", "a_constant_view"]

    def test_materialized_view_on_a_view(self):
        metadata = MetaData()
        self._base(metadata)
        View("b_order_view", metadata, definition="SELECT id FROM z_orders")
        MaterializedView("a_order_mv", metadata, definition="SELECT id FROM b_order_view", starrocks_refresh="MANUAL")
        assert self._order(metadata) == ["z_orders", "b_order_view", "a_order_mv"]
