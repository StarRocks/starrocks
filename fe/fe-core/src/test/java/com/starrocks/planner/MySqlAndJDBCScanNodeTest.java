// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.starrocks.planner;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LargeStringLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.sql.parser.NodePosition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MySqlAndJDBCScanNodeTest {

    private List<Expr> createConjuncts() {
        Expr slotRef = new SlotRef("col", new SlotDescriptor(new SlotId(1), "col", Type.VARCHAR, true));
        Expr expr0 = new InPredicate(slotRef,
                Lists.newArrayList(new LargeStringLiteral(Strings.repeat("ABCDE", 11), NodePosition.ZERO)), true);
        Expr expr1 = new BinaryPredicate(BinaryType.EQ, slotRef, StringLiteral.create("ABC"));
        Expr expr2 = new CompoundPredicate(CompoundPredicate.Operator.OR, expr0, expr1);
        return Lists.newArrayList(expr0, expr1, expr2);
    }

    @Test
    public void testFiltersInMySQLScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("host", "127.0.0.1");
        properties.put("port", "3036");
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("database", "test_db");
        properties.put("table", "test_table");
        MysqlTable mysqlTable = new MysqlTable(1, "mysql_table",
                Collections.singletonList(new Column("col", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        MysqlScanNode scanNode = new MysqlScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.getConjuncts().addAll(createConjuncts());
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("SELECT * FROM `test_table` " +
                "WHERE (col NOT IN ('ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE')) " +
                "AND (col = 'ABC') AND " +
                "((col NOT IN ('ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE')) OR " +
                "(col = 'ABC'))"), nodeString);
    }

    @Test
    public void testFiltersInJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "driver_class");
        JDBCTable mysqlTable = new JDBCTable(1, "jdbc_table",
                Collections.singletonList(new Column("col", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.getConjuncts().addAll(createConjuncts());
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("SELECT * FROM `jdbc_table` WHERE " +
                "(`col` NOT IN ('ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE')) AND " +
                "(`col` = 'ABC') AND ((`col` NOT IN ('ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE')) " +
                "OR (`col` = 'ABC'))\n"), nodeString);
    }

    @Test
    public void testFiltersInPostgreSQLJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.postgres.Driver");
        JDBCTable pgTable = new JDBCTable(1, "order",
                Collections.singletonList(new Column("user", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: \"order\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"order\""), nodeString);
    }

    @Test
    public void testFiltersInPostgresShortURIJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgres://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.postgres.Driver");
        JDBCTable pgTable = new JDBCTable(1, "order",
                Collections.singletonList(new Column("user", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: \"order\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"order\""), nodeString);
    }

    @Test
    public void testFiltersInOracleJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "oracle");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:oracle:thin:@localhost:1521:orcl");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "oracle.jdbc.driver.OracleDriver");
        JDBCTable oracleTable = new JDBCTable(1, "select",
                Collections.singletonList(new Column("group", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(oracleTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, oracleTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: \"select\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"select\""), nodeString);
    }

    @Test
    public void testFiltersInSqlServerJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "sa");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:sqlserver://localhost:1433;databaseName=testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        JDBCTable sqlServerTable = new JDBCTable(1, "table",
                Collections.singletonList(new Column("index", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(sqlServerTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, sqlServerTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: \"table\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"table\""), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForMySQL() throws DdlException {
        // Test MySQL with backticks
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        JDBCTable mysqlTable = new JDBCTable(1, "test_table",
                Collections.singletonList(new Column("col1", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        // Should wrap table name with backticks
        Assertions.assertTrue(nodeString.contains("TABLE: `test_table`"), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM `test_table`"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForSchemaQualifiedTable() throws DdlException {
        // Test PostgreSQL with schema-qualified table name
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.postgresql.Driver");
        JDBCTable pgTable = new JDBCTable(1, "public.users",
                Collections.singletonList(new Column("id", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        // Should wrap each part with double quotes
        Assertions.assertTrue(nodeString.contains("TABLE: \"public\".\"users\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"public\".\"users\""), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForAlreadyWrappedTable() throws DdlException {
        // Test that already wrapped identifiers are not double-wrapped
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        JDBCTable mysqlTable = new JDBCTable(1, "`test_table`",
                Collections.singletonList(new Column("col1", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        // Should not double-wrap
        Assertions.assertTrue(nodeString.contains("TABLE: `test_table`"), nodeString);
        Assertions.assertFalse(nodeString.contains("``test_table``"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForMariaDB() throws DdlException {
        // Test MariaDB with backticks
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mariadb://localhost:3306/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.mariadb.jdbc.Driver");
        JDBCTable mariadbTable = new JDBCTable(1, "test_table",
                Collections.singletonList(new Column("col1", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mariadbTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mariadbTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: `test_table`"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForClickHouse() throws DdlException {
        // Test ClickHouse with backticks
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "default");
        properties.put("password", "");
        properties.put("jdbc_uri", "jdbc:clickhouse://localhost:8123/default");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "ru.yandex.clickhouse.ClickHouseDriver");
        JDBCTable clickhouseTable = new JDBCTable(1, "events",
                Collections.singletonList(new Column("event_id", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(clickhouseTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, clickhouseTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: `events`"), nodeString);
    }

    @Test
    public void testCreateJDBCTableColumnsWithMultipleColumns() throws DdlException {
        // Test multiple columns are properly wrapped
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.postgresql.Driver");
        List<Column> columns = Lists.newArrayList(
                new Column("id", Type.VARCHAR),
                new Column("name", Type.VARCHAR),
                new Column("age", Type.VARCHAR)
        );
        JDBCTable pgTable = new JDBCTable(1, "users", columns, properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        SlotDescriptor slot1 = new SlotDescriptor(new SlotId(1), "id", Type.VARCHAR, true);
        slot1.setColumn(columns.get(0));
        slot1.setIsMaterialized(true);
        tupleDesc.addSlot(slot1);
        SlotDescriptor slot2 = new SlotDescriptor(new SlotId(2), "name", Type.VARCHAR, true);
        slot2.setColumn(columns.get(1));
        slot2.setIsMaterialized(true);
        tupleDesc.addSlot(slot2);
        SlotDescriptor slot3 = new SlotDescriptor(new SlotId(3), "age", Type.VARCHAR, true);
        slot3.setColumn(columns.get(2));
        slot3.setIsMaterialized(true);
        tupleDesc.addSlot(slot3);

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        // Should have all columns wrapped with double quotes
        Assertions.assertTrue(nodeString.contains("\"id\", \"name\", \"age\""), nodeString);
    }

    @Test
    public void testCreateJDBCTableColumnsWithAlreadyWrappedColumnName() throws DdlException {
        // Test column that already has identifier symbols
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        List<Column> columns = Lists.newArrayList(
                new Column("`select`", Type.VARCHAR)
        );
        JDBCTable mysqlTable = new JDBCTable(1, "test_table", columns, properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), "`select`", Type.VARCHAR, true);
        slot.setColumn(columns.get(0));
        slot.setIsMaterialized(true);
        tupleDesc.addSlot(slot);

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        // Should not double-wrap the column name
        Assertions.assertTrue(nodeString.contains("SELECT `select`"), nodeString);
        Assertions.assertFalse(nodeString.contains("``select``"), nodeString);
    }

    @Test
    public void testCreateJDBCTableColumnsForCountStar() throws DdlException {
        // Test count(*) scenario where no columns are materialized
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        JDBCTable mysqlTable = new JDBCTable(1, "test_table",
                Collections.singletonList(new Column("col1", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        // Don't add any materialized slots to simulate count(*)

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        // Should use SELECT *
        Assertions.assertTrue(nodeString.contains("SELECT *"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForComplexSchemaPath() throws DdlException {
        // Test database.schema.table format
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.postgresql.Driver");
        JDBCTable pgTable = new JDBCTable(1, "mydb.public.users",
                Collections.singletonList(new Column("id", Type.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        // Should wrap each part separately
        Assertions.assertTrue(nodeString.contains("\"mydb\".\"public\".\"users\""), nodeString);
    }
}