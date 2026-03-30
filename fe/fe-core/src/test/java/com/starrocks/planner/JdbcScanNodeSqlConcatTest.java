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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.VarcharType;
import com.starrocks.thrift.TJDBCScanNode;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Verify {@link JDBCScanNode#toThrift} sets {@code TJDBCScanNode.sql} with correct dialect-specific
 * limit handling (SQL Server TOP, Oracle ROWNUM subquery, generic LIMIT).
 */
public class JdbcScanNodeSqlConcatTest {

    private JDBCScanNode createAndPrepareScanNode(String jdbcUri, long limit) throws DdlException {
        JDBCTable table = createJdbcTable(jdbcUri);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(table);

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, table);

        // Add a simple conjunct so the generated sql includes WHERE clause.
        SlotRef slotRef = new SlotRef(
                "col",
                new SlotDescriptor(new SlotId(1), "col", VarcharType.VARCHAR, true));
        Expr predicate = new BinaryPredicate(BinaryType.EQ, slotRef, StringLiteral.create("ABC"));
        scanNode.getConjuncts().add(predicate);
        scanNode.computeColumnsAndFilters();
        scanNode.setLimit(limit);

        return scanNode;
    }

    private JDBCTable createJdbcTable(String jdbcUri) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        // JDBCScanNode's getJdbcUri() reads JDBCResource.URI from table connect info.
        properties.put("jdbc_uri", jdbcUri);
        // Unused by sql generation, but required by some table constructors.
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "driver_class");

        return new JDBCTable(
                1,
                "jdbc_table",
                Collections.singletonList(new Column("col", VarcharType.VARCHAR)),
                properties);
    }

    @Test
    public void testSqlServerTopWithLimit() throws Exception {
        JDBCScanNode scanNode = createAndPrepareScanNode(
                "jdbc:sqlserver://localhost:1433;databaseName=testdb", 5);

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        TJDBCScanNode jdbcScanNode = planNode.jdbc_scan_node;

        String sql = jdbcScanNode.getSql();
        Assertions.assertNotNull(sql);
        Assertions.assertFalse(sql.contains(" LIMIT "));
        Assertions.assertFalse(sql.contains("ROWNUM"));

        // Must have whitespace between TOP(...) and select list.
        Assertions.assertTrue(sql.matches("(?s).*SELECT\\s+TOP\\(5\\)\\s+.*FROM\\s+jdbc_table.*"));
        Assertions.assertTrue(sql.contains("WHERE"));
        Assertions.assertTrue(sql.contains("col = 'ABC'"));
    }

    @Test
    public void testOracleRowNumWithLimit() throws Exception {
        JDBCScanNode scanNode = createAndPrepareScanNode(
                "jdbc:oracle:thin:@localhost:1521:orcl", 7);

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        TJDBCScanNode jdbcScanNode = planNode.jdbc_scan_node;

        String sql = jdbcScanNode.getSql();
        Assertions.assertNotNull(sql);
        Assertions.assertFalse(sql.contains(" LIMIT "));

        Assertions.assertTrue(sql.contains("SELECT * FROM ("));
        Assertions.assertTrue(sql.contains(") WHERE ROWNUM <= 7"));
        Assertions.assertTrue(sql.contains("col = 'ABC'"));
    }

    @Test
    public void testGenericJdbcLimit() throws Exception {
        JDBCScanNode scanNode = createAndPrepareScanNode(
                "jdbc:mysql://localhost:3306", 3);

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        TJDBCScanNode jdbcScanNode = planNode.jdbc_scan_node;

        String sql = jdbcScanNode.getSql();
        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains(" LIMIT 3"));
        Assertions.assertFalse(sql.contains("TOP("));
        Assertions.assertFalse(sql.contains("ROWNUM"));
        Assertions.assertTrue(sql.contains("`jdbc_table`"));
        Assertions.assertTrue(sql.contains("`col` = 'ABC'"));
    }

    @Test
    public void testPostgreSqlLimitWithDoubleQuotedIdentifiers() throws Exception {
        JDBCScanNode scanNode = createAndPrepareScanNode(
                "jdbc:postgresql://localhost:5432/db", 4);

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        String sql = planNode.jdbc_scan_node.getSql();

        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains(" LIMIT 4"));
        Assertions.assertFalse(sql.contains("TOP("));
        Assertions.assertFalse(sql.contains("ROWNUM"));
        Assertions.assertTrue(sql.contains("\"jdbc_table\""));
        Assertions.assertTrue(sql.contains("\"col\" = 'ABC'"));
    }

    @Test
    public void testWithoutLimitOmitsLimitTopAndRownum() throws Exception {
        JDBCTable table = createJdbcTable("jdbc:mysql://localhost:3306");
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(table);

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, table);
        SlotRef slotRef = new SlotRef(
                "col",
                new SlotDescriptor(new SlotId(1), "col", VarcharType.VARCHAR, true));
        Expr predicate = new BinaryPredicate(BinaryType.EQ, slotRef, StringLiteral.create("ABC"));
        scanNode.getConjuncts().add(predicate);
        scanNode.computeColumnsAndFilters();

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        String sql = planNode.jdbc_scan_node.getSql();

        Assertions.assertNotNull(sql);
        Assertions.assertFalse(sql.contains(" LIMIT "));
        Assertions.assertFalse(sql.contains("TOP("));
        Assertions.assertFalse(sql.contains("ROWNUM"));
        Assertions.assertTrue(sql.contains("`jdbc_table`"));
        Assertions.assertTrue(sql.contains("`col` = 'ABC'"));
    }
}

