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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LargeStringLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
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
}