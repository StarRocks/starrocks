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

package com.starrocks.connector.jdbc;

import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LargeStringLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class JDBCTableTest {

    @Test
    public void testJDBCTableNameClass() {
        try {
            JDBCTableName jdbcTableName = new JDBCTableName("catalog", "db", "tbl");
            Assertions.assertTrue(jdbcTableName.getCatalogName().equals("catalog"));
            Assertions.assertTrue(jdbcTableName.getDatabaseName().equals("db"));
            Assertions.assertTrue(jdbcTableName.getTableName().equals("tbl"));
            Assertions.assertTrue(jdbcTableName.toString().contains("tbl"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testJDBCPartitionClass() {
        try {
            Partition partition = new Partition("20230810", 1000L);
            Assertions.assertTrue(partition.equals(partition));
            Assertions.assertTrue(partition.hashCode() == Objects.hash("20230810", 1000L));
            Assertions.assertTrue(partition.toString().contains("20230810"));
            Assertions.assertTrue(partition.toJson().toString().contains("20230810"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    private JDBCTable buildJDBCTable(String uri) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(JDBCResource.URI, uri);
        props.put(JDBCResource.USER, "root");
        props.put(JDBCResource.PASSWORD, "");
        props.put(JDBCResource.DRIVER_URL, "https://example.com/driver.jar");
        props.put(JDBCResource.CHECK_SUM, "abc");
        props.put(JDBCResource.DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        return new JDBCTable(1L, "test", null, props);
    }

    @Test
    public void testIsMySQLCompatible() throws Exception {
        Assertions.assertTrue(buildJDBCTable("jdbc:mysql://host:3306/db").isMySQLCompatible());
        Assertions.assertTrue(buildJDBCTable("jdbc:mariadb://host:3306/db").isMySQLCompatible());
        Assertions.assertFalse(buildJDBCTable("jdbc:postgresql://host:5432/db").isMySQLCompatible());
        Assertions.assertFalse(buildJDBCTable("jdbc:oracle:thin:@host:1521:db").isMySQLCompatible());
        Assertions.assertFalse(buildJDBCTable("jdbc:sqlserver://host:1433").isMySQLCompatible());
    }

    @Test
    public void testJDBCPredicateRewrite() {
        {
            Expr left = new SlotRef(new TableName("db", "tbl"), "k1");
            Expr right = new LargeStringLiteral("main_interface_of_live#all_module#null#write_real_time_start#0",
                    NodePosition.ZERO);
            Expr expr = new BinaryPredicate(BinaryType.EQ, left, right);
            String str = AstToStringBuilder.toString(expr);
            Assertions.assertEquals(str, "db.tbl.k1 = 'main_interface_of_live#all_module#null#write_real_time_start#0'");
        }

        {
            Expr left = new SlotRef(new TableName("db", "tbl"), "k1");
            Expr right = new StringLiteral("123", NodePosition.ZERO);
            Expr expr = new BinaryPredicate(BinaryType.LE, left, right);
            String str = AstToStringBuilder.toString(expr);
            Assertions.assertEquals(str, "db.tbl.k1 <= '123'");
        }
    }
}
