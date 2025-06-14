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

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LargeStringLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

public class JDBCTableTest {

    @Test
    public void testJDBCTableNameClass() {
        try {
            JDBCTableName jdbcTableName = new JDBCTableName("catalog", "db", "tbl");
            Assert.assertTrue(jdbcTableName.getCatalogName().equals("catalog"));
            Assert.assertTrue(jdbcTableName.getDatabaseName().equals("db"));
            Assert.assertTrue(jdbcTableName.getTableName().equals("tbl"));
            Assert.assertTrue(jdbcTableName.toString().contains("tbl"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testJDBCPartitionClass() {
        try {
            Partition partition = new Partition("20230810", 1000L);
            Assert.assertTrue(partition.equals(partition));
            Assert.assertTrue(partition.hashCode() == Objects.hash("20230810", 1000L));
            Assert.assertTrue(partition.toString().contains("20230810"));
            Assert.assertTrue(partition.toJson().toString().contains("20230810"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testJDBCPredicateRewrite() {
        {
            Expr left = new SlotRef(new TableName("db", "tbl"), "k1");
            Expr right = new LargeStringLiteral("main_interface_of_live#all_module#null#write_real_time_start#0",
                    NodePosition.ZERO);
            Expr expr = new BinaryPredicate(BinaryType.EQ, left, right);
            String str = AstToStringBuilder.toString(expr);
            Assert.assertEquals(str, "db.tbl.k1 = 'main_interface_of_live#all_module#null#write_real_time_start#0'");
        }

        {
            Expr left = new SlotRef(new TableName("db", "tbl"), "k1");
            Expr right = new StringLiteral("123", NodePosition.ZERO);
            Expr expr = new BinaryPredicate(BinaryType.LE, left, right);
            String str = AstToStringBuilder.toString(expr);
            Assert.assertEquals(str, "db.tbl.k1 <= '123'");
        }
    }
}
