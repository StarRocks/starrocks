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


package com.starrocks.load;

import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.ImportWhereStmt;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RoutineLoadDescTest {
    @Test
    public void testToSql() throws Exception {
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo("CREATE ROUTINE LOAD job ON tbl " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c`=1), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "WHERE a = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"3\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);

        RoutineLoadDesc desc = new RoutineLoadDesc();
        // set column separator and check
        desc.setColumnSeparator(originLoad.getColumnSeparator());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';'", desc.toSql());
        // set row delimiter and check
        desc.setRowDelimiter(originLoad.getRowDelimiter());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n'", desc.toSql());
        // set columns and check
        desc.setColumnsInfo(originLoad.getColumnsInfo());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1)", desc.toSql());
        // set partitions and check
        desc.setPartitionNames(originLoad.getPartitionNames());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), " +
                        "TEMPORARY PARTITION(`p1`, `p2`)",
                desc.toSql());
        // set where and check
        desc.setWherePredicate(originLoad.getWherePredicate());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), " +
                        "TEMPORARY PARTITION(`p1`, `p2`), " +
                        "WHERE `a` = 1",
                desc.toSql());
    }

    @Test
    public void testPrecedingFilter() {
        String sql = "CREATE ROUTINE LOAD job ON tbl " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c`=1), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "PRECEDING FILTER a = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"3\") " +
                "FROM KAFKA\n"
                + "(\n"
                + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                + "\"kafka_topic\" = \"topictest\"\n"
                + ");";
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(sql), null);
        Assertions.assertNotNull(originLoad);
        ImportWhereStmt precedingFilter = originLoad.getPrecedingFilter();
        Assertions.assertTrue(precedingFilter.isPrecedingFilter());
        Expr expr = precedingFilter.getExpr();
        Assertions.assertInstanceOf(BinaryPredicate.class, expr);
        Assertions.assertEquals(BinaryType.EQ, ((BinaryPredicate) expr).getOp());
        List<Expr> children = expr.getChildren();
        Assertions.assertEquals(2, children.size());
        Assertions.assertInstanceOf(SlotRef.class, children.get(0));
        Assertions.assertEquals("a", ((SlotRef) children.get(0)).getColName());
        Assertions.assertInstanceOf(IntLiteral.class, children.get(1));
        Assertions.assertEquals(1, ((IntLiteral) children.get(1)).getValue());
    }
}
