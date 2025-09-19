// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class GroupByClauseTest {


    @BeforeEach
    public void setUp() {
    }

    @Test
    public void testGroupBy() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        Assertions.assertEquals("`testdb`.`t`.`k2`, `testdb`.`t`.`k2`, `testdb`.`t`.`k3`, `testdb`.`t`.`k1`",
                groupByClause.toSql());
        Assertions.assertEquals(3, groupByClause.getGroupingExprs().size());
        groupingExprs.remove(0);
        Assertions.assertEquals(groupByClause.getGroupingExprs(), groupingExprs);
    }

    @Test
    public void testReset() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        try {
            groupByClause.reset();
        } catch (Exception e) {
            Assertions.fail("reset throw exceptions!" + e);
        }
    }

    @Test
    public void testGetTuple() throws AnalysisException {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k1", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }
        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
    }
}
