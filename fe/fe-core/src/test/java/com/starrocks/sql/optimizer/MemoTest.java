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


package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemoTest {
    @Test
    public void testInit(@Mocked OlapTable olapTable1,
                         @Mocked OlapTable olapTable2) {
        new Expectations() {
            {
                olapTable1.getId();
                result = 0;
                minTimes = 0;

                olapTable2.getId();
                result = 1;
                minTimes = 0;
            }
        };

        OptExpression expr = OptExpression.create(new LogicalProjectOperator(Maps.newHashMap()),
                OptExpression.create(new LogicalJoinOperator(),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable1)),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable2))));

        Memo memo = new Memo();
        GroupExpression groupExpression = memo.init(expr);

        assertEquals(OperatorType.LOGICAL_PROJECT, groupExpression.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_JOIN,
                groupExpression.inputAt(0).getFirstLogicalExpression().getOp().getOpType());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN,
                groupExpression.inputAt(0).getFirstLogicalExpression().inputAt(0)
                        .getFirstLogicalExpression().getOp()
                        .getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN,
                groupExpression.inputAt(0).getFirstLogicalExpression().inputAt(1)
                        .getFirstLogicalExpression().getOp()
                        .getOpType());

        assertEquals(memo.getGroups().size(), 4);
        assertEquals(memo.getGroupExpressions().size(), 4);

        assertEquals(memo.getGroups().get(0).getId(), 0);
        assertEquals(memo.getGroups().get(1).getId(), 1);
        assertEquals(memo.getGroups().get(2).getId(), 2);
        assertEquals(memo.getGroups().get(3).getId(), 3);
    }

    @Test
    public void testInsertGroupExpression(@Mocked OlapTable olapTable1,
                                          @Mocked OlapTable olapTable2) {
        new Expectations() {
            {
                olapTable1.getId();
                result = 0;
                minTimes = 0;

                olapTable2.getId();
                result = 1;
                minTimes = 0;
            }
        };

        OptExpression expr = OptExpression.create(new LogicalProjectOperator(Maps.newHashMap()),
                OptExpression.create(new LogicalJoinOperator(),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable1)),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable2))));

        Memo memo = new Memo();
        memo.init(expr);

        Operator projectOperator = LogicalLimitOperator.init(1, 1);
        GroupExpression newGroupExpression = new GroupExpression(projectOperator, Lists.newArrayList());

        memo.insertGroupExpression(newGroupExpression, memo.getGroups().get(3));

        assertEquals(memo.getGroups().size(), 4);
        assertEquals(memo.getGroupExpressions().size(), 5);
        assertEquals(memo.getGroups().get(3).getLogicalExpressions().size(), 2);
        assertEquals(memo.getGroups().get(3).getPhysicalExpressions().size(), 0);
    }

}
