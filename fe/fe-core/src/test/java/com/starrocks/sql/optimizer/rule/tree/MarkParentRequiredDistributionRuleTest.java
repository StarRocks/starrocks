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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MarkParentRequiredDistributionRuleTest extends PlanTestBase {

    @Test
    public void testJoin() throws Exception {
        String sql = "select count(*) from t1 join t2 on t1.v4 = t2.v7";
        ExecPlan execPlan = getExecPlan(sql);
        Assertions.assertTrue(execPlan.getOptExpression(3).getOp() instanceof PhysicalHashJoinOperator);
        Assertions.assertFalse(execPlan.getOptExpression(3).isExistRequiredDistribution(),
                "No global dict exists. No requirement from parent, " +
                        "we can change the output distribution of this join");

        sql = "select count(*) from t1 join t2 on t1.v4 = t2.v7 group by t1.v4";
        execPlan = getExecPlan(sql);
        Assertions.assertTrue(execPlan.getOptExpression(4).getOp() instanceof PhysicalHashJoinOperator);
        Assertions.assertTrue(execPlan.getOptExpression(4).isExistRequiredDistribution(),
                "Had requirement from parent, we can't change the output distribution of this join");

        sql = "select count(*) from t1 join t2 on t1.v4 = t2.v7 join t3 on t1.v4 = t3.v11";
        execPlan = getExecPlan(sql);
        Assertions.assertTrue(execPlan.getOptExpression(4).getOp() instanceof PhysicalHashJoinOperator);
        Assertions.assertTrue(execPlan.getOptExpression(8).getOp() instanceof PhysicalHashJoinOperator);

        Assertions.assertTrue(execPlan.getOptExpression(4).isExistRequiredDistribution(),
                "Had requirement from parent, we can't change the output distribution of this join");
        Assertions.assertFalse(execPlan.getOptExpression(8).isExistRequiredDistribution(),
                "No global dict exists. No requirement from parent, " +
                        "we can't change the output distribution of this join");
    }

    @Test
    public void testOrderBy() throws Exception {
        String sql = "select * from (select * from t1 order by v4 limit 50) t where v5 = 1";
        ExecPlan execPlan = getExecPlan(sql);

        PhysicalTopNOperator partitionTopN = (PhysicalTopNOperator) execPlan.getOptExpression(1).getOp();
        PhysicalTopNOperator finalTopN = (PhysicalTopNOperator) execPlan.getOptExpression(2).getOp();
        Assertions.assertTrue(partitionTopN.getSortPhase().isPartial());
        Assertions.assertTrue(finalTopN.getSortPhase().isFinal());
        Assertions.assertTrue(execPlan.getOptExpression(1).isExistRequiredDistribution());
        Assertions.assertFalse(execPlan.getOptExpression(2).isExistRequiredDistribution());
    }

}