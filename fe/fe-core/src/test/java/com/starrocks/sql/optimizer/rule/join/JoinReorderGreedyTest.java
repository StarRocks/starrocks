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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class JoinReorderGreedyTest {

    private JoinOrder.GroupInfo createGroupInfoWithCost(int atomIndex, double cost) {
        final var atoms = new BitSet();
        atoms.set(atomIndex);
        JoinOrder.GroupInfo groupInfo = new JoinOrder.GroupInfo(atoms);
        JoinOrder.ExpressionInfo exprInfo = new JoinOrder.ExpressionInfo(null);
        exprInfo.cost = cost;
        groupInfo.bestExprInfo = exprInfo;
        groupInfo.lowestExprCost = cost;
        return groupInfo;
    }

    @Test
    public void testGetBestGroupInfoWithMaxValueCosts() {
        // GIVEN
        final var greedy = new JoinReorderGreedy(OptimizerFactory.mockContext(new ColumnRefFactory()));
        final var group0 = createGroupInfoWithCost(0, Double.MAX_VALUE);
        final var group1 = createGroupInfoWithCost(1, Double.MAX_VALUE);
        final var groupInfos = List.of(group0, group1);

        // WHEN
        final var best = greedy.getBestGroupInfo(groupInfos);

        // THEN
        assertNotNull(best);
        assertSame(group0, best);
    }
}


