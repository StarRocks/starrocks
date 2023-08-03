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

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mocked;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OlapScanImplementationRuleTest {

    @Test
    public void transform(@Mocked OlapTable table) {
        LogicalOlapScanOperator logical = new LogicalOlapScanOperator(table, Maps.newHashMap(), Maps.newHashMap(),
                null, -1, ConstantOperator.createBoolean(true),
                1, Lists.newArrayList(1L, 2L, 3L), null,
                Lists.newArrayList(4L), null);

        List<OptExpression> output =
                new OlapScanImplementationRule().transform(new OptExpression(logical), new OptimizerContext(
                        new Memo(), new ColumnRefFactory()));

        assertEquals(1, output.size());

        PhysicalOlapScanOperator physical = (PhysicalOlapScanOperator) output.get(0).getOp();
        assertEquals(1, physical.getSelectedIndexId());

        assertEquals(3, physical.getSelectedPartitionId().size());
        assertEquals(1, physical.getSelectedTabletId().size());
        assertEquals(ConstantOperator.createBoolean(true), physical.getPredicate());
    }

}