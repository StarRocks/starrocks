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


package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import mockit.Mocked;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PushDownMinMaxConjunctsRuleTest {
    @Test
    public void transformIceberg(@Mocked IcebergTable table) {
        ExternalScanPartitionPruneRule rule0 = ExternalScanPartitionPruneRule.ICEBERG_SCAN;

        PredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(
                BinaryType.EQ, new ColumnRefOperator(1, Type.INT, "id", true),
                ConstantOperator.createInt(1));

        OptExpression scan =
                new OptExpression(new LogicalIcebergScanOperator(table,
                                Maps.newHashMap(), Maps.newHashMap(), -1, binaryPredicateOperator));
        scan.getInputs().add(scan);

        assertEquals(0, ((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());

        rule0.transform(scan, new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(2, ((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());
    }
}
