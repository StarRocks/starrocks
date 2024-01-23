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

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

// transform empty scan to empty values
public class PruneEmptyScanRule extends TransformationRule {
    public static final PruneEmptyScanRule OLAP_SCAN = new PruneEmptyScanRule(OperatorType.LOGICAL_OLAP_SCAN);
    public static final PruneEmptyScanRule HIVE_SCAN = new PruneEmptyScanRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final PruneEmptyScanRule HUDI_SCAN = new PruneEmptyScanRule(OperatorType.LOGICAL_HUDI_SCAN);
    public static final PruneEmptyScanRule ICEBERG_SCAN = new PruneEmptyScanRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    public static final PruneEmptyScanRule PAIMON_SCAN = new PruneEmptyScanRule(OperatorType.LOGICAL_PAIMON_SCAN);
    public static final PruneEmptyScanRule ODPS_SCAN = new PruneEmptyScanRule(OperatorType.LOGICAL_ODPS_SCAN);
    private PruneEmptyScanRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PRUNE_EMPTY_SCAN, Pattern.create(logicalOperatorType));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!FeConstants.enablePruneEmptyOutputScan) {
            return false;
        }

        LogicalScanOperator scan = input.getOp().cast();
        return scan.isEmptyOutputRows();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<ColumnRefOperator> refs =
                input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        return Lists.newArrayList(OptExpression.create(new LogicalValuesOperator(refs, Lists.newArrayList())));
    }
}
