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

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

// Marks an Iceberg TopN scan (ORDER BY <col> [ASC|DESC] LIMIT k) so the BE can use the leading sort
// key's min/max per file: it reorders the morsels so the best files come first (the TopN runtime
// filter then gets its bound early), and drops files whose min/max cannot beat that filter before
// their footer is read.
//
// Like MinMaxOptOnScanRule, the rule only sets a flag on ScanOptimizeOption and returns an empty
// result (no structural change). It must run after IcebergEqualityDeleteRewriteRule (see
// QueryOptimizer) so it never marks an equality-delete-rewritten scan; the fromEqDeleteRewriteRule
// check below is a second guard.
public class IcebergTopNScanPruneRule extends TransformationRule {
    public IcebergTopNScanPruneRule() {
        super(RuleType.TF_ICEBERG_TOPN_SCAN_PRUNE,
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.LOGICAL_ICEBERG_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableIcebergTopnScanPruning()) {
            return false;
        }

        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();
        // Only useful when the TopN runtime filter is actually built (see SortNode.buildRuntimeFilters):
        // there is a LIMIT, the RF is enabled, and this is not the agg-pushdown TopN path (there the
        // aggregate builds the filter, not the sort).
        if (!topn.hasLimit() || topn.getLimit() < 0) {
            return false;
        }
        if (!context.getSessionVariable().getEnableTopNRuntimeFilter()) {
            return false;
        }
        if (topn.isTopNPushDownAgg()) {
            return false;
        }

        List<Ordering> orderings = topn.getOrderByElements();
        if (orderings == null || orderings.isEmpty()) {
            return false;
        }
        ColumnRefOperator key = orderings.get(0).getColumnRef();
        if (key == null) {
            return false;
        }

        LogicalScanOperator scan = (LogicalScanOperator) input.getInputs().get(0).getOp();
        if (((LogicalIcebergScanOperator) scan).isFromEqDeleteRewriteRule()) {
            return false;
        }

        // The leading key must be a real file column: in the scan's column map, not a partition
        // column, and an int/date/datetime type whose bound fits TExprMinMaxValue (int64). Float,
        // decimal, string and largeint are excluded.
        Column column = scan.getColRefToColumnMetaMap().get(key);
        if (column == null) {
            return false;
        }
        if (scan.getPartitionColumns().contains(column.getName())) {
            return false;
        }
        return key.getType().isIntegerType() || key.getType().isDateType();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();
        LogicalScanOperator scan = (LogicalScanOperator) input.getInputs().get(0).getOp();
        Ordering leading = topn.getOrderByElements().get(0);
        scan.getScanOptimizeOption().setTopnReorder(
                leading.getColumnRef(), !leading.isAscending(), leading.isNullsFirst());
        return Collections.emptyList();
    }
}
