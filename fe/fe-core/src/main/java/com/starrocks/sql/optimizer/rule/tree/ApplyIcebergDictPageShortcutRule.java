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

import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Set;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

// Marks PhysicalIcebergScanOperator with dictPageShortcutHint=true when the plan is
// a pure single-column DISTINCT / GROUP BY over an iceberg-parquet scan with no
// predicate. The hint asks BE to consider emitting the column's dict-page values
// directly. BE applies per-RG safety gates before using the shortcut.
//
// Must run after low-cardinality / decode rewrites so that the scan operator and
// group-by column refs are stable.
public class ApplyIcebergDictPageShortcutRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (!ConnectContext.get().getSessionVariable().isEnableIcebergDictPageShortcut()) {
            return root;
        }
        root.getOp().accept(new Visitor(), root, false);
        return root;
    }

    private static class Visitor extends OptExpressionVisitor<Void, Boolean> {
        @Override
        public Void visit(OptExpression optExpression, Boolean limitAbove) {
            // A LIMIT on any ancestor caps the distinct/group-by output. `select distinct c
            // ... limit 5` lands the limit on the agg's parent project, not the agg itself,
            // so checking the agg alone misses it. Carry the signal down to tryMark.
            boolean limitSeen = Boolean.TRUE.equals(limitAbove) || optExpression.getOp().hasLimit();
            if (optExpression.getOp().getOpType() == OperatorType.PHYSICAL_HASH_AGG) {
                tryMark(optExpression, limitSeen);
            }
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, limitSeen);
            }
            return null;
        }

        private void tryMark(OptExpression aggExpr, boolean limitAbove) {
            PhysicalHashAggregateOperator agg = (PhysicalHashAggregateOperator) aggExpr.getOp();

            // distinct / pure group-by — no aggregate functions
            if (agg.getAggregations() != null && !agg.getAggregations().isEmpty()) {
                return;
            }
            // PR1 — single group-by key
            if (agg.getGroupBys() == null || agg.getGroupBys().size() != 1) {
                return;
            }
            // agg with predicate (HAVING) — bail
            if (agg.getPredicate() != null) {
                return;
            }
            // A LIMIT on the distinct/group-by — whether carried on the agg itself or, as with
            // `select distinct c ... limit 5`, on a parent project above it — caps the row set,
            // but the shortcut emits the whole column-chunk dictionary, so the capped result
            // would be wrong. limitAbove folds in both the agg's own limit and any ancestor's.
            if (limitAbove) {
                return;
            }
            ColumnRefOperator groupByCol = agg.getGroupBys().get(0);

            // Walk down only through identity Project operators. Any other unary op
            // (TopN, Sort, Limit, Filter, etc.) changes the row set the agg sees, so
            // dict-page values from the scan would be the wrong input.
            OptExpression cur = aggExpr;
            while (cur.getInputs().size() == 1
                    && cur.getInputs().get(0).getOp().getOpType() != OperatorType.PHYSICAL_ICEBERG_SCAN) {
                OptExpression child = cur.getInputs().get(0);
                if (child.getOp().getOpType() != OperatorType.PHYSICAL_PROJECT) {
                    return;
                }
                if (!isIdentityProjection(child)) {
                    return;
                }
                // a LIMIT carried on the intermediate project caps the row set too
                if (child.getOp().hasLimit()) {
                    return;
                }
                cur = child;
            }
            if (cur.getInputs().size() != 1) {
                return;
            }
            OptExpression scanExpr = cur.getInputs().get(0);
            if (scanExpr.getOp().getOpType() != OperatorType.PHYSICAL_ICEBERG_SCAN) {
                return;
            }
            PhysicalIcebergScanOperator scan = (PhysicalIcebergScanOperator) scanExpr.getOp();

            // any per-row predicate on the scan disqualifies us — BE cannot prove
            // the RG-level filter alone satisfies the predicate for every surviving row.
            if (scan.getPredicate() != null) {
                return;
            }
            // scan-level LIMIT changes the row set in ways dict-page values cannot
            // reproduce (we'd emit the full RG dict instead of the first N rows).
            if (scan.hasLimit()) {
                return;
            }

            IcebergTable table = (IcebergTable) scan.getTable();
            // parquet only
            String fileFormat = table.getNativeTable().properties()
                    .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
            if (!"parquet".equalsIgnoreCase(fileFormat)) {
                return;
            }

            Set<String> partitionNames = Set.copyOf(table.getPartitionColumnNames());

            // scan must read exactly one non-partition data slot, and that slot must
            // equal the group-by column. partition slots are fine (they materialize
            // outside the parquet file).
            List<ColumnRefOperator> nonPartitionSlots = Lists.newArrayList();
            for (ColumnRefOperator col : scan.getColRefToColumnMetaMap().keySet()) {
                if (!partitionNames.contains(col.getName())) {
                    nonPartitionSlots.add(col);
                }
            }
            if (nonPartitionSlots.size() != 1) {
                return;
            }
            // pure partition-col DISTINCT — there's a separate manifest-based path
            // (use_partition_column_value_only); do not steal it.
            if (partitionNames.contains(groupByCol.getName())) {
                return;
            }
            ColumnRefOperator dataSlot = nonPartitionSlots.get(0);
            if (dataSlot.getId() != groupByCol.getId()) {
                return;
            }

            // projection on scan must not transform the data slot (we feed raw values
            // to the agg; any transform makes dict-page values invalid).
            if (scan.getProjection() != null
                    && scan.getProjection().getColumnRefMap() != null
                    && !scan.getProjection().getColumnRefMap().isEmpty()
                    && !isIdentityProjection(scanExpr)) {
                return;
            }

            scan.getScanOptimizeOption().setDictPageShortcutHint(true);
        }

        private static boolean isIdentityProjection(OptExpression expr) {
            if (expr.getOp().getProjection() == null) {
                return true;
            }
            return expr.getOp().getProjection().getColumnRefMap().entrySet().stream()
                    .allMatch(e -> e.getKey().equals(e.getValue()));
        }
    }
}
