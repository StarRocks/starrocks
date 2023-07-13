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

package com.starrocks.sql.optimizer.rule.transformation.pruner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.UnionFind;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// Prune extra tables in primary key table's update statement.
// Users often select a portion of primary key table, then use left join to change the columns, finally
// write back the modified data into primary key table for partial update. Primary key tables' update
// operation must be designated which rows should be updated by specifying its certain primary keys, so
// so, an extra inner join which joining primary key table itself is added to the plan. this extra inner
// join can be pruned. for an example:
//```SQL
// with cte0 as (
// select t0.pk, t1.col
// from t0 left join t1 on t0.fk = t0.pk
// )
// update t0 set col = cte0.col from cte0 where cte0.pk = t0.pk
//```
// The SQL above is equivalent to
//```SQL
// insert into t0 select t0.pk, tmp.col
// from t0 inner join
//  (select t0.pk, t1.col from t0 left join t1 on t0.fk = t1.pk) tmp on t0.pk = t1.pk
//```
// Extra inner join t0 can be pruned, so
//```SQL
// insert into t0 select t0.pk, t1.col from t0 left join t1 on t0.fk = t1.pk
//```
public class PrimaryKeyUpdateTableRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        long updateTableId = taskContext.getOptimizerContext().getUpdateTableId();
        if (updateTableId < 0) {
            return root;
        }
        Collector collector = new Collector(updateTableId);
        collector.collect(root);
        if (!collector.markPrunedTables(root)) {
            return root;
        }
        CPJoinGardener.pruneFrontier(
                taskContext.getOptimizerContext().getColumnRefFactory(),
                root.inputAt(0),
                root,
                0,
                collector.getPrunedTables(),
                collector.getColRefRemapping(),
                collector.getColRefRemapping().values().stream().map(v -> (ColumnRefOperator) v)
                        .collect(Collectors.toSet())
        );
        return root;
    }

    private static class Collector extends OptExpressionVisitor<Boolean, Void> {
        private final long updateTableId;
        private final Map<OptExpression, Integer> scanToOrdinals = Maps.newHashMap();
        private final List<OptExpression> ordinalToScan = Lists.newArrayList();
        private final Map<OptExpression, RoaringBitmap> optToCPScanNodes = Maps.newHashMap();
        private final UnionFind<ColumnRefOperator> colRefEquivClasses = new UnionFind<>();

        private final Set<OptExpression> prunedTables = Sets.newHashSet();
        private final Map<ColumnRefOperator, ScalarOperator> colRefRemapping = Maps.newHashMap();

        private Collector(long updateTableId) {
            this.updateTableId = updateTableId;
        }

        public Set<OptExpression> getPrunedTables() {
            return prunedTables;
        }

        public Map<ColumnRefOperator, ScalarOperator> getColRefRemapping() {
            return colRefRemapping;
        }

        public Optional<CPBiRel> getSameTablesJoinOnPK(OptExpression lhs, OptExpression rhs) {
            List<CPBiRel> cpBiRels = CPBiRel.extractCPBiRels(lhs, rhs, true);
            return cpBiRels.stream().filter(cpBiRel -> !cpBiRel.isFromForeignKey()).findFirst();
        }

        private Boolean collectInnerJoin(OptExpression optExpression, Void context) {
            OptExpression lhsChild = optExpression.inputAt(0);
            OptExpression rhsChild = optExpression.inputAt(1);
            RoaringBitmap lhsCPBits = optToCPScanNodes.getOrDefault(lhsChild, EMPTY_CP_SCAN_NODES);
            RoaringBitmap rhsCPBits = optToCPScanNodes.getOrDefault(rhsChild, EMPTY_CP_SCAN_NODES);

            Optional<OptExpression> lhsCPScan = Utils.getIntStream(lhsCPBits).map(ordinalToScan::get).findFirst();
            Optional<OptExpression> rhsCPScan = Utils.getIntStream(rhsCPBits).map(ordinalToScan::get).findFirst();

            boolean match = lhsCPScan.map(lhs -> rhsCPScan.map(rhs -> getSameTablesJoinOnPK(lhs, rhs)
                    .map(biRel -> CPJoinGardener.eqColRefPairsMatch(colRefEquivClasses,
                            Utils.getJoinEqualColRefPairs(optExpression),
                            biRel.getPairs())).orElse(false)).orElse(false)).orElse(false);
            if (match) {
                optToCPScanNodes.put(optExpression, RoaringBitmap.or(lhsCPBits, rhsCPBits));
                return true;
            } else {
                return propagateBottomUp(optExpression, 0, context) ||
                        propagateBottomUp(optExpression, 1, context);
            }
        }

        @Override
        public Boolean visitLogicalJoin(OptExpression optExpression, Void context) {
            LogicalJoinOperator joinOp = optExpression.getOp().cast();
            JoinOperator joinType = joinOp.getJoinType();
            if (!joinType.isInnerJoin() && !joinType.isLeftOuterJoin() && !joinType.isRightOuterJoin()) {
                return visit(optExpression, context);
            }
            if (joinOp.getPredicate() != null) {
                return visit(optExpression, context);
            }

            if (joinType.isInnerJoin()) {
                return collectInnerJoin(optExpression, context);
            } else if (joinType.isLeftOuterJoin()) {
                return propagateBottomUp(optExpression, 0, context);
            } else if (joinType.isRightJoin()) {
                return propagateBottomUp(optExpression, 1, context);
            }
            return visit(optExpression, context);
        }

        @Override
        public Boolean visit(OptExpression optExpression, Void context) {
            return false;
        }

        @Override
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOp = optExpression.getOp().cast();
            if (!(scanOp.getTable() instanceof OlapTable)) {
                return visit(optExpression, context);
            }
            OlapTable table = ((OlapTable) scanOp.getTable());
            if (table.getKeysType() == KeysType.PRIMARY_KEYS && table.getId() == updateTableId) {
                int ordinal = scanToOrdinals.getOrDefault(optExpression, -1);
                Preconditions.checkArgument(ordinal >= 0);
                optToCPScanNodes.put(optExpression, RoaringBitmap.bitmapOf(ordinal));
                return true;
            }
            return visit(optExpression, context);
        }

        private static final RoaringBitmap EMPTY_CP_SCAN_NODES = RoaringBitmap.bitmapOf();

        private Boolean propagateBottomUp(OptExpression optExpression, int fromChildIdx, Void context) {
            OptExpression child = optExpression.inputAt(fromChildIdx);
            RoaringBitmap cpNodes = optToCPScanNodes.getOrDefault(child, EMPTY_CP_SCAN_NODES);
            if (!cpNodes.isEmpty()) {
                optToCPScanNodes.put(optExpression, cpNodes);
                return true;
            }
            return visit(optExpression, context);
        }

        @Override
        public Boolean visitLogicalCTEConsume(OptExpression optExpression, Void context) {
            return propagateBottomUp(optExpression, 0, context);
        }

        @Override
        public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
            return propagateBottomUp(optExpression, 0, context);
        }

        @Override
        public Boolean visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
            return propagateBottomUp(optExpression, 1, context);
        }

        private boolean collect(OptExpression root) {
            // Prefer to prune the Scan operator that sits higher in the plan.
            // please to see CPGardener::process
            for (OptExpression input : root.getInputs()) {
                if (input.getOp() instanceof LogicalScanOperator) {
                    int ordinal = scanToOrdinals.size();
                    scanToOrdinals.put(input, ordinal);
                    ordinalToScan.add(input);
                }
            }
            for (OptExpression input : root.getInputs()) {
                collect(input);
            }
            return root.getOp().accept(this, root, null);
        }

        private boolean markPrunedTables(OptExpression root) {
            RoaringBitmap cpBits = optToCPScanNodes.getOrDefault(root.inputAt(0), EMPTY_CP_SCAN_NODES);
            List<OptExpression> cpNodes = Utils.getIntStream(cpBits).map(i -> Pair.create(ordinalToScan.get(i), i))
                    .sorted(Pair.comparingBySecond()).map(p -> p.first).collect(Collectors.toList());

            if (cpNodes.size() < 2) {
                return false;
            }

            int lastIdx = cpNodes.size() - 1;
            OptExpression retainedScan = cpNodes.get(lastIdx);
            LogicalOlapScanOperator retainedScanOp = retainedScan.getOp().cast();
            List<OptExpression> prunedScans = cpNodes.subList(0, lastIdx);
            this.prunedTables.addAll(prunedScans);
            for (OptExpression node : prunedScans) {
                LogicalOlapScanOperator scanOp = node.getOp().cast();
                colRefRemapping.putAll(Utils.makeEqColumRefMapFromSameTables(scanOp, retainedScanOp));
            }
            return true;
        }
    }
}
