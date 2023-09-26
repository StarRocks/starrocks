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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrunePartitionShuffleColumnRule implements TreeRewriteRule {
    private static final Logger LOG = LogManager.getLogger(PrunePartitionShuffleColumnRule.class);

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        PruneShuffleColumnVisitor visitor = new PruneShuffleColumnVisitor(taskContext);
        visitor.rewrite(root);
        return root;
    }

    private static class PartitionColumns extends OptExpressionVisitor<OptExpression, Void> {
        private final SessionVariable sessionVariable;
        private final ColumnRefSet allPartitionRefs = new ColumnRefSet();

        public PartitionColumns(SessionVariable sessionVariable) {
            this.sessionVariable = sessionVariable;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, context);
            }

            Projection pj = optExpression.getOp().getProjection();
            if (pj == null) {
                return null;
            }
            pj.getColumnRefMap().forEach((k, v) -> {
                ColumnRefSet refs = v.getUsedColumns();
                if (refs.cardinality() == 1 && allPartitionRefs.containsAll(refs)) {
                    allPartitionRefs.union(k);
                }
            });

            return null;
        }

        @Override
        public OptExpression visitPhysicalScan(OptExpression optExpression, Void context) {
            PhysicalScanOperator scan = optExpression.getOp().cast();
            if (!StringUtils.isEmpty(sessionVariable.getShuffleOptimizationColumnHints())) {
                scan.getColRefToColumnMetaMap().entrySet().stream()
                        .filter(e -> sessionVariable.getShuffleOptimizationColumnHints()
                                .contains(e.getValue().getName())).map(Map.Entry::getKey).findFirst()
                        .ifPresent(allPartitionRefs::union);
            }
            Column column = findTablePartitionColumn(scan.getTable());
            scan.getColRefToColumnMetaMap().entrySet().stream().filter(e -> e.getValue().equals(column))
                    .map(Map.Entry::getKey).findFirst().ifPresent(allPartitionRefs::union);
            return null;
        }

        private Column findTablePartitionColumn(Table table) {
            // find partition column
            Column partitionColumn = null;
            if (table instanceof OlapTable) {
                PartitionInfo info = ((OlapTable) table).getPartitionInfo();
                if (info instanceof RangePartitionInfo) {
                    RangePartitionInfo ri = (RangePartitionInfo) info;
                    if (ri.getPartitionColumns().size() == 1) {
                        return ri.getPartitionColumns().get(0);
                    }
                }
            } else if (table.isHiveTable() || table.isIcebergTable() || table.isHudiTable()) {
                List<Column> partitionColumns = PartitionUtil.getPartitionColumns(table);
                if (partitionColumns.size() == 1) {
                    return partitionColumns.get(0);
                }
            }
            return partitionColumn;
        }

    }

    private static class PruneShuffleColumnVisitor extends OptExpressionVisitor<OptExpression, DistributionContext> {
        private final SessionVariable sessionVariable;
        private final ColumnRefFactory factory;
        private ColumnRefSet partitionRefs;

        public PruneShuffleColumnVisitor(TaskContext taskContext) {
            sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
            factory = taskContext.getOptimizerContext().getColumnRefFactory();
        }

        public void rewrite(OptExpression root) {
            PartitionColumns pc = new PartitionColumns(sessionVariable);
            root.getOp().accept(pc, root, null);
            partitionRefs = pc.allPartitionRefs;

            DistributionContext rootContext = new DistributionContext();
            root.getOp().accept(this, root, rootContext);
            prune(rootContext);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, DistributionContext context) {
            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), context);
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalCTEProduce(OptExpression optExpression, DistributionContext context) {
            DistributionContext childContext = new DistributionContext();
            visit(optExpression, childContext);
            prune(childContext);
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalDistribution(OptExpression optExpression, DistributionContext context) {
            context.addDistribution(optExpression);

            DistributionContext childContext = new DistributionContext();
            visit(optExpression, childContext);

            prune(childContext);
            return optExpression;
        }

        private void prune(DistributionContext childContext) {
            // only join can be return more distribution
            if (childContext.distributionList.size() < 2) {
                return;
            }

            Preconditions.checkState(childContext.distributionList.stream()
                    .allMatch(s -> s.getDistributionSpec().getType() == DistributionSpec.DistributionType.SHUFFLE));

            List<HashDistributionDesc> descs = childContext.distributionList.stream()
                    .map(s -> ((HashDistributionSpec) s.getDistributionSpec()).getHashDistributionDesc())
                    .collect(Collectors.toList());

            Preconditions.checkState(
                    descs.stream().mapToInt(d -> d.getDistributionCols().size()).distinct().count() == 1);

            // only use left child's partition column

            int index = 0;
            for (; index < descs.get(0).getDistributionCols().size(); index++) {
                if (!StringUtils.isEmpty(sessionVariable.getShuffleOptimizationColumnHints())) {
                    int cid = descs.get(0).getDistributionCols().get(index).getColId();
                    ColumnRefOperator ref = factory.getColumnRef(cid);
                    if (sessionVariable.getShuffleOptimizationColumnHints().contains(ref.getName())) {
                        LOG.debug("use partition column: " + ref.getName() + ", index: " + index);
                        break;
                    }
                    LOG.debug("can't partition column: " + ref.getName());
                }

                if (partitionRefs.contains(descs.get(0).getDistributionCols().get(index).getColId())) {
                    break;
                }
            }

            if (index < descs.get(0).getDistributionCols().size()) {
                for (HashDistributionDesc d : descs) {
                    d.setTablePartitionColumnIds(Lists.newArrayList(index));
                    // DistributionCol x = d.getDistributionCols().get(index);
                    // d.getDistributionCols().clear();
                    // d.getDistributionCols().add(x);
                }
            }
        }

        @Override
        public OptExpression visitPhysicalJoin(OptExpression optExpression, DistributionContext context) {
            DistributionContext lc = new DistributionContext();
            DistributionContext rc = new DistributionContext();

            optExpression.getInputs().get(0).getOp().accept(this, optExpression.getInputs().get(0), lc);
            optExpression.getInputs().get(1).getOp().accept(this, optExpression.getInputs().get(1), rc);

            if (lc.distributionList.isEmpty() || rc.distributionList.isEmpty()) {
                return optExpression;
            }

            if (lc.isShuffle() && rc.isBroadcast()) {
                context.add(lc);
            } else if (lc.isShuffle() && rc.isShuffle()) {
                context.add(lc);
                context.add(rc);
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalUnion(OptExpression optExpression, DistributionContext context) {
            visit(optExpression, new DistributionContext());
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalExcept(OptExpression optExpression, DistributionContext context) {
            visit(optExpression, new DistributionContext());
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalIntersect(OptExpression optExpression, DistributionContext context) {
            visit(optExpression, new DistributionContext());
            return optExpression;
        }
    }

    public static class DistributionContext {
        public final List<PhysicalDistributionOperator> distributionList = Lists.newArrayList();
        public final List<Statistics> statistics = Lists.newArrayList();

        public void addDistribution(OptExpression optExpression) {
            Preconditions.checkState(optExpression.getOp().getOpType() == OperatorType.PHYSICAL_DISTRIBUTION);
            this.distributionList.add((PhysicalDistributionOperator) optExpression.getOp());
            this.statistics.add(optExpression.getStatistics());
        }

        public void add(DistributionContext other) {
            this.distributionList.addAll(other.distributionList);
            this.statistics.addAll(other.statistics);
        }

        private boolean isShuffle() {
            if (distributionList.isEmpty()) {
                return false;
            }

            for (PhysicalDistributionOperator d : distributionList) {
                if (d.getDistributionSpec().getType() != DistributionSpec.DistributionType.SHUFFLE) {
                    return false;
                }

                HashDistributionDesc desc = ((HashDistributionSpec) d.getDistributionSpec()).getHashDistributionDesc();
                if (!desc.isShuffle() && !desc.isShuffleEnforce()) {
                    return false;
                }
            }
            return true;
        }

        private boolean isBroadcast() {
            if (distributionList.isEmpty()) {
                return false;
            }

            for (PhysicalDistributionOperator d : distributionList) {
                if (d.getDistributionSpec().getType() != DistributionSpec.DistributionType.BROADCAST) {
                    return false;
                }
            }
            return true;
        }
    }
}
