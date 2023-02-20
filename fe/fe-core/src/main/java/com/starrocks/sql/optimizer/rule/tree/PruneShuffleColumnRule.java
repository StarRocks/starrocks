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
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.stream.Collectors;

/*
 * Prune join shuffle columns, only support shuffle join
 * e.g.
 *  select * from t1 join[shuffle] t2 on t1.v1 = t2.v1 and t1.v2 = t2.v2
 * before: join required shuffle by (v1, v2)
 * after: join required shuffle by (v1) or (v2)
 *
 * e.g.
 *  select * from t1 join[shuffle] t2 on t1.v1 = t2.v1 and t1.v2 = t2.v2
 *                   join[shuffle] t3 on t1.v1 = t3.v1 and t1.v3 = t3.v3
 * before: t1 x t2 join required shuffle by (v1, v2)
 *         t1 x t3 join required shuffle by (v1, v3)
 * after:  t1 x t2 join required shuffle by (v1) or (v2)
 *         t1 x t3 join required shuffle by (v1) or (v3)
 *
 * e.g.
 *  select * from t1 join[shuffle] t2 on t1.v1 = t2.v1 and t1.v2 = t2.v2
 *                   join[bucket_shuffle] t3 on t1.v1 = t3.v1 and t1.v2 = t3.v2
 * before: t1 x t2 x t3 join required shuffle by (v1, v2)
 * after:  t1 x t2 x t3 join required shuffle by (v1) or (v2)
 */
public class PruneShuffleColumnRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        PruneShuffleColumnVisitor visitor = new PruneShuffleColumnVisitor(taskContext);
        visitor.rewrite(root);
        return root;
    }

    private static class PruneShuffleColumnVisitor extends OptExpressionVisitor<OptExpression, DistributionContext> {
        private final ColumnRefFactory factory;
        private final SessionVariable sessionVariable;

        public PruneShuffleColumnVisitor(TaskContext taskContext) {
            this.factory = taskContext.getOptimizerContext().getColumnRefFactory();
            this.sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
        }

        public void rewrite(OptExpression root) {
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
                    .map(s -> ((HashDistributionSpec) s.getDistributionSpec()).getHashDistributionDesc()).collect(
                            Collectors.toList());

            Preconditions.checkState(descs.stream().mapToInt(d -> d.getColumns().size()).distinct().count() == 1);

            if (descs.stream().mapToInt(d -> d.getColumns().size()).min().orElse(0) < 2) {
                return;
            }

            // choose high cardinality column
            int columnSize = descs.get(0).getColumns().size();
            int maxColumnIndex = -1;
            double maxRatio = -1;

            for (int i = 0; i < columnSize; i++) {
                for (int j = 0; j < descs.size(); j++) {
                    ColumnRefOperator ref = factory.getColumnRef(descs.get(j).getColumns().get(i));
                    ColumnStatistic cs = childContext.statistics.get(j).getColumnStatistic(ref);

                    if (cs.isUnknown()) {
                        continue;
                    }

                    double ratio =
                            cs.getDistinctValuesCount() / childContext.statistics.get(j).getOutputRowCount();
                    if ((cs.getDistinctValuesCount() <
                            StatisticsEstimateCoefficient.DEFAULT_PRUNE_SHUFFLE_COLUMN_ROWS_LIMIT) ||
                            (ratio < sessionVariable.getCboPruneShuffleColumnRate())) {
                        continue;
                    }

                    if (ratio > maxRatio) {
                        maxRatio = ratio;
                        maxColumnIndex = i;
                    }
                }
            }

            if (maxColumnIndex > -1) {
                for (HashDistributionDesc d : descs) {
                    int x = d.getColumns().get(maxColumnIndex);
                    d.getColumns().clear();
                    d.getColumns().add(x);
                }
            }
        }

        @Override
        public OptExpression visitPhysicalNestLoopJoin(OptExpression optExpression, DistributionContext context) {
            // NestLoopJoin right table does not support shuffle
            return optExpression;
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
