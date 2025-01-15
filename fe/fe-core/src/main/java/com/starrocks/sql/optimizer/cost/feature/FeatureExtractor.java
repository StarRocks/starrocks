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

package com.starrocks.sql.optimizer.cost.feature;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.system.BackendResourceStat;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extract features from physical plan
 */
public class FeatureExtractor {

    public static PlanFeatures extractFeatures(OptExpression plan) {
        Extractor extractor = new Extractor();
        OperatorFeatures root = plan.getOp().accept(extractor, plan, null);
        PlanFeatures planFeatures = new PlanFeatures();

        // tables
        List<OperatorFeatures.ScanOperatorFeatures> scanNodes = Lists.newArrayList();
        root.collect(OperatorFeatures.ScanOperatorFeatures.class, scanNodes);
        if (CollectionUtils.isNotEmpty(scanNodes)) {
            Set<Table> tables = scanNodes.stream()
                    .map(OperatorFeatures.ScanOperatorFeatures::getTable)
                    .collect(Collectors.toSet());
            planFeatures.addTableFeatures(tables);
        }

        // operator features
        var sumVector = PlanFeatures.aggregate(root);
        planFeatures.addOperatorFeatures(sumVector);

        // environment
        planFeatures.setAvgCpuCoreOfBe(BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        planFeatures.setNumBeNodes(BackendResourceStat.getInstance().getNumBes());
        planFeatures.setMemCapacityOfBE(BackendResourceStat.getInstance().getAvgMemLimitBytes());

        // variables
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            planFeatures.setDop(ctx.getSessionVariable().getPipelineDop());
        }

        return planFeatures;
    }

    static class Extractor extends OptExpressionVisitor<OperatorFeatures, Void> {

        @Override
        public OperatorFeatures visit(OptExpression optExpression, Void context) {
            CostEstimate cost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));
            Statistics stats = optExpression.getStatistics();
            OperatorFeatures node = createOperatorFeatures(optExpression, cost, stats);

            visitChildren(optExpression, context, node);
            return node;
        }

        private void visitChildren(OptExpression optExpression, Void context, OperatorFeatures node) {
            for (var child : optExpression.getInputs()) {
                Operator operator = child.getOp();
                OperatorFeatures childNode = operator.accept(this, child, null);
                node.addChild(childNode);
            }
        }

        private OperatorFeatures createOperatorFeatures(OptExpression optExpression, CostEstimate cost,
                                                        Statistics stats) {
            OperatorType opType = optExpression.getOp().getOpType();
            if (opType.isPhysicalScan()) {
                return new OperatorFeatures.ScanOperatorFeatures(optExpression, cost, stats);
            }
            switch (opType) {
                case PHYSICAL_HASH_JOIN:
                    return new OperatorFeatures.JoinOperatorFeatures(optExpression, cost, stats);
                case PHYSICAL_HASH_AGG:
                    return new OperatorFeatures.AggOperatorFeatures(optExpression, cost, stats);
                case PHYSICAL_DISTRIBUTION:
                    return new OperatorFeatures.ExchangeOperatorFeatures(optExpression, cost, stats);
                default:
                    return new OperatorFeatures(optExpression, cost, stats);
            }
        }
    }
}
