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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.List;

public class MarkParentRequiredDistributionRule implements TreeRewriteRule {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(VISITOR, root, false);
        return root;
    }

    // If any OptExpression's child has global dict, we cannot change its distribution even if
    // its parent didn't require anything. If we want to change its distribution, we also need to
    // ensure the global dict should be reset correctly in the new distribution which is a tedious work.
    // If no OptExpression's child has global dict, we just need consider the requirement from its parent.
    private static class Visitor extends OptExpressionVisitor<Boolean, Boolean> {
        private boolean visitChildren(List<OptExpression> children, Boolean existRequiredDistribution) {
            boolean existGlobalDict = false;
            for (OptExpression child : children) {
                existGlobalDict |= child.getOp().accept(this, child, existRequiredDistribution);
            }
            return existGlobalDict;
        }

        @Override
        public Boolean visit(OptExpression optExpression, Boolean existRequiredDistribution) {
            boolean existGlobalDict = visitChildren(optExpression.getInputs(), existRequiredDistribution);
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalHashAggregate(OptExpression optExpression, Boolean existRequiredDistribution) {
            PhysicalHashAggregateOperator aggregateOperator = (PhysicalHashAggregateOperator) optExpression.getOp();
            boolean existGlobalDict;
            if (aggregateOperator.getType().isAnyGlobal()) {
                existGlobalDict = visitChildren(optExpression.getInputs(), true);
            } else {
                existGlobalDict = visitChildren(optExpression.getInputs(), existRequiredDistribution);
            }
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalHashJoin(OptExpression optExpression, Boolean existRequiredDistribution) {
            boolean existGlobalDict = visitChildren(optExpression.getInputs(), true);
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalNestLoopJoin(OptExpression optExpression, Boolean existRequiredDistribution) {
            boolean existGlobalDict = visitChildren(optExpression.getInputs(), true);
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }


        @Override
        public Boolean visitPhysicalDistribution(OptExpression optExpression, Boolean existRequiredDistribution) {
            boolean existGlobalDict =  visitChildren(optExpression.getInputs(), false);
            optExpression.setExistRequiredDistribution(true);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalAnalytic(OptExpression optExpression, Boolean existRequiredDistribution) {
            boolean existGlobalDict = visitChildren(optExpression.getInputs(), true);
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalTopN(OptExpression optExpression, Boolean existRequiredDistribution) {
            boolean existGlobalDict;
            PhysicalTopNOperator operator = (PhysicalTopNOperator) optExpression.getOp();
            if (operator.getSortPhase().isFinal()) {
                existGlobalDict = visitChildren(optExpression.getInputs(), true);
            } else {
                existGlobalDict = visitChildren(optExpression.getInputs(), existRequiredDistribution);
            }
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalLimit(OptExpression optExpression, Boolean existRequiredDistribution) {
            boolean existGlobalDict = visitChildren(optExpression.getInputs(), true);
            optExpression.setExistRequiredDistribution(existRequiredDistribution);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalOlapScan(OptExpression optExpression, Boolean existRequiredDistribution) {
            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            boolean existGlobalDict = CollectionUtils.isNotEmpty(scanOperator.getGlobalDicts()) ||
                    MapUtils.isNotEmpty(scanOperator.getGlobalDictsExpr());
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalHiveScan(OptExpression optExpression, Boolean existRequiredDistribution) {
            PhysicalHiveScanOperator scanOperator = (PhysicalHiveScanOperator) optExpression.getOp();
            boolean existGlobalDict = CollectionUtils.isNotEmpty(scanOperator.getGlobalDicts()) ||
                    MapUtils.isNotEmpty(scanOperator.getGlobalDictsExpr());
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }

        @Override
        public Boolean visitPhysicalIcebergScan(OptExpression optExpression, Boolean existRequiredDistribution) {
            PhysicalIcebergScanOperator scanOperator = (PhysicalIcebergScanOperator) optExpression.getOp();
            boolean existGlobalDict = CollectionUtils.isNotEmpty(scanOperator.getGlobalDicts()) ||
                    MapUtils.isNotEmpty(scanOperator.getGlobalDictsExpr());
            optExpression.setExistRequiredDistribution(existRequiredDistribution || existGlobalDict);
            return existGlobalDict;
        }
    }
}
