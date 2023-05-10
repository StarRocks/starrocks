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

package com.starrocks.sql.optimizer.validate;

import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class CTEUniqueChecker implements PlanValidator.Checker {

    private static final String PREFIX = "CTE unique check failed.";

    private static final CTEUniqueChecker INSTANCE = new CTEUniqueChecker();

    private CTEUniqueChecker() {}

    public static CTEUniqueChecker getInstance() {
        return INSTANCE;
    }


    @Override
    public void validate(OptExpression physicalPlan, TaskContext taskContext) {
        CTEUniqueChecker.Visitor visitor = new CTEUniqueChecker.Visitor();
        physicalPlan.getOp().accept(visitor, physicalPlan, null);
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {

        private Set<Integer> cteIds = Sets.newHashSet();

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                Operator operator = input.getOp();
                operator.accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
            LogicalCTEAnchorOperator anchorOperator = (LogicalCTEAnchorOperator) optExpression.getOp();
            checkArgument(cteIds.add(anchorOperator.getCteId()), "%s CTE id %s has been defined.",
                    PREFIX, anchorOperator.getCteId());
            visit(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalCTEProduce(OptExpression optExpression, Void context) {
            LogicalCTEProduceOperator produceOperator = (LogicalCTEProduceOperator) optExpression.getOp();
            checkArgument(cteIds.contains(produceOperator.getCteId()), "%s CTE id %s has not been defined.",
                    PREFIX, produceOperator.getCteId());
            visit(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalCTEConsume(OptExpression optExpression, Void context) {
            LogicalCTEConsumeOperator consumeOperator = (LogicalCTEConsumeOperator) optExpression.getOp();
            checkArgument(cteIds.contains(consumeOperator.getCteId()), "%s CTE id %s has not been defined.",
                    PREFIX, consumeOperator.getCteId());
            visit(optExpression, context);
            return null;
        }


        @Override
        public Void visitPhysicalCTEAnchor(OptExpression optExpression, Void context) {
            PhysicalCTEAnchorOperator anchorOperator = (PhysicalCTEAnchorOperator) optExpression.getOp();
            checkArgument(cteIds.add(anchorOperator.getCteId()), "%s CTE id %s has been defined.",
                    PREFIX, anchorOperator.getCteId());
            visit(optExpression, context);
            return null;
        }

        @Override
        public Void visitPhysicalCTEProduce(OptExpression optExpression, Void context) {
            PhysicalCTEProduceOperator produceOperator = (PhysicalCTEProduceOperator) optExpression.getOp();
            checkArgument(cteIds.contains(produceOperator.getCteId()), "%s CTE id %s has not been defined.",
                    PREFIX, produceOperator.getCteId());
            visit(optExpression, context);
            return null;
        }

        @Override
        public Void visitPhysicalCTEConsume(OptExpression optExpression, Void context) {
            PhysicalCTEConsumeOperator consumeOperator = (PhysicalCTEConsumeOperator) optExpression.getOp();
            checkArgument(cteIds.contains(consumeOperator.getCteId()), "%s CTE id %s has not been defined.",
                    PREFIX, consumeOperator.getCteId());
            visit(optExpression, context);
            return null;
        }

        @Override
        public Void visitPhysicalNoCTE(OptExpression optExpression, Void context) {
            PhysicalNoCTEOperator noCTEOperator = (PhysicalNoCTEOperator) optExpression.getOp();
            checkArgument(!cteIds.contains(noCTEOperator.getCteId()), "%s CTE id %s defined but not use it.",
                    PREFIX, noCTEOperator.getCteId());
            visit(optExpression, context);
            return null;
        }
    }
}
