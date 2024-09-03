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
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.feedback.OperatorTuningGuides;
import com.starrocks.qe.feedback.PlanTuningAdvisor;
import com.starrocks.qe.feedback.guide.TuningGuide;
import com.starrocks.qe.feedback.skeleton.SkeletonBuilder;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ApplyTuningGuideRule implements TreeRewriteRule {

    private final String sql;

    public ApplyTuningGuideRule(ConnectContext connectContext) {
        StatementBase stmt = connectContext.getExecutor() == null ?
                null : connectContext.getExecutor().getParsedStmt();
        sql = extractQuerySql(stmt);
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable session = taskContext.getOptimizerContext().getSessionVariable();
        if (sql.isEmpty() || !session.isEnablePlanAdvisor()) {
            return root;
        }
        SkeletonBuilder builder = new SkeletonBuilder();
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = builder.buildSkeleton(root);
        OperatorTuningGuides tuningGuides = PlanTuningAdvisor.getInstance().getTuningGuides(sql, pair.first);
        if (tuningGuides == null) {
            return root;
        }

        // delete the useless tuning guides
        if (!tuningGuides.isUseful()) {
            PlanTuningAdvisor.getInstance().deleteTuningGuides(tuningGuides.getOriginalQueryId());
            return root;
        }

        Visitor visitor = new Visitor(tuningGuides);
        OptExpression res = root.getOp().accept(visitor, root, pair.first);
        if (!visitor.getUsedGuides().isEmpty()) {
            PlanTuningAdvisor.getInstance().addOptimizedQueryRecord(taskContext.getOptimizerContext().getQueryId(),
                    new OperatorTuningGuides.OptimizedRecord(tuningGuides, visitor.getUsedGuides()));
        }
        return res;
    }

    private String extractQuerySql(StatementBase stmt) {
        if (stmt == null || stmt.getOrigStmt() == null || !(stmt instanceof QueryStatement)) {
            return "";
        }
        QueryStatement queryStmt = (QueryStatement) stmt;

        String sql = stmt.getOrigStmt().getOrigStmt();
        if (queryStmt.getQueryStartIndex() != -1 && queryStmt.getQueryStartIndex() < sql.length()) {
            sql = sql.substring(queryStmt.getQueryStartIndex());
        }
        return sql;
    }

    private static class Visitor extends OptExpressionVisitor<OptExpression, SkeletonNode> {

        private final OperatorTuningGuides tuningGuides;

        private final List<TuningGuide> usedGuides;

        public Visitor(OperatorTuningGuides tuningGuides) {
            this.tuningGuides = tuningGuides;
            this.usedGuides = Lists.newArrayList();
        }

        public List<TuningGuide> getUsedGuides() {
            return usedGuides;
        }

        @Override
        public OptExpression visit(OptExpression opt, SkeletonNode context) {
            Preconditions.checkState(opt.getInputs().size() == context.getChildren().size());
            for (int i = 0; i < opt.getInputs().size(); i++) {
                OptExpression child = opt.inputAt(i);
                OptExpression newChild = child.getOp().accept(this, child, context.getChild(i));
                opt.setChild(i, newChild);
            }
            List<TuningGuide> guideList = tuningGuides.getTuningGuides(context.getNodeId());
            OptExpression res = opt;
            if (guideList != null) {
                for (TuningGuide guide : guideList) {
                    Optional<OptExpression> newOpt = guide.apply(res);
                    if (newOpt.isPresent()) {
                        res = newOpt.get();
                        usedGuides.add(guide);
                    }
                }
            }
            return res;
        }
    }
}
