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

package com.starrocks.sql.optimizer.rule.tree.pieces;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class PiecesPlanTransformer {
    private final ColumnRefFactory factory;
    private final List<QueryPiecesPlan> planPieces = Lists.newArrayList();
    private int planId = 0;

    public PiecesPlanTransformer(ColumnRefFactory factory) {
        this.factory = factory;
    }

    public List<QueryPiecesPlan> getPlanPieces() {
        return planPieces;
    }

    private static boolean checkTrees(OptExpression root, Predicate<Operator> lambda) {
        if (!lambda.test(root.getOp())) {
            return false;
        }
        List<OptExpression> inputs = root.getInputs();
        for (OptExpression input : inputs) {
            if (!checkTrees(input, lambda)) {
                return false;
            }
        }
        return true;
    }

    public boolean isSPJGPieces(OptExpression tree) {
        if (!tree.getOp().getOpType().equals(OperatorType.LOGICAL_AGGR)) {
            return false;
        }
        return checkTrees(tree.inputAt(0), op -> op.getOpType().equals(OperatorType.LOGICAL_PROJECT)
                || op.getOpType().equals(OperatorType.LOGICAL_JOIN)
                || op instanceof LogicalScanOperator);
    }

    // trans to special pieces plan
    public OptExpression transformSPJGPieces(OptExpression root) {
        for (int i = 0; i < root.arity(); i++) {
            if (root.inputAt(i).getOp().getOpType() == OperatorType.LOGICAL_CTE_CONSUME) {
                // don't check CTE consume plan, it's duplicate work
                break;
            }
            if (isSPJGPieces(root.inputAt(i))) {
                QueryPiecesPlan piece = planToPiece(root.inputAt(i), planId++);
                LogicalPiecesOperator op =
                        new LogicalPiecesOperator(OperatorType.LOGICAL_SPJG_PIECES, root.inputAt(i), piece);

                root.getInputs().set(i, OptExpression.create(op));
                planPieces.add(piece);
                continue;
            }
            transformSPJGPieces(root.inputAt(i));
        }
        return root;
    }

    private QueryPiecesPlan planToPiece(OptExpression plan, int id) {
        QueryPiecesPlan piecesPlan = new QueryPiecesPlan(id, new ScalarOperatorConverter());
        piecesPlan.planId = id;
        piecesPlan.root = new Transformer().visit(plan, piecesPlan);
        return piecesPlan;
    }

    public OptExpression transformPlan(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_SPJG_PIECES) {
            LogicalPiecesOperator op = root.getOp().cast();
            return op.getPlan();
        }
        for (int i = 0; i < root.arity(); i++) {
            root.setChild(i, transformPlan(root.inputAt(i)));
        }
        return root;
    }

    // replace columnRef with newId
    private class Transformer extends OptExpressionVisitor<QueryPieces, QueryPiecesPlan> {
        @Override
        public QueryPieces visit(OptExpression optExpression, QueryPiecesPlan context) {

            if (optExpression.getOp().getOpType() == OperatorType.LOGICAL_PROJECT) {
                OptExpression child = optExpression.inputAt(0);
                QueryPieces childPieces = child.getOp().accept(this, child, context);
                childPieces.op = child.getOp();

                QueryPieces pieces = visitProjection(optExpression.getOp().cast(), context);
                pieces.inputs.add(childPieces);
                pieces.algebra = childPieces.algebra;
                return pieces;
            } else {
                QueryPieces childPieces = optExpression.getOp().accept(this, optExpression, context);
                childPieces.op = optExpression.getOp();

                ColumnRefSet refs = optExpression.getOutputColumns();
                Map<ColumnRefOperator, ScalarOperator> project = Maps.newHashMap();
                refs.getColumnRefOperators(factory).forEach(ref -> project.put(ref, ref));

                LogicalProjectOperator temp = new LogicalProjectOperator(project);
                QueryPieces pieces = visitProjection(temp, context);
                pieces.inputs.add(childPieces);
                pieces.algebra = childPieces.algebra;
                return pieces;
            }
        }

        private QueryPieces visitProjection(LogicalProjectOperator project, QueryPiecesPlan context) {
            QueryPieces pieces = new QueryPieces();

            project.getColumnRefMap().entrySet().stream()
                    .map(e -> Pair.create(e.getKey(), context.columnRefConverter.convert(e.getValue())))
                    .sorted(Comparator.comparing(p -> p.second.toString()))
                    .forEach(p -> {
                        ColumnRefOperator newRef = context.columnRefConverter.convertRef(p.first);
                        context.columnRefConverter.addExpr(newRef, p.second);
                    });

            pieces.op = project;
            return pieces;
        }

        @Override
        public QueryPieces visitLogicalAggregate(OptExpression optExpression, QueryPiecesPlan context) {
            QueryPieces pieces = new QueryPieces();
            pieces.inputs.add(visit(optExpression.inputAt(0), context));

            LogicalAggregationOperator aggregate = optExpression.getOp().cast();
            List<ScalarOperator> groupBy = aggregate.getGroupingKeys()
                    .stream().map(g -> context.columnRefConverter.convertExpr(g))
                    .sorted(Comparator.comparing(ScalarOperator::toString))
                    .collect(Collectors.toList());

            pieces.algebra = "G(" + groupBy + " => " + pieces.inputs.get(0).algebra + ")";
            return pieces;
        }

        @Override
        public QueryPieces visitLogicalJoin(OptExpression optExpression, QueryPiecesPlan context) {
            LogicalJoinOperator join = optExpression.getOp().cast();

            QueryPieces pieces = new QueryPieces();
            pieces.inputs.add(visit(optExpression.inputAt(0), context));
            pieces.inputs.add(visit(optExpression.inputAt(1), context));

            pieces.algebra = pieces.inputs.stream().map(p -> p.algebra)
                    .collect(Collectors.joining(" " + join.getJoinType().toAlgebra() + " "));
            pieces.algebra = "(" + pieces.algebra + " on "
                    + context.columnRefConverter.convertExpr(join.getPredicate()) + " & "
                    + context.columnRefConverter.convertExpr(join.getOnPredicate()) + ")";
            return pieces;
        }

        @Override
        public QueryPieces visitLogicalTableScan(OptExpression optExpression, QueryPiecesPlan context) {
            QueryPieces pieces = new QueryPieces();
            LogicalScanOperator scan = optExpression.getOp().cast();

            Map<Column, Integer> columnMetaToIdMap = Maps.newHashMap();
            scan.getTable().getColumns().stream().sorted(Comparator.comparing(Column::getName)).forEach(c ->
                    columnMetaToIdMap.put(c, context.columnRefConverter.getNextID())
            );

            scan.getColumnMetaToColRefMap().forEach((c, ref) -> {
                context.columnRefConverter.convertRef(ref, columnMetaToIdMap.get(c));
            });

            pieces.algebra = scan.getTable().getUUID() + ":" + scan.getTable().getName();
            return pieces;
        }
    }

}
