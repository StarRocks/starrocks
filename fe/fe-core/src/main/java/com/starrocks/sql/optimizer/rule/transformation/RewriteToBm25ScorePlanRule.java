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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Rewrites `SELECT score() ... WHERE col MATCH 'x' ORDER BY score() DESC LIMIT N`
 * on an OLAP table with a GIN/tantivy index into a scan that materializes the
 * BM25 score as a synthetic FLOAT output column (filled by the BE inverted-index
 * scored search). Mirrors {@link RewriteToVectorPlanRule}: the MATCH predicate is
 * KEPT (it both filters rows and produces the scores).
 */
public class RewriteToBm25ScorePlanRule extends TransformationRule {

    public RewriteToBm25ScorePlanRule() {
        super(RuleType.TF_BM25_SCORE_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();
        if (scanOp.getProjection() == null || scanOp.getBm25ScoreSlotId() >= 0) {
            return false;
        }
        if (topNOp.getLimit() <= 0 || topNOp.getOrderByElements().size() != 1) {
            return false;
        }
        return scanOp.getTable() instanceof OlapTable;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();
        OlapTable table = (OlapTable) scanOp.getTable();

        // 1. The ordering column must be a zero-arg score() call.
        Ordering ordering = topNOp.getOrderByElements().get(0);
        ColumnRefOperator outColRef = ordering.getColumnRef();
        ScalarOperator inOp = scanOp.getProjection().getColumnRefMap().get(outColRef);
        if (!(inOp instanceof CallOperator)) {
            return List.of();
        }
        CallOperator scoreCall = (CallOperator) inOp;
        if (!FunctionSet.MATCH_SCORE.equalsIgnoreCase(scoreCall.getFnName()) || !scoreCall.getChildren().isEmpty()) {
            return List.of();
        }

        // 2. The query must MATCH a column that carries a GIN index.
        ColumnRefOperator matchCol = findMatchColumn(scanOp.getPredicate());
        if (matchCol == null) {
            return List.of();
        }
        Column column = scanOp.getColRefToColumnMetaMap().get(matchCol);
        if (column == null) {
            return List.of();
        }
        Index ginIndex = table.getIndexes().stream()
                .filter(i -> i.getIndexType() == IndexDef.IndexType.GIN &&
                        i.getColumns().contains(column.getColumnId()))
                .findFirst().orElse(null);
        if (ginIndex == null) {
            return List.of();
        }

        // 3. Add a synthetic FLOAT score column + col ref (mirrors the vector path).
        String scoreColName = "__bm25_score_" + outColRef.getId();
        Column scoreColumn = new Column(scoreColName, Type.FLOAT);
        table.addColumn(scoreColumn);
        ColumnRefOperator scoreColRef = context.getColumnRefFactory().create(scoreColName, Type.FLOAT, false);

        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = new HashMap<>(scanOp.getColRefToColumnMetaMap());
        newColRefToColumnMetaMap.put(scoreColRef, scoreColumn);
        Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = new HashMap<>(scanOp.getColumnMetaToColRefMap());
        newColumnMetaToColRefMap.put(scoreColumn, scoreColRef);

        // 4. Replace the score() call in the projection with the score column ref.
        Map<ColumnRefOperator, ScalarOperator> newProjectMap = scanOp.getProjection().getColumnRefMap().entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey,
                        e -> rewriteScoreCall(e.getValue(), scoreCall, scoreColRef)));

        LogicalOlapScanOperator newScanOp = LogicalOlapScanOperator.builder()
                .withOperator(scanOp)
                .setProjection(new Projection(newProjectMap))
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setColumnMetaToColRefMap(newColumnMetaToColRefMap)
                .build();
        newScanOp.setBm25ScoreSlotId(scoreColRef.getId());
        // 5. Push the LIMIT into the scored GIN query so tantivy returns only the
        // top-k rows by score (WAND pruning), mirroring the vector ANN top-k path.
        // Only valid for ORDER BY score() DESC (TopDocs keeps highest scores); for
        // ASC, leave the limit at 0 so the BE scores every matched row.
        if (!ordering.isAscending()) {
            newScanOp.setBm25ScoreLimit(topNOp.getLimit() + Math.max(0L, topNOp.getOffset()));
        }

        return List.of(OptExpression.create(topNOp, OptExpression.create(newScanOp)));
    }

    private ColumnRefOperator findMatchColumn(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof MatchExprOperator) {
            ScalarOperator col = predicate.getChild(0);
            return col instanceof ColumnRefOperator ? (ColumnRefOperator) col : null;
        }
        for (ScalarOperator child : predicate.getChildren()) {
            ColumnRefOperator c = findMatchColumn(child);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    private ScalarOperator rewriteScoreCall(ScalarOperator op, CallOperator scoreCall, ColumnRefOperator scoreColRef) {
        if (op.equals(scoreCall)) {
            return scoreColRef;
        }
        for (int i = 0; i < op.getChildren().size(); i++) {
            op.setChild(i, rewriteScoreCall(op.getChild(i), scoreCall, scoreColRef));
        }
        return op;
    }
}
