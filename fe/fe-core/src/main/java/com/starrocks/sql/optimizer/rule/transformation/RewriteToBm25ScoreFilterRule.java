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

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Makes `score()` usable as a predicate: rewrites a scan predicate that contains
 * top-level `score() >/>=/</<=/= c` conjuncts (alongside a GIN/tantivy MATCH on the
 * same table) into a BM25 score gate pushed into the scored GIN query. The score()
 * conjuncts are removed from the predicate and replaced by an inclusive [min, max]
 * range threaded to {@code TOlapScanNode.bm25_score_min/max}; the BE's scored seek
 * narrows the row bitmap to in-range rows inside tantivy, so the filter applies even
 * when score() is not in the SELECT list. Complements {@link RewriteToBm25ScorePlanRule}
 * (the ORDER BY score() top-k path): when both apply this rule reuses that rule's
 * synthetic score slot.
 */
public class RewriteToBm25ScoreFilterRule extends TransformationRule {

    public RewriteToBm25ScoreFilterRule() {
        super(RuleType.TF_BM25_SCORE_FILTER_REWRITE_RULE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getOp();
        if (!(scanOp.getTable() instanceof OlapTable) || scanOp.getPredicate() == null) {
            return false;
        }
        // Fire only when a top-level score() comparison is present (and re-fire guard:
        // once removed there is no such conjunct, so the rule does not loop).
        return Utils.extractConjuncts(scanOp.getPredicate()).stream().anyMatch(this::isScoreComparison);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getOp();
        OlapTable table = (OlapTable) scanOp.getTable();

        // The query must MATCH a column that carries a GIN index (mirrors the ORDER BY rule).
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

        // Split the predicate: score() comparisons feed the [min, max] gate, the rest
        // (the MATCH and any other filters) is kept and re-applied.
        double min = Double.NEGATIVE_INFINITY;
        double max = Double.POSITIVE_INFINITY;
        List<ScalarOperator> kept = new ArrayList<>();
        boolean rewroteAny = false;
        for (ScalarOperator conj : Utils.extractConjuncts(scanOp.getPredicate())) {
            double[] bound = scoreBound(conj);
            if (bound == null) {
                kept.add(conj);
                continue;
            }
            min = Math.max(min, bound[0]);
            max = Math.min(max, bound[1]);
            rewroteAny = true;
        }
        if (!rewroteAny) {
            return List.of();
        }

        LogicalOlapScanOperator.Builder builder = LogicalOlapScanOperator.builder().withOperator(scanOp);
        builder.setPredicate(Utils.compoundAnd(kept));

        // Decouple the score GATE from score MATERIALIZATION:
        //  - The [min,max] gate alone narrows the inverted-index bitmap; a pure
        //    `WHERE score()>c` / count(*) needs no score output column.
        //  - A synthetic FLOAT score column is materialized ONLY when score() is
        //    actually projected (SELECT score()), or the ORDER BY rule already
        //    created the slot. Injecting an output column for the filter-only case
        //    leaves the scan reading a storage-less synthetic column -> 0 rows.
        long slotId = scanOp.getBm25ScoreSlotId();
        if (slotId < 0 && projectionReferencesScore(scanOp.getProjection())) {
            String scoreColName = "__bm25_score_filter_" + matchCol.getId();
            Column scoreColumn = new Column(scoreColName, Type.FLOAT);
            table.addColumn(scoreColumn);
            ColumnRefOperator scoreColRef = context.getColumnRefFactory().create(scoreColName, Type.FLOAT, false);

            Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = new HashMap<>(scanOp.getColRefToColumnMetaMap());
            newColRefToColumnMetaMap.put(scoreColRef, scoreColumn);
            Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = new HashMap<>(scanOp.getColumnMetaToColRefMap());
            newColumnMetaToColRefMap.put(scoreColumn, scoreColRef);
            builder.setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                    .setColumnMetaToColRefMap(newColumnMetaToColRefMap);

            // Rewrite the `score()` calls in the projection to the slot ref.
            Map<ColumnRefOperator, ScalarOperator> newProjectMap = scanOp.getProjection().getColumnRefMap()
                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            e -> rewriteScoreCall(e.getValue(), scoreColRef)));
            builder.setProjection(new Projection(newProjectMap));
            slotId = scoreColRef.getId();
        }

        LogicalOlapScanOperator newScanOp = builder.build();
        // slotId >= 0 only when score() is materialized; the gate is always pushed.
        // The BE runs the scored seek (to apply the gate) whenever min/max is set,
        // and materializes the score column only when the slot id is present.
        newScanOp.setBm25ScoreSlotId(slotId);
        newScanOp.setBm25ScoreMin(min);
        newScanOp.setBm25ScoreMax(max);
        return List.of(OptExpression.create(newScanOp));
    }

    // True if `op` is a top-level `score() <cmp> const` (or the const-on-left mirror).
    private boolean isScoreComparison(ScalarOperator op) {
        return scoreBound(op) != null;
    }

    // True if the scan projection actually outputs score() (SELECT score()), so a
    // score column must be materialized. Filter-only queries (count(*), SELECT
    // other_col WHERE score()>c) return false: only the gate is needed.
    private boolean projectionReferencesScore(Projection projection) {
        if (projection == null) {
            return false;
        }
        return projection.getColumnRefMap().values().stream().anyMatch(this::containsScoreCall);
    }

    private boolean containsScoreCall(ScalarOperator op) {
        if (isScoreCall(op)) {
            return true;
        }
        for (ScalarOperator child : op.getChildren()) {
            if (containsScoreCall(child)) {
                return true;
            }
        }
        return false;
    }

    // Returns the inclusive [lower, upper] bound a score() comparison contributes, or
    // null if `op` is not such a comparison. `>`/`>=` give a lower bound, `<`/`<=` an
    // upper bound, `=` both. The inclusive-on-`>` boundary is conservative for float
    // BM25 scores (exact equality to the bound is measure-zero in practice).
    private double[] scoreBound(ScalarOperator op) {
        // score() [NOT] BETWEEN lo AND hi  ->  inclusive [lo, hi].
        if (op instanceof BetweenPredicateOperator) {
            BetweenPredicateOperator bt = (BetweenPredicateOperator) op;
            if (bt.isNotBetween() || !isScoreExpr(bt.getChild(0))) {
                return null;
            }
            Double lo = constDouble(bt.getChild(1));
            Double hi = constDouble(bt.getChild(2));
            return (lo == null || hi == null) ? null : new double[] {lo, hi};
        }
        if (!(op instanceof BinaryPredicateOperator)) {
            return null;
        }
        BinaryPredicateOperator bp = (BinaryPredicateOperator) op;
        ScalarOperator left = bp.getChild(0);
        ScalarOperator right = bp.getChild(1);
        BinaryType type = bp.getBinaryType();
        double v;
        boolean scoreOnLeft;
        Double rightConst = constDouble(right);
        Double leftConst = constDouble(left);
        if (isScoreExpr(left) && rightConst != null) {
            v = rightConst;
            scoreOnLeft = true;
        } else if (isScoreExpr(right) && leftConst != null) {
            v = leftConst;
            scoreOnLeft = false;
        } else {
            return null;
        }
        // Normalize so the comparison reads `score() <type> v` (flip if const is on the left).
        if (!scoreOnLeft) {
            switch (type) {
                case LT:
                    type = BinaryType.GT;
                    break;
                case LE:
                    type = BinaryType.GE;
                    break;
                case GT:
                    type = BinaryType.LT;
                    break;
                case GE:
                    type = BinaryType.LE;
                    break;
                default:
                    break;
            }
        }
        switch (type) {
            case GT:
            case GE:
                return new double[] {v, Double.POSITIVE_INFINITY};
            case LT:
            case LE:
                return new double[] {Double.NEGATIVE_INFINITY, v};
            case EQ:
                return new double[] {v, v};
            default:
                return null;
        }
    }

    // A numeric constant's double value, or null if `op` is not a non-null numeric constant.
    // Reads via the boxed value (Integer/Long/Float/Double/BigDecimal all extend Number);
    // ConstantOperator.getDouble() would ClassCastException on a non-Double box (e.g. the
    // INT `200` in a sibling `status=200` conjunct we probe while scanning the predicate).
    private Double constDouble(ScalarOperator op) {
        if (!(op instanceof ConstantOperator)) {
            return null;
        }
        ConstantOperator c = (ConstantOperator) op;
        if (c.isNull() || !c.getType().isNumericType()) {
            return null;
        }
        Object v = c.getValue();
        return (v instanceof Number) ? ((Number) v).doubleValue() : null;
    }

    // score(), possibly wrapped in casts (`CAST(score() AS DOUBLE)` from type coercion
    // against the comparison constant).
    private boolean isScoreExpr(ScalarOperator op) {
        while (op instanceof CastOperator) {
            op = op.getChild(0);
        }
        return isScoreCall(op);
    }

    private boolean isScoreCall(ScalarOperator op) {
        return op instanceof CallOperator
                && FunctionSet.MATCH_SCORE.equalsIgnoreCase(((CallOperator) op).getFnName())
                && op.getChildren().isEmpty();
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

    private ScalarOperator rewriteScoreCall(ScalarOperator op, ColumnRefOperator scoreColRef) {
        if (isScoreCall(op)) {
            return scoreColRef;
        }
        for (int i = 0; i < op.getChildren().size(); i++) {
            op.setChild(i, rewriteScoreCall(op.getChild(i), scoreColRef));
        }
        return op;
    }
}
