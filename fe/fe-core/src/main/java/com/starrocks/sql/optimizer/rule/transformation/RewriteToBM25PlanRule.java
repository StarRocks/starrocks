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
import com.starrocks.common.BM25SearchOptions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.FloatType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Mechanical FE rewrite for builtin GIN BM25 full-text top-N ranking. Pattern
 * {@code LOGICAL_TOPN -> LOGICAL_OLAP_SCAN}: synthesize a per-query {@code __bm25_score} column only in the
 * scan operator's colRef maps (never {@code Table.addColumn}), attach {@link BM25SearchOptions}, and rewrite
 * the scan projection so the bare {@code score()} call resolves to the synthetic score column.
 *
 * <p>{@code score()} is zero-arg, so its indexed column and query come from the single
 * {@code MatchExprOperator} in the scan predicate, not from function args.
 *
 * <p>All user-visible enforcement already happened fail-fast in the analyzer ({@code Bm25ScoreValidator}),
 * so a {@code score()} query reaching here is well-formed. This rule only does the mechanical rewrite: it
 * returns an empty list for a plain (non-score) top-N query, and throws {@link IllegalStateException} if an
 * already-validated {@code score()} query cannot be rewritten (an internal-invariant violation).
 */
public class RewriteToBM25PlanRule extends TransformationRule {

    private static final Logger LOG = LogManager.getLogger(RewriteToBM25PlanRule.class);

    public RewriteToBM25PlanRule() {
        super(RuleType.TF_BM25_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();

        // Do not re-fire on a scan this rule already rewrote (its score() is gone from the projection and
        // the BM25 options are set); the transform would return empty anyway, but bail early and explicitly.
        if (scanOp.getProjection() == null || scanOp.getBm25SearchOptions().isEnable()) {
            return false;
        }

        if (topNOp.getLimit() <= 0 || topNOp.getOrderByElements().size() != 1) {
            return false;
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();

        Optional<BM25SearchOptions.Bm25ColumnQuery> optionalColumn = extractBM25Info(topNOp, scanOp);
        if (optionalColumn.isEmpty()) {
            // Not a score() query: a plain top-N over an OLAP scan. Do not rewrite.
            return List.of();
        }

        return List.of(rewriteOptByScoreColumn(topNOp, scanOp, context, optionalColumn.get()));
    }

    private OptExpression rewriteOptByScoreColumn(LogicalTopNOperator topNOp,
                                                  LogicalOlapScanOperator scanOp,
                                                  OptimizerContext context,
                                                  BM25SearchOptions.Bm25ColumnQuery columnQuery) {
        // The score column is per-query synthetic state: it only needs to live in the scan operator's
        // colRef maps (PlanFragmentBuilder builds the scan slot's Column from colRefToColumnMetaMap). It
        // must NOT be added to the shared catalog table's fullSchema -- Table.addColumn appends to a
        // non-dedup List, so doing that would accumulate duplicate "__bm25_score" columns on the live
        // table across queries.
        String scoreColumnName = BM25SearchOptions.SCORE_COLUMN_NAME;
        Column scoreColumn = new Column(scoreColumnName, FloatType.DOUBLE);

        ColumnRefOperator scoreColRef = context.getColumnRefFactory().create(scoreColumnName, FloatType.DOUBLE, false);
        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = new HashMap<>(scanOp.getColRefToColumnMetaMap());
        newColRefToColumnMetaMap.put(scoreColRef, scoreColumn);

        Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = new HashMap<>(scanOp.getColumnMetaToColRefMap());
        newColumnMetaToColRefMap.put(scoreColumn, scoreColRef);

        BM25SearchOptions opts = new BM25SearchOptions();
        opts.setEnable(true);
        opts.setColumns(List.of(columnQuery));
        opts.setScoreColumnName(scoreColumnName);
        opts.setScoreSlotId(scoreColRef.getId());
        opts.setK1(context.getSessionVariable().getBm25K1());
        opts.setB(context.getSessionVariable().getBm25B());

        // Top-k pushdown: push LIMIT (+OFFSET) into the scored scan so the BE keeps only the top-k rows by
        // score instead of scoring/returning every matched row. Only safe when ORDER BY score() is DESC
        // (the bounded top-k keeps the highest scores) AND MATCH is the entire scan predicate -- a post-scan
        // scalar filter (e.g. `MATCH 'x' AND status=200`) would drop rows after the top-k and could shrink
        // the result below the limit (under-return). Otherwise leave topk=0: the BE scores every matched row
        // and the TopN above applies the limit.
        if (!topNOp.getOrderByElements().get(0).isAscending()
                && scanOp.getPredicate() instanceof MatchExprOperator) {
            opts.setTopk(topNOp.getLimit() + Math.max(0L, topNOp.getOffset()));
        }

        // Replace every occurrence of the score() call in the scan projection with the score column ref,
        // so both the ORDER BY key and any SELECT-list score() resolve to the BE-produced score slot.
        Map<ColumnRefOperator, ScalarOperator> newScanProjectMap =
                scanOp.getProjection().getColumnRefMap().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> rewriteScalarOperatorByScoreColumn(entry.getValue(), scoreColRef)
                        ));

        LogicalOlapScanOperator newScanOp = LogicalOlapScanOperator.builder()
                .withOperator(scanOp)
                .setProjection(new Projection(newScanProjectMap))
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setColumnMetaToColRefMap(newColumnMetaToColRefMap)
                .setBm25SearchOptions(opts)
                .build();

        return OptExpression.create(topNOp, OptExpression.create(newScanOp));
    }

    private ScalarOperator rewriteScalarOperatorByScoreColumn(ScalarOperator scalarOperator,
                                                              ColumnRefOperator scoreColRef) {
        // Replace every bare builtin score() call (SELECT-list and ORDER BY alike) with the single synthetic
        // score column. Match only the builtin zero-arg score() (CallOperator.isBM25ScoreCall), never a user
        // function named score.
        if (scalarOperator instanceof CallOperator && ((CallOperator) scalarOperator).isBM25ScoreCall()) {
            return scoreColRef;
        }

        for (int i = 0; i < scalarOperator.getChildren().size(); i++) {
            ScalarOperator child = scalarOperator.getChild(i);
            scalarOperator.setChild(i, rewriteScalarOperatorByScoreColumn(child, scoreColRef));
        }

        return scalarOperator;
    }

    /**
     * Extract the BM25 rewrite info from a {@code TopN -> OlapScan}. Returns empty when the query is not a
     * rewritable {@code score()} top-N: the ORDER BY key is not a bare {@code score()}, or the MATCH is no
     * longer the expected {@code <column> MATCH <constant>} shape. A surviving {@code score()} is then
     * rejected downstream by the {@code ScalarOperatorToExpr} guard with a clear message.
     */
    private Optional<BM25SearchOptions.Bm25ColumnQuery> extractBM25Info(LogicalTopNOperator topNOp,
                                                                        LogicalOlapScanOperator scanOp) {
        ColumnRefOperator outColRef = topNOp.getOrderByElements().get(0).getColumnRef();
        ScalarOperator inOperator = scanOp.getProjection().getColumnRefMap().get(outColRef);
        // The ORDER BY key must resolve to the bare zero-arg builtin score() (CallOperator.isBM25ScoreCall);
        // a plain top-N over an OLAP scan, or an ORDER BY on a user function named score, leaves it untouched.
        if (!(inOperator instanceof CallOperator) || !((CallOperator) inOperator).isBM25ScoreCall()) {
            return Optional.empty();
        }

        // score() is present: normally exactly one MATCH (analyzer-guaranteed). If an intervening rewrite
        // reshaped it, skip the rewrite and let the downstream guard reject the surviving score() cleanly.
        List<MatchExprOperator> matchExprs = new ArrayList<>();
        if (scanOp.getPredicate() != null) {
            for (ScalarOperator conjunct : Utils.extractConjuncts(scanOp.getPredicate())) {
                if (conjunct instanceof MatchExprOperator) {
                    matchExprs.add((MatchExprOperator) conjunct);
                }
            }
        }
        if (matchExprs.size() != 1) {
            LOG.warn("BM25 score() reached rewrite with {} MATCH predicates; skipping rewrite", matchExprs.size());
            return Optional.empty();
        }

        MatchExprOperator matchExpr = matchExprs.get(0);
        ScalarOperator matchColumn = matchExpr.getChild(0);
        ScalarOperator matchQuery = matchExpr.getChild(1);
        if (!(matchColumn instanceof ColumnRefOperator) || !(matchQuery instanceof ConstantOperator)) {
            LOG.warn("BM25 score() MATCH is not <column> MATCH <constant> at rewrite; skipping rewrite");
            return Optional.empty();
        }

        Column column = scanOp.getColRefToColumnMetaMap().get((ColumnRefOperator) matchColumn);
        if (column == null) {
            LOG.warn("BM25 score() MATCH column is not bound to the scanned table at rewrite; skipping rewrite");
            return Optional.empty();
        }

        String query = String.valueOf(((ConstantOperator) matchQuery).getValue());
        // columnId is the stable Column.columnId (unchanged by rename); the BE resolves it to the index the
        // same way it resolves a MATCH predicate's column.
        return Optional.of(new BM25SearchOptions.Bm25ColumnQuery(column.getColumnId().getId(), query));
    }
}
