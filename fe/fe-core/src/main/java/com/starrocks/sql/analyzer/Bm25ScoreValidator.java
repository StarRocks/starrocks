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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.InvertedIndexParams;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Analyzer-phase, per-query-block fail-fast validator for the builtin BM25 {@code score()} function.
 *
 * <p>score() has no BE fallback: used outside the supported top-N shape it would return wrong/zero scores,
 * so every unsupported use is rejected here with a {@link SemanticException}. Only invoked for blocks that
 * reference score() (flagged during expression analysis).
 *
 * <p>"score()" means the builtin: a zero-arg, unqualified call (see {@code FunctionCallExpr.isBM25ScoreCall});
 * a user function {@code score(col)} / {@code db.score()} is not it. score() reaching a context this
 * per-SelectRelation validator does not see (JOIN ON, VALUES, set-operation ORDER BY, table-function arg) is
 * rejected by {@code AnalyzerUtils.verifyNoScoreFunction}.
 */
public class Bm25ScoreValidator {

    private static final String SUPPORTED_SHAPE =
            "score() is only supported for full-text top-N ranking: "
                    + "SELECT ... WHERE <col> MATCH|MATCH_ANY|MATCH_ALL '<q>' ORDER BY score() [DESC] LIMIT <n>; "
                    + "it cannot appear in WHERE/HAVING/GROUP BY, aggregates, window functions, JOIN conditions, "
                    + "or across multiple MATCH columns";

    private static final String INDEX_OPTIONS_KEY =
            InvertedIndexParams.IndexParamsKey.INDEX_OPTIONS.name().toLowerCase(Locale.ROOT);
    private static final String DOCS_AND_FREQS =
            InvertedIndexParams.InvertedIndexOption.DOCS_AND_FREQS.name();
    private static final String IMP_LIB_KEY =
            InvertedIndexParams.CommonIndexParamKey.IMP_LIB.name().toLowerCase(Locale.ROOT);
    private static final String BUILTIN =
            InvertedIndexParams.InvertedIndexImpType.BUILTIN.name();

    private Bm25ScoreValidator() {
    }

    public static void validate(SelectRelation queryBlock) {
        // score() may only appear in the SELECT list and the ORDER BY key -- reject it elsewhere.
        if (containsScore(queryBlock.getPredicate())
                || containsScore(queryBlock.getHaving())
                || anyContainsScore(queryBlock.getGroupBy())) {
            throw new SemanticException(SUPPORTED_SHAPE);
        }

        // A window/analytic function makes the plan TopN -> Window -> Scan, which the mechanical rewrite
        // (pattern TopN -> OlapScan) cannot match, so score() would survive unrewritten and hit the BE.
        if (queryBlock.hasAnalyticInfo()) {
            throw new SemanticException(
                    "score() cannot be combined with window/analytic functions; " + SUPPORTED_SHAPE);
        }

        // After GROUP BY / aggregation / DISTINCT a row no longer maps to a single document, so a
        // per-document score is meaningless.
        boolean aggregationBlock = (queryBlock.getAggregate() != null && !queryBlock.getAggregate().isEmpty())
                || (queryBlock.getGroupBy() != null && !queryBlock.getGroupBy().isEmpty())
                || queryBlock.getGroupByClause() != null
                || queryBlock.isDistinct();
        if (aggregationBlock) {
            throw new SemanticException(SUPPORTED_SHAPE);
        }

        // score() must bind to a single base OLAP-table scan in its own query block.
        Relation from = queryBlock.getRelation();
        if (!(from instanceof TableRelation) || !(((TableRelation) from).getTable() instanceof OlapTable)) {
            throw new SemanticException(
                    "score() must directly rank a single full-text base table in its own query block; " + SUPPORTED_SHAPE);
        }
        OlapTable table = (OlapTable) ((TableRelation) from).getTable();

        // A single ORDER BY key that is (or resolves to) a bare score(), plus a positive LIMIT.
        List<OrderByElement> orderBy = queryBlock.getOrderBy();
        if (orderBy == null || orderBy.size() != 1 || !orderKeyIsBareScore(queryBlock, orderBy.get(0).getExpr())) {
            throw new SemanticException(
                    "score() requires a single ORDER BY score() key; " + SUPPORTED_SHAPE);
        }
        if (queryBlock.getLimit() == null || !queryBlock.getLimit().hasLimit() || queryBlock.getLimit().getLimit() <= 0) {
            throw new SemanticException(
                    "score() requires a positive LIMIT for top-N ranking; " + SUPPORTED_SHAPE);
        }

        // WHERE must contain exactly one MATCH predicate, as a top-level conjunct (single column).
        Expr where = queryBlock.getPredicate();
        List<MatchExpr> matchExprs = new ArrayList<>();
        if (where != null) {
            where.collect(MatchExpr.class, matchExprs);
        }
        if (matchExprs.isEmpty()) {
            throw new SemanticException(
                    "score() requires a MATCH predicate on the ranked column in WHERE; " + SUPPORTED_SHAPE);
        }
        if (matchExprs.size() > 1) {
            throw new SemanticException(
                    "score() supports exactly one MATCH column; " + SUPPORTED_SHAPE);
        }
        MatchExpr matchExpr = matchExprs.get(0);
        // MATCH must be a top-level AND conjunct; an OR-nested MATCH does not define a single candidate set.
        boolean isTopLevel = AnalyzerUtils.extractConjuncts(where).stream().anyMatch(conjunct -> conjunct == matchExpr);
        if (!isTopLevel) {
            throw new SemanticException(
                    "score() does not support a MATCH predicate nested inside OR; " + SUPPORTED_SHAPE);
        }

        // The MATCH column must carry a builtin GIN index built with index_options='DOCS_AND_FREQS', which
        // stores the term frequencies / doc lengths BM25 needs. clucene cannot serve that data, so a
        // non-builtin GIN is rejected even if it carries DOCS_AND_FREQS.
        String matchColumnName = matchColumnName(matchExpr);
        if (matchColumnName == null) {
            throw new SemanticException(
                    "score() MATCH predicate must reference an indexed column; " + SUPPORTED_SHAPE);
        }
        Index ginIndex = findGinIndex(table, matchColumnName);
        if (ginIndex == null) {
            throw new SemanticException(
                    "score() requires a GIN full-text index on the MATCH column '" + matchColumnName + "'");
        }
        Map<String, String> props = ginIndex.getProperties();
        String impLib = IndexAnalyzer.getPropertyIgnoreCase(props, IMP_LIB_KEY);
        if (impLib == null || !impLib.equalsIgnoreCase(BUILTIN)) {
            throw new SemanticException(
                    "score() requires a builtin GIN index (imp_lib='builtin') on column '" + matchColumnName
                            + "'; the clucene implementation does not produce the BM25 posting data scoring needs");
        }
        String indexOptions = IndexAnalyzer.getPropertyIgnoreCase(props, INDEX_OPTIONS_KEY);
        if (indexOptions == null || !indexOptions.equalsIgnoreCase(DOCS_AND_FREQS)) {
            throw new SemanticException(
                    "score() requires the GIN index on column '" + matchColumnName
                            + "' to be built with index_options='DOCS_AND_FREQS' (rebuild the index to enable BM25 scoring)");
        }
    }

    /**
     * Whether the single ORDER BY key is (or resolves to) a bare {@code score()}: written directly, or a
     * SELECT alias resolving to a bare {@code score()} output (an ordinal is already resolved to that slot
     * before this runs). Ordering by any non-score column is rejected -- the rewrite cannot score it.
     */
    private static boolean orderKeyIsBareScore(SelectRelation queryBlock, Expr orderKey) {
        if (isBareScore(orderKey)) {
            return true;
        }
        if (orderKey instanceof SlotRef) {
            String name = ((SlotRef) orderKey).getColumnName();
            List<String> outNames = queryBlock.getColumnOutputNames();
            List<Expr> outExprs = queryBlock.getOutputExpression();
            if (name != null && outNames != null && outExprs != null) {
                for (int i = 0; i < outNames.size() && i < outExprs.size(); i++) {
                    if (name.equalsIgnoreCase(outNames.get(i)) && isBareScore(outExprs.get(i))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isBareScore(Expr expr) {
        return expr instanceof FunctionCallExpr && ((FunctionCallExpr) expr).isBM25ScoreCall();
    }

    private static boolean anyContainsScore(List<Expr> exprs) {
        if (exprs == null) {
            return false;
        }
        for (Expr expr : exprs) {
            if (containsScore(expr)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsScore(Expr expr) {
        if (expr == null) {
            return false;
        }
        if (isBareScore(expr)) {
            return true;
        }
        for (Expr child : expr.getChildren()) {
            if (containsScore(child)) {
                return true;
            }
        }
        return false;
    }

    private static String matchColumnName(MatchExpr matchExpr) {
        Expr column = matchExpr.getChild(0);
        if (column instanceof SlotRef) {
            return ((SlotRef) column).getColumnName();
        }
        return null;
    }

    private static Index findGinIndex(OlapTable table, String columnName) {
        // Compare by the column's stable ColumnId, not the raw name: the index stores the rename-stable
        // ColumnId while columnName is the current display name, so after RENAME COLUMN a name comparison
        // would miss the index. Resolve the column (by current name) to its ColumnId first.
        Column matchColumn = table.getColumn(columnName);
        if (matchColumn == null) {
            return null;
        }
        ColumnId targetColumnId = matchColumn.getColumnId();
        List<Index> indexes = table.getIndexes();
        if (indexes == null) {
            return null;
        }
        for (Index index : indexes) {
            if (index.getIndexType() != IndexDef.IndexType.GIN) {
                continue;
            }
            for (ColumnId columnId : index.getColumns()) {
                if (columnId.equalsIgnoreCase(targetColumnId)) {
                    return index;
                }
            }
        }
        return null;
    }
}
