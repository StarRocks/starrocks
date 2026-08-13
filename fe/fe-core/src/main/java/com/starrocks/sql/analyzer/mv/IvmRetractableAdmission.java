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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * Enterprise-only admission gates for retractable (delete/update) IVM over a cloud-native PRIMARY KEY base.
 * {@link IVMAnalyzer#rewriteSelectRelation} and {@link IVMAnalyzer#rewriteSetRelation} call these so the shared
 * analyzer keeps only stable one-line hooks and the retraction rules (which each later PR extends) stay here.
 */
final class IvmRetractableAdmission {
    private IvmRetractableAdmission() {
    }

    /**
     * Decide whether a single (projection / filter / aggregate) block carries {@code __ROW_ID__} on its own
     * output -- prepending {@code encode(primary key)} for a retractable PK projection ({@link IvmRowIdInjector})
     * -- and reject the shapes this foundation does not yet maintain over a PK base.
     *
     * @param isAggregate whether {@code rewriteAggregate} already prepended {@code encode(group keys)}
     * @return whether this block's own output carries {@code __ROW_ID__}
     */
    static boolean admitRowIdOnOutput(CreateMaterializedViewStatement statement, SelectRelation selectRelation,
                                      boolean isAggregate, Integer pinnedEncodeRowIdVersion) {
        if (isAggregate) {
            // Aggregate over a cloud-native PK base is maintained by per-group recompute in
            // IvmDeltaAggregateRule.transformRetractable; a mixed PK/non-PK join is rejected there, not here.
            if (IvmBaseTableValidator.hasCloudNativePrimaryKeyBase(selectRelation)) {
                rejectOrderBy(statement);
            }
            return true;
        }
        boolean retractable = IvmRowIdInjector.injectRowId(statement, selectRelation, pinnedEncodeRowIdVersion);
        if (retractable) {
            rejectOrderBy(statement);
        }
        return retractable;
    }

    // __ROW_ID__ must stay the sole MV key: a user ORDER BY folds into the key, but net-collapse keys only
    // on __ROW_ID__, so an update that changes a value or aggregate output leaves the old sort-keyed row
    // stale (sort-only: a later PR). statement is null on the refresh path -- skip.
    private static void rejectOrderBy(CreateMaterializedViewStatement statement) {
        if (statement != null && CollectionUtils.isNotEmpty(statement.getOrderByElements())) {
            throw new SemanticException("IVMAnalyzer does not yet support ORDER BY for a materialized view "
                    + "over a cloud-native PRIMARY KEY base");
        }
    }

    /**
     * A block retractable only through a nested input carries no {@code __ROW_ID__} on its own output, so its
     * deletes would be dropped. A PK base thus requires the row id on this block's output; the remaining nested
     * PK shapes (union / sub-query) come in later PRs.
     *
     * <p>{@code rewriteRelation} runs first and rejects every other row-id-less shape (unsupported join type,
     * non-forwardable derived table, mixed union) with its own reason, which leaves a base mix as what actually
     * reaches here -- so name the base instead of describing a shape.
     */
    static void requirePkBaseHasRowId(SelectRelation selectRelation, boolean rowIdOnOutput) {
        if (rowIdOnOutput || !IvmBaseTableValidator.hasCloudNativePrimaryKeyBase(selectRelation)) {
            return;
        }
        String nonPkBase = IvmBaseTableValidator.findNonPrimaryKeyBaseName(selectRelation);
        if (nonPkBase != null) {
            throw new SemanticException("IVM over a cloud-native PRIMARY KEY base requires every base to be a "
                    + "cloud-native PRIMARY KEY table, but '%s' is not: the materialized view output would carry "
                    + "no %s, so a delete or update on the PRIMARY KEY base would have no row to target. A single "
                    + "PRIMARY KEY table and an INNER / CROSS JOIN of PRIMARY KEY tables are both supported.",
                    nonPkBase, IvmOpUtils.COLUMN_ROW_ID);
        }
        throw new SemanticException("IVM over a cloud-native PRIMARY KEY base requires this materialized view "
                + "output to carry %s, which this query shape cannot derive, so a delete or update on the "
                + "PRIMARY KEY base would have no row to target.", IvmOpUtils.COLUMN_ROW_ID);
    }

    /**
     * A retractable join is maintained by net-collapse, which orders by every output column to cancel the
     * intermediate rows the two joined deltas emit for one {@code __ROW_ID__} when both sides change the same
     * key. A non-orderable output type ({@code canOrderBy() == false}, e.g. JSON / BITMAP) has no such order,
     * so reject it at CREATE instead of failing the first delete/update refresh. An aggregate collapses each
     * group to one row and is not net-collapsed, so it is exempt.
     */
    static void requireOrderableJoinOutput(SelectRelation selectRelation, boolean rowIdOnOutput,
                                           boolean isAggregate) {
        if (!rowIdOnOutput || isAggregate || !(selectRelation.getRelation() instanceof JoinRelation)) {
            return;
        }
        for (Expr output : selectRelation.getOutputExpression()) {
            if (!output.getType().canOrderBy()) {
                throw new SemanticException("IVMAnalyzer does not support a join materialized view with a "
                        + "non-orderable output column type over a cloud-native PRIMARY KEY base, but got: "
                        + output.getType());
            }
        }
    }

    /**
     * A {@code SELECT *} / {@code t.*} over a forwardable derived table re-expands during MV re-analysis and
     * picks up the inner block's own prepended {@code __ROW_ID__}, colliding with the outer {@code __ROW_ID__}
     * ("Duplicate column name"). Reject it with a clear message; list the columns explicitly instead.
     */
    static void rejectStarOverDerivedTable(SelectRelation selectRelation) {
        if (!(selectRelation.getRelation() instanceof SubqueryRelation)
                || !IvmRowIdDeriver.canForwardSubqueryRowId((SubqueryRelation) selectRelation.getRelation())) {
            return;
        }
        if (selectRelation.getSelectList().getItems().stream().anyMatch(SelectListItem::isStar)) {
            throw new SemanticException("IVMAnalyzer does not support SELECT * over a derived table on a "
                    + "cloud-native PRIMARY KEY base; list the columns explicitly");
        }
    }

    /**
     * A derived table with an explicit column alias list ({@code t(a, b)}) fixes the sub-query's column count,
     * but exposeKeysAsOutputs appends a hidden __rowid_key_N__ to forward the row id -- so the re-analysis
     * count check rejects the MV with an opaque error. Reject it up front; alias the columns in the sub-query.
     */
    static void rejectExplicitColumnAliasDerivedTable(SelectRelation selectRelation) {
        if (!(selectRelation.getRelation() instanceof SubqueryRelation)
                || !IvmRowIdDeriver.canForwardSubqueryRowId((SubqueryRelation) selectRelation.getRelation())) {
            return;
        }
        if (CollectionUtils.isNotEmpty(selectRelation.getRelation().getExplicitColumnNames())) {
            throw new SemanticException("IVMAnalyzer does not support a derived table with an explicit column "
                    + "alias list (e.g. t(a, b)) over a cloud-native PRIMARY KEY base; alias the columns inside "
                    + "the sub-query instead");
        }
    }

    /**
     * A GROUP BY / DISTINCT union branch lowers to an aggregation, and net-collapse gates join tuple cancellation
     * off containsAggregate -- so a sibling join branch would silently keep its stale intermediate. Reject it; a
     * retractable union branch must be a plain projection / filter / join.
     */
    static void rejectGroupedUnionBranch(SelectRelation selectChild) {
        if (selectChild.getGroupByClause() != null || selectChild.isDistinct()) {
            throw new SemanticException("IVMAnalyzer does not support a GROUP BY or DISTINCT branch in a "
                    + "UNION ALL materialized view");
        }
    }

    /**
     * Decide a UNION ALL once IVMAnalyzer has rewritten every branch: admit a fully-retractable cloud-native
     * PRIMARY KEY union -- re-key each branch to {@code encode(branch ordinal, keys)}
     * ({@link IvmRowIdInjector#discriminateUnionBranchRowIds}) so two branches whose keys collide stay distinct
     * rows under net-collapse -- reject a union that mixes a retractable and an append-only branch, or return
     * {@code false} to leave a fully append-only union to the community AUTO_INCREMENT path.
     */
    static boolean admitRetractableUnion(CreateMaterializedViewStatement statement, List<QueryRelation> children,
                                         int retractableBranches, Integer pinnedEncodeRowIdVersion) {
        if (retractableBranches == 0) {
            return false;
        }
        if (retractableBranches != children.size()) {
            throw new SemanticException("IVMAnalyzer does not support a UNION ALL that mixes a retractable "
                    + "cloud-native PRIMARY KEY branch with an append-only branch");
        }
        IvmRowIdInjector.discriminateUnionBranchRowIds(statement, children, pinnedEncodeRowIdVersion);
        return true;
    }
}
