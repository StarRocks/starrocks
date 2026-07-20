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
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Enterprise-only admission gates for retractable (delete/update) IVM over a cloud-native PRIMARY KEY base.
 * {@link IVMAnalyzer#rewriteSelectRelation} calls these at two fixed points so the shared analyzer keeps only
 * stable one-line hooks and the retraction rules (which each later PR extends) stay in one place.
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
                                      boolean isAggregate) {
        if (isAggregate) {
            // Aggregate over a cloud-native PK base is maintained by per-group recompute in
            // IvmDeltaAggregateRule.transformRetractable; a mixed PK/non-PK join is rejected there, not here.
            if (IvmBaseTableValidator.hasCloudNativePrimaryKeyBase(selectRelation)) {
                rejectOrderBy(statement);
            }
            return true;
        }
        boolean retractable = IvmRowIdInjector.injectRowId(statement, selectRelation);
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
     */
    static void requirePkBaseHasRowId(SelectRelation selectRelation, boolean rowIdOnOutput) {
        if (!rowIdOnOutput && IvmBaseTableValidator.hasCloudNativePrimaryKeyBase(selectRelation)) {
            throw new SemanticException("IVMAnalyzer only supports a single-table projection/filter materialized "
                    + "view over a cloud-native PRIMARY KEY base in this version");
        }
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
}
