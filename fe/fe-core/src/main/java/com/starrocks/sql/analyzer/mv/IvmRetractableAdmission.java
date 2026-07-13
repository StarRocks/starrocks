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
import com.starrocks.sql.ast.SelectRelation;
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
            // Aggregate over a PK base needs per-group recompute under retraction (a later PR); reject until then.
            if (IvmBaseTableValidator.hasCloudNativePrimaryKeyBase(selectRelation)) {
                throw new SemanticException("IVMAnalyzer does not yet support an aggregate materialized view over a "
                        + "cloud-native PRIMARY KEY base");
            }
            return true;
        }
        boolean retractable = IvmRowIdInjector.injectRowId(statement, selectRelation);
        // __ROW_ID__ must stay the sole MV key: a user ORDER BY folds into the key and would let a value
        // update leave a stale row (sort-only: a later PR). statement is null on the refresh path -- skip.
        if (retractable && statement != null && CollectionUtils.isNotEmpty(statement.getOrderByElements())) {
            throw new SemanticException("IVMAnalyzer does not yet support ORDER BY for a materialized view "
                    + "over a cloud-native PRIMARY KEY base");
        }
        return retractable;
    }

    /**
     * A block retractable only through a nested input carries no {@code __ROW_ID__} on its own output, so its
     * deletes would be dropped. A PK base thus requires the row id on this block's output; nested PK shapes
     * (join / union / sub-query) come in later PRs.
     */
    static void requirePkBaseHasRowId(SelectRelation selectRelation, boolean rowIdOnOutput) {
        if (!rowIdOnOutput && IvmBaseTableValidator.hasCloudNativePrimaryKeyBase(selectRelation)) {
            throw new SemanticException("IVMAnalyzer only supports a single-table projection/filter materialized "
                    + "view over a cloud-native PRIMARY KEY base in this version");
        }
    }
}
