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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.ArrayList;
import java.util.List;

/**
 * Derives the ordered row-id key expressions for a retractable IVM materialized view by walking the
 * query's AST relation tree bottom-up. The row id is the MV primary key, so each output row's identity
 * is built from the rows that produced it. There is one case per relation type, so growing operator
 * coverage is filling in a case rather than changing the {@code List<Expr>} contract:
 * <ul>
 *   <li>{@link SelectRelation}: a projection / filter forwards the FROM relation's identity.
 *   <li>cloud-native PRIMARY KEY {@link TableRelation}: its primary key columns.
 *   <li>inner/cross {@link JoinRelation}: both sides' row-id keys concatenated.
 * </ul>
 *
 * <p>Any other shape (sub-query, union, outer join, ...) returns {@code null}: it is not a maintainable
 * retractable shape yet, so the analyzer rejects it at CREATE. Those cases are added by the follow-up
 * retraction PRs (union, derived table).
 */
public final class IvmRowIdDeriver {

    private IvmRowIdDeriver() {
    }

    public static List<Expr> deriveRowIdKeys(Relation relation) {
        if (relation instanceof SelectRelation) {
            return deriveSelectKeys((SelectRelation) relation);
        }
        if (relation instanceof TableRelation) {
            return derivePrimaryKeyColumns((TableRelation) relation);
        }
        if (relation instanceof JoinRelation) {
            return deriveJoinKeys((JoinRelation) relation);
        }
        // Any other shape returns null and is rejected at the analyzer gate (see class doc).
        return null;
    }

    private static List<Expr> deriveSelectKeys(SelectRelation select) {
        return deriveRowIdKeys(select.getRelation());
    }

    private static List<Expr> derivePrimaryKeyColumns(TableRelation tableRelation) {
        Table table = tableRelation.getTable();
        if (!(table instanceof OlapTable)) {
            return null;
        }
        OlapTable olapTable = (OlapTable) table;
        if (!olapTable.isCloudNativeTableOrMaterializedView() || olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
            return null;
        }
        List<Expr> keys = new ArrayList<>();
        for (Column column : olapTable.getBaseSchema()) {
            if (column.isKey()) {
                keys.add(new SlotRef(tableRelation.getResolveTableName(), column.getName()));
            }
        }
        return keys.isEmpty() ? null : keys;
    }

    private static List<Expr> deriveJoinKeys(JoinRelation joinRelation) {
        // Identity of a join row = both sides' row ids concatenated. Only inner/cross are maintainable
        // (rewriteRelation rejects other join types); a side without a row id (e.g. append-only base) is not.
        if (!IVMAnalyzer.IVM_SUPPORTED_JOIN_OPS.contains(joinRelation.getJoinOp())) {
            return null;
        }
        List<Expr> leftKeys = deriveRowIdKeys(joinRelation.getLeft());
        List<Expr> rightKeys = deriveRowIdKeys(joinRelation.getRight());
        if (leftKeys == null || rightKeys == null) {
            return null;
        }
        List<Expr> keys = new ArrayList<>(leftKeys);
        keys.addAll(rightKeys);
        return keys;
    }
}
