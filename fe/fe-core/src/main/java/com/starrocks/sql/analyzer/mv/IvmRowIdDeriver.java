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
 * </ul>
 *
 * <p>Any other shape (join, sub-query, union, aggregate group-by, ...) returns {@code null}: it is not a
 * maintainable retractable shape in this projection/filter foundation, so the analyzer rejects it at CREATE.
 * Those cases are added by the follow-up retraction PRs (aggregate, join, union, derived table).
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
}
