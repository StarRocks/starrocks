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
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import org.apache.commons.collections4.CollectionUtils;

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
 *   <li>{@link SubqueryRelation} (derived table): the inner block's keys, exposed as inner output
 *       columns and forwarded through the sub-query alias.
 * </ul>
 *
 * <p>Any other shape (outer join, an aggregate / union inner sub-query, ...) returns {@code null}: it is
 * not a maintainable retractable shape yet, so the analyzer rejects it at CREATE. A top-level union is
 * keyed at the analyzer instead ({@link IvmRowIdInjector#discriminateUnionBranchRowIds}), so it stays null here.
 */
public final class IvmRowIdDeriver {

    private static final String EXPOSED_KEY_PREFIX = "__rowid_key_";

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
        if (relation instanceof SubqueryRelation) {
            return deriveSubqueryKeys((SubqueryRelation) relation);
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

    /**
     * Whether the outer block can forward a row id out of this derived table: the inner must be a plain
     * projection / filter / join over a maintainable base. An aggregate / DISTINCT inner (its identity is the
     * group keys, not the base PK) or a union / nested-sub-query inner (multi-input) cannot be forwarded. This
     * is side-effect-free (unlike {@link #deriveSubqueryKeys}), so {@code IVMAnalyzer.rewriteSubqueryRelation}
     * can reject an inner that computes a row id it cannot forward instead of admitting a row-id-less MV.
     */
    static boolean canForwardSubqueryRowId(SubqueryRelation subquery) {
        QueryRelation inner = subquery.getQueryStatement().getQueryRelation();
        if (!(inner instanceof SelectRelation)) {
            return false;
        }
        SelectRelation innerSelect = (SelectRelation) inner;
        if (CollectionUtils.isNotEmpty(innerSelect.getGroupBy())
                || CollectionUtils.isNotEmpty(innerSelect.getAggregate())
                || innerSelect.isDistinct()) {
            return false;
        }
        // A nested derived table (the inner's own FROM is another sub-query) would stack row-id exposures and
        // break the CREATE-time trial; keep the first cut to a single derived-table level.
        if (innerSelect.getRelation() instanceof SubqueryRelation) {
            return false;
        }
        return deriveRowIdKeys(innerSelect) != null;
    }

    private static List<Expr> deriveSubqueryKeys(SubqueryRelation subquery) {
        if (!canForwardSubqueryRowId(subquery)) {
            return null;
        }
        SelectRelation innerSelect = (SelectRelation) subquery.getQueryStatement().getQueryRelation();
        List<Expr> innerKeys = deriveRowIdKeys(innerSelect);
        // The inner keys reference the sub-query's own tables; the outer block can only see them once they are
        // output columns of the sub-query, so expose them and forward SlotRefs qualified by the sub-query alias.
        List<Expr> exposed = exposeKeysAsOutputs(innerSelect, innerKeys);
        List<Expr> forwarded = new ArrayList<>(exposed.size());
        for (Expr exposedKey : exposed) {
            forwarded.add(new SlotRef(subquery.getResolveTableName(), ((SlotRef) exposedKey).getColumnName()));
        }
        return forwarded;
    }

    /**
     * Append each key to the block's output as {@code <key> AS __rowid_key_<i>__} and return SlotRefs to the
     * new columns. Always injects (never reuses an already-projected key column) so the exposed set is identical
     * at CREATE and at the refresh re-derive; the cost is a redundant column when the key was already projected.
     */
    private static List<Expr> exposeKeysAsOutputs(SelectRelation block, List<Expr> keys) {
        List<SelectListItem> items = new ArrayList<>(block.getSelectList().getItems());
        List<Expr> outputs = new ArrayList<>(block.getOutputExpression());
        List<Expr> exposedRefs = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            String columnName = EXPOSED_KEY_PREFIX + i + "__";
            items.add(new SelectListItem(keys.get(i).clone(), columnName));
            outputs.add(keys.get(i).clone());
            exposedRefs.add(new SlotRef(null, columnName));
        }
        block.getSelectList().setItems(items);
        block.setOutputExpr(outputs);
        return exposedRefs;
    }
}
