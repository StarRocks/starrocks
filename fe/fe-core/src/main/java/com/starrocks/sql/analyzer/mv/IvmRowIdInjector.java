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

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;

import java.util.List;

/**
 * Injects the retractable (delete/update) {@code __ROW_ID__ = encode(primary key)} column into a cloud-native
 * PRIMARY KEY IVM query. Enterprise-only: it prepends the encoded row id the community/append-only analyzer has
 * no notion of, so it lives outside {@link IVMAnalyzer} to keep that shared file's dispatch aligned with the
 * community path. {@link IVMAnalyzer} calls {@link #injectRowId} for a single (projection/filter) block.
 */
final class IvmRowIdInjector {
    private IvmRowIdInjector() {
    }

    /**
     * Non-aggregate (projection/filter) MV over a retractable cloud-native PRIMARY KEY base: derive
     * {@code __ROW_ID__ = encode(<row-id keys>)} from the relation tree ({@link IvmRowIdDeriver}) so a
     * delete/update on the base can target the exact MV rows it produced. Returns false for any shape
     * without a stable per-row identity, leaving the append-only path to the caller's gate.
     */
    static boolean injectRowId(CreateMaterializedViewStatement statement, SelectRelation selectRelation) {
        List<Expr> rowIdKeys = IvmRowIdDeriver.deriveRowIdKeys(selectRelation);
        if (rowIdKeys == null || rowIdKeys.isEmpty()) {
            return false;
        }
        int encodeRowIdVersion = IvmOpUtils.deduceEncodeRowIdVersion(rowIdKeys);
        if (statement != null) {
            statement.setEncodeRowIdVersion(encodeRowIdVersion);
        }
        FunctionCallExpr rowIdFuncExpr = IvmOpUtils.buildRowIdFuncExpr(encodeRowIdVersion, rowIdKeys);
        prependRowIdColumn(selectRelation, rowIdFuncExpr);
        return true;
    }

    private static void prependRowIdColumn(SelectRelation selectRelation, FunctionCallExpr rowIdFuncExpr) {
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> newItems = Lists.newArrayList();
        newItems.add(new SelectListItem(rowIdFuncExpr, IvmOpUtils.COLUMN_ROW_ID));
        // clone() (not new SelectListItem(expr, alias)) so a star item (SELECT *, expr == null) is copied
        // as a star instead of NPEing on getExpr().clone(); the analyzer expands it into outputExpression.
        selectList.getItems().forEach(item -> newItems.add(item.clone()));
        selectList.setItems(newItems);

        List<Expr> newOutputExpressions = Lists.newArrayList();
        newOutputExpressions.add(rowIdFuncExpr);
        selectRelation.getOutputExpression().forEach(expr -> newOutputExpressions.add(expr.clone()));
        selectRelation.setOutputExpr(newOutputExpressions);
    }

    /**
     * Retractable UNION ALL: {@link IVMAnalyzer} has already injected each branch's own {@code encode(row-id
     * keys)}. Re-key each as {@code encode(branch ordinal, keys)} under one shared (max) version so the ordinal
     * keeps two branches with equal keys as distinct rows -- net-collapse keys only on {@code __ROW_ID__}.
     */
    static void discriminateUnionBranchRowIds(CreateMaterializedViewStatement statement, List<QueryRelation> branches) {
        List<List<Expr>> discriminatedKeys = Lists.newArrayList();
        int encodeRowIdVersion = 0;
        for (int i = 0; i < branches.size(); i++) {
            List<Expr> keys = Lists.newArrayList();
            keys.add(new IntLiteral(i));
            keys.addAll(IvmRowIdDeriver.deriveRowIdKeys(branches.get(i)));
            discriminatedKeys.add(keys);
            encodeRowIdVersion = Math.max(encodeRowIdVersion, IvmOpUtils.deduceEncodeRowIdVersion(keys));
        }
        if (statement != null) {
            statement.setEncodeRowIdVersion(encodeRowIdVersion);
        }
        for (int i = 0; i < branches.size(); i++) {
            FunctionCallExpr rowIdFuncExpr = IvmOpUtils.buildRowIdFuncExpr(encodeRowIdVersion, discriminatedKeys.get(i));
            replaceRowIdColumn((SelectRelation) branches.get(i), rowIdFuncExpr);
        }
    }

    // Replace output column 0 -- the __ROW_ID__ IVMAnalyzer prepended for this branch -- with rowIdFuncExpr.
    private static void replaceRowIdColumn(SelectRelation selectRelation, FunctionCallExpr rowIdFuncExpr) {
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> items = selectList.getItems();
        List<SelectListItem> newItems = Lists.newArrayList();
        newItems.add(new SelectListItem(rowIdFuncExpr, IvmOpUtils.COLUMN_ROW_ID));
        for (int i = 1; i < items.size(); i++) {
            newItems.add(items.get(i));
        }
        selectList.setItems(newItems);

        List<Expr> outputs = selectRelation.getOutputExpression();
        List<Expr> newOutputs = Lists.newArrayList();
        newOutputs.add(rowIdFuncExpr);
        for (int i = 1; i < outputs.size(); i++) {
            newOutputs.add(outputs.get(i));
        }
        selectRelation.setOutputExpr(newOutputs);
    }
}
