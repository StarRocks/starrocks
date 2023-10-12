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

package com.starrocks.privilege.ranger;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SecurityPolicyRewriteRule {
    public static QueryStatement buildView(ConnectContext context, Relation relation, TableName tableName) {
        if (!Config.access_control.equals("ranger") &&
                (tableName.getCatalog() == null
                        || tableName.getCatalog().equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))) {
            return null;
        }

        if (relation instanceof TableRelation && ((TableRelation) relation).isSyncMVQuery()) {
            return null;
        }

        List<Column> columns;
        if (relation instanceof ViewRelation) {
            ViewRelation viewRelation = (ViewRelation) relation;
            columns = viewRelation.getView().getBaseSchema();
        } else if (relation instanceof TableRelation) {
            TableRelation tableRelation = (TableRelation) relation;
            columns = tableRelation.getTable().getBaseSchema();
        } else {
            return null;
        }

        boolean hasPolicy = false;
        List<SelectListItem> selectListItemList = new ArrayList<>();
        Map<TableName, Relation> allTablesRelations;
        for (Column column : columns) {
            if (column.getType().isUnknown()) {
                continue;
            }
            Expr maskingExpr = Authorizer.getColumnMaskingPolicy(
                    context, tableName, column.getName(), column.getType());

            if (maskingExpr != null) {
                hasPolicy = true;
                selectListItemList.add(new SelectListItem(maskingExpr, column.getName(), NodePosition.ZERO));
                allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(maskingExpr);
                allTablesRelations.values().forEach(r -> r.setCreateByPolicyRewritten(true));
            } else {
                selectListItemList.add(new SelectListItem(new SlotRef(tableName, column.getName()), column.getName(),
                        NodePosition.ZERO));
            }
        }

        Expr rowAccessExpr = Authorizer.getRowAccessPolicy(context, tableName);
        if (rowAccessExpr != null) {
            hasPolicy = true;
            allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(rowAccessExpr);
            allTablesRelations.values().forEach(r -> r.setCreateByPolicyRewritten(true));
        }

        if (!hasPolicy) {
            return null;
        }

        SelectRelation selectRelation = new SelectRelation(
                new SelectList(selectListItemList, false),
                relation,
                rowAccessExpr,
                null,
                null);
        selectRelation.setOrderBy(Collections.emptyList());
        return new QueryStatement(selectRelation);
    }
}
