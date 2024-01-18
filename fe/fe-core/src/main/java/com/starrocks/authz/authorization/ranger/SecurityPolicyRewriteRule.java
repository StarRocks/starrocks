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

package com.starrocks.authz.authorization.ranger;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
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
import java.util.stream.Collectors;

public class SecurityPolicyRewriteRule {
    public static QueryStatement buildView(ConnectContext context, Relation relation, TableName tableName) {
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

        List<Column> validColumns = columns.stream().filter(c -> !c.getType().isUnknown()).collect(Collectors.toList());
        Map<String, Expr> maskingExprMap = Authorizer.getColumnMaskingPolicy(context, tableName, validColumns);
        Expr rowAccessExpr = Authorizer.getRowAccessPolicy(context, tableName);
        if ((maskingExprMap == null || maskingExprMap.isEmpty()) && rowAccessExpr == null) {
            return null;
        }

        List<SelectListItem> selectListItemList = new ArrayList<>();
        for (Column column : validColumns) {
            String columnName = column.getName();
            if (maskingExprMap != null && maskingExprMap.containsKey(columnName)) {
                Expr maskingExpr = maskingExprMap.get(columnName);
                selectListItemList.add(new SelectListItem(maskingExpr, columnName, NodePosition.ZERO));
                Map<TableName, Relation> allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(maskingExpr);
                allTablesRelations.values().forEach(r -> r.setCreateByPolicyRewritten(true));
            } else {
                selectListItemList.add(new SelectListItem(new SlotRef(tableName, columnName), columnName, NodePosition.ZERO));
            }
        }

        if (rowAccessExpr != null) {
            Map<TableName, Relation> allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(rowAccessExpr);
            allTablesRelations.values().forEach(r -> r.setCreateByPolicyRewritten(true));
        }

        SelectRelation selectRelation = new SelectRelation(new SelectList(selectListItemList, false),
                relation, rowAccessExpr, null, null);
        selectRelation.setOrderBy(Collections.emptyList());
        return new QueryStatement(selectRelation);
    }
}
