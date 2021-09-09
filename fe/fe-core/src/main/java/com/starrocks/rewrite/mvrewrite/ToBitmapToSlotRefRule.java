// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/mvrewrite/ToBitmapToSlotRefRule.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.rewrite.mvrewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.rewrite.ExprRewriteRule;

import java.util.List;

/**
 * Rewrite bitmap_union(to_bitmap(c1)) to bitmap_union(mv_bitmap_c1)
 * Rewrite bitmap_union_count(to_bitmap(c1)) to bitmap_union_count(mv_bitmap_c1)
 */
public class ToBitmapToSlotRefRule implements ExprRewriteRule {

    public static final ExprRewriteRule INSTANCE = new ToBitmapToSlotRefRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        SlotRef queryColumnSlotRef;
        Column mvColumn;

        // meet the condition
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                && !fnNameString.equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)) {
            return expr;
        }
        if (!(fnExpr.getChild(0) instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr child0FnExpr = (FunctionCallExpr) fnExpr.getChild(0);
        if (!child0FnExpr.getFnName().getFunction().equalsIgnoreCase("to_bitmap")) {
            return expr;
        }
        if (child0FnExpr.getChild(0) instanceof SlotRef) {
            queryColumnSlotRef = (SlotRef) child0FnExpr.getChild(0);
        } else if (child0FnExpr.getChild(0) instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) child0FnExpr.getChild(0);
            if (!(castExpr.getChild(0) instanceof SlotRef)) {
                return expr;
            }
            queryColumnSlotRef = (SlotRef) castExpr.getChild(0);
        } else {
            return expr;
        }
        Column column = queryColumnSlotRef.getColumn();
        Table table = queryColumnSlotRef.getTable();
        if (column == null || table == null || !(table instanceof OlapTable)) {
            return expr;
        }
        OlapTable olapTable = (OlapTable) table;

        // check column
        String queryColumnName = column.getName();
        String mvColumnName = CreateMaterializedViewStmt
                .mvColumnBuilder(AggregateType.BITMAP_UNION.name().toLowerCase(), queryColumnName);
        mvColumn = olapTable.getVisibleColumn(mvColumnName);
        if (mvColumn == null) {
            return expr;
        }

        // equal expr
        return rewriteExpr(fnNameString, queryColumnSlotRef, mvColumn, analyzer);
    }

    private Expr rewriteExpr(String fnName, SlotRef queryColumnSlotRef, Column mvColumn, Analyzer analyzer) {
        Preconditions.checkNotNull(mvColumn);
        Preconditions.checkNotNull(queryColumnSlotRef);
        TableName tableName = queryColumnSlotRef.getTableName();
        Preconditions.checkNotNull(tableName);
        SlotRef mvSlotRef = new SlotRef(tableName, mvColumn.getName());
        List<Expr> newFnParams = Lists.newArrayList();
        newFnParams.add(mvSlotRef);
        FunctionCallExpr result = new FunctionCallExpr(fnName, newFnParams);
        result.analyzeNoThrow(analyzer);
        return result;
    }
}
