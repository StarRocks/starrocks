// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/mvrewrite/PercentileApproxToUnionRule.java

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
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.rewrite.ExprRewriteRule;

import java.util.List;

public class PercentileApproxToUnionRule implements ExprRewriteRule {
    public static final ExprRewriteRule INSTANCE = new PercentileApproxToUnionRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        // meet condition
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        if (!fnExpr.getFnName().getFunction().equalsIgnoreCase("percentile_approx")) {
            return expr;
        }

        SlotRef fnChild;
        if (!(fnExpr.getChild(0) instanceof SlotRef)) {
            if (fnExpr.getChild(0) instanceof CastExpr && fnExpr.getChild(0).getChild(0) instanceof SlotRef) {
                fnChild = (SlotRef) fnExpr.getChild(0).getChild(0);
            } else {
                return expr;
            }
        } else {
            fnChild = (SlotRef) fnExpr.getChild(0);
        }

        Column column = fnChild.getColumn();
        Table table = fnChild.getTable();
        if (column == null || table == null || !(table instanceof OlapTable)) {
            return expr;
        }

        OlapTable olapTable = (OlapTable) table;

        // check column
        String queryColumnName = column.getName();
        String mvColumnName = CreateMaterializedViewStmt
                .mvColumnBuilder(AggregateType.PERCENTILE_UNION.name().toLowerCase(), queryColumnName);
        Column mvColumn = olapTable.getVisibleColumn(mvColumnName);
        if (mvColumn == null) {
            return expr;
        }

        // rewrite expr
        //return rewriteExpr(fnChild, mvColumn, analyzer);

        Preconditions.checkNotNull(mvColumn);
        Preconditions.checkNotNull(fnChild);
        TableName tableName = fnChild.getTableName();
        Preconditions.checkNotNull(tableName);
        SlotRef mvSlotRef = new SlotRef(tableName, mvColumn.getName());
        List<Expr> newFnParams = Lists.newArrayList();
        newFnParams.add(mvSlotRef);
        FunctionCallExpr result = new FunctionCallExpr("percentile_union", newFnParams);

        FunctionCallExpr raw = new FunctionCallExpr("percentile_approx_raw",
                Lists.newArrayList(result, (FloatLiteral) fnExpr.getChild(1)));
        raw.analyzeNoThrow(analyzer);
        return raw;
    }
}
