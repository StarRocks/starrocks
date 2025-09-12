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

package com.starrocks.sql.spm;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.ast.spm.ControlBaselinePlanStmt;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.ast.spm.DropBaselinePlanStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Stream;

public class SPMStmtExecutor {
    public static BaselinePlan execute(ConnectContext context, CreateBaselinePlanStmt stmt) {
        SPMPlanBuilder builder = new SPMPlanBuilder(context, stmt);
        BaselinePlan plan = builder.execute();
        plan.setEnable(true);
        plan.setGlobal(stmt.isGlobal());
        plan.setSource(BaselinePlan.SOURCE_USER);
        if (stmt.isGlobal()) {
            context.getGlobalStateMgr().getSqlPlanStorage().storeBaselinePlan(List.of(plan));
        } else {
            context.getSqlPlanStorage().storeBaselinePlan(List.of(plan));
        }
        return plan;
    }

    public static void execute(ConnectContext context, DropBaselinePlanStmt stmt) {
        context.getSqlPlanStorage().dropBaselinePlan(stmt.getBaseLineId());
        context.getGlobalStateMgr().getSqlPlanStorage().dropBaselinePlan(stmt.getBaseLineId());
    }

    public static ShowResultSet execute(ConnectContext context, ShowBaselinePlanStmt stmt) {
        Expr where = null;
        if (stmt.getQuery() != null) {
            QueryStatement p = new QueryStatement(stmt.getQuery());
            try (PlannerMetaLocker locker = new PlannerMetaLocker(context, p)) {
                locker.lock();
                Analyzer.analyze(p, context);
            }
            SPMAst2SQLBuilder builder = new SPMAst2SQLBuilder(false, true);
            String digest = builder.build(p);
            long hash = builder.buildHash();
            SlotRef ref1 = new SlotRef(new TableName(), "bindsqldigest");
            ref1.setType(Type.VARCHAR);
            SlotRef ref2 = new SlotRef(new TableName(), "bindsqlhash");
            ref2.setType(Type.BIGINT);
            where = new CompoundPredicate(CompoundPredicate.Operator.AND,
                    new BinaryPredicate(BinaryType.EQ, ref1, new StringLiteral(digest)),
                    new BinaryPredicate(BinaryType.EQ, ref2, new IntLiteral(hash)));
        } else if (stmt.getWhere() != null) {
            where = stmt.getWhere();
        }

        List<BaselinePlan> baselines1 = context.getSqlPlanStorage().getBaselines(where);
        List<BaselinePlan> baselines2 = context.getGlobalStateMgr().getSqlPlanStorage().getBaselines(where);
        List<List<String>> rows = Lists.newArrayList();

        Stream.concat(baselines1.stream(), baselines2.stream()).forEach(baseline -> {
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(baseline.getId()));
            row.add(baseline.isGlobal() ? "Y" : "N");
            row.add(baseline.isEnable() ? "Y" : "N");
            row.add(baseline.getBindSqlDigest());
            row.add(String.valueOf(baseline.getBindSqlHash()));
            row.add(baseline.getBindSql());
            row.add(baseline.getPlanSql());
            row.add(String.valueOf(baseline.getCosts()));
            row.add(String.valueOf(baseline.getQueryMs()));
            row.add(baseline.getSource());
            row.add(baseline.getUpdateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            rows.add(row);
        });

        return new ShowResultSet(new ShowResultMetaFactory().getMetadata(stmt), rows);
    }

    public static void execute(ConnectContext context, ControlBaselinePlanStmt stmt) {
        context.getGlobalStateMgr().getSqlPlanStorage().controlBaselinePlan(stmt.isEnable(), stmt.getBaseLineId());
        context.getSqlPlanStorage().controlBaselinePlan(stmt.isEnable(), stmt.getBaseLineId());
    }
}
