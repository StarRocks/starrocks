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
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.spm.ControlBaselinePlanStmt;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.ast.spm.DropBaselinePlanStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Stream;

public class SPMStmtExecutor {
    public static void execute(ConnectContext context, CreateBaselinePlanStmt stmt) {
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
    }

    public static void execute(ConnectContext context, DropBaselinePlanStmt stmt) {
        context.getGlobalStateMgr().getSqlPlanStorage().dropBaselinePlan(stmt.getBaseLineId());
    }

    public static ShowResultSet execute(ConnectContext context, ShowBaselinePlanStmt stmt) {
        List<BaselinePlan> baselines1 = context.getSqlPlanStorage().getAllBaselines();
        List<BaselinePlan> baselines2 = context.getGlobalStateMgr().getSqlPlanStorage().getAllBaselines();
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
        return new ShowResultSet(stmt.getMetaData(), rows);
    }

    public static void execute(ConnectContext context, ControlBaselinePlanStmt stmt) {
        context.getGlobalStateMgr().getSqlPlanStorage().controlBaselinePlan(stmt.isEnable(), stmt.getBaseLineId());
        context.getSqlPlanStorage().controlBaselinePlan(stmt.isEnable(), stmt.getBaseLineId());
    }
}
