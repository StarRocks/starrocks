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
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.ast.spm.DropBaselinePlanStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;

import java.time.format.DateTimeFormatter;
import java.util.List;

public class SPMStmtExecutor {
    public static void execute(ConnectContext context, CreateBaselinePlanStmt stmt) {
        SPMPlanBuilder builder = new SPMPlanBuilder(context, stmt);
        BaselinePlan plan = builder.execute();
        context.getGlobalStateMgr().getSqlPlanManager().storeBaselinePlan(plan);
    }

    public static void execute(ConnectContext context, DropBaselinePlanStmt stmt) {
        context.getGlobalStateMgr().getSqlPlanManager().dropBaselinePlan(stmt.getBaseLineId());
    }

    public static ShowResultSet execute(ConnectContext context, ShowBaselinePlanStmt stmt) {
        List<BaselinePlan> baselines = context.getGlobalStateMgr().getSqlPlanManager().getAllBaselines();
        List<List<String>> rows = Lists.newArrayList();
        for (BaselinePlan baseline : baselines) {
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(baseline.getId()));
            row.add(baseline.bindSqlDigest);
            row.add(String.valueOf(baseline.bindSqlHash));
            row.add(baseline.planSql);
            row.add(String.valueOf(baseline.costs));
            row.add(baseline.updateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            rows.add(row);
        }
        return new ShowResultSet(stmt.getMetaData(), rows);
    }
}
