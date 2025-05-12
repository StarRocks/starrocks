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

package com.starrocks.summary;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.Explain;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.spm.SPMAst2SQLBuilder;

import java.time.LocalDateTime;
import java.util.Map;

public class QueryHistory {
    private String db;
    private String originSQL;
    private final StatementBase statement;
    private final ExecPlan plan;
    private double queryMs;
    private LocalDateTime datetime;
    private final Map<String, String> other = Map.of();

    private double costs;
    private String sqlDigest;

    QueryHistory() {
        statement = null;
        plan = null;
    }

    QueryHistory(ConnectContext ctx, ExecPlan plan) {
        originSQL = ctx.getExecutor().getOriginStmtInString();
        statement = ctx.getExecutor().getParsedStmt();
        datetime = LocalDateTime.now();
        queryMs = System.currentTimeMillis() - ctx.getStartTime();

        this.db = ctx.getCurrentCatalog() + "." + ctx.getDatabase();
        this.plan = plan;
        this.costs = plan.getPhysicalPlan().getCost();
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setOriginSQL(String originSQL) {
        this.originSQL = originSQL;
    }

    public void setQueryMs(double queryMs) {
        this.queryMs = queryMs;
    }

    public void setDatetime(LocalDateTime datetime) {
        this.datetime = datetime;
    }

    public void setCosts(double costs) {
        this.costs = costs;
    }

    public void setSqlDigest(String sqlDigest) {
        this.sqlDigest = sqlDigest;
    }

    public String getOriginSQL() {
        return originSQL;
    }

    public String getSqlDigest() {
        return sqlDigest;
    }

    public double getCosts() {
        return costs;
    }

    public double getQueryMs() {
        return queryMs;
    }

    public LocalDateTime getDatetime() {
        return datetime;
    }

    String toJSON() {
        Map<String, Object> jsonMaps = Maps.newHashMap();
        jsonMaps.put("dt", DateUtils.formatDateTimeUnix(datetime));
        jsonMaps.put("frontend", GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName() + ":" +
                GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getQueryPort());
        jsonMaps.put("db", db);
        jsonMaps.put("sql", originSQL);
        jsonMaps.put("plan_costs", costs);
        jsonMaps.put("query_ms", queryMs);
        jsonMaps.put("other", other);

        SPMAst2SQLBuilder builder = new SPMAst2SQLBuilder(false, true);
        sqlDigest = builder.build((QueryStatement) statement);

        Explain explain = new Explain(true, true, "\t", " |");
        String planStr = explain.print(plan.getPhysicalPlan(), plan.getOutputColumns());
        jsonMaps.put("sql_digest", sqlDigest);
        jsonMaps.put("plan", planStr);

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jsonMaps);
    }
}
