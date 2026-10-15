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

package com.starrocks.qe;

import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.plan.ExecPlan;

public class PrepareStmtContext {
    private final PrepareStmt stmt;
    private final ConnectContext connectContext;
    private ExecPlan execPlan;
    private boolean isCached = false;
    private long lastSchemaUpdateTime = -1;
    private long tableId = -1;

    public PrepareStmtContext(PrepareStmt stmt, ConnectContext connectContext, ExecPlan execPlan) {
        this.stmt = stmt;
        this.connectContext = connectContext;
        this.execPlan = execPlan;
    }

    public PrepareStmt getStmt() {
        return stmt;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setExecPlan(ExecPlan execPlan) {
        this.execPlan = execPlan;
    }

    public ExecPlan getExecPlan() {
        return execPlan;
    }

    public boolean isCached() {
        return isCached;
    }

<<<<<<< HEAD
    public void updateLastSchemaUpdateTime(QueryStatement stmt, ConnectContext session) {
        SelectRelation selectRelation = (SelectRelation) (stmt.getQueryRelation());
        TableRelation tableRelation = (TableRelation) selectRelation.getRelation();
        QueryAnalyzer queryAnalyzer = new QueryAnalyzer(session);
        OlapTable table = (OlapTable) queryAnalyzer.resolveTable(tableRelation);
        this.lastSchemaUpdateTime = table.lastSchemaUpdateTime.get();
=======
    // the schema time of the table the statement names, empty when the name does not lead to an olap table; read
    // before planning, so a schema change that lands while the statement is planned replans the next EXECUTE
    public OptionalLong schemaUpdateTimeBeforePlanning(QueryStatement stmt, ConnectContext session) {
        try {
            OlapTable table = resolveOlapTable(stmt, session);
            return table == null ? OptionalLong.empty() : OptionalLong.of(table.lastSchemaUpdateTime.get());
        } catch (RuntimeException e) {
            // an unknown table: planning raises it from the path that can name the statement
            return OptionalLong.empty();
        }
    }

    // false when the name no longer leads to the table the plan scans, since a hit validates the plan by that name;
    // false too when the plan scans the live table itself rather than a private copy of it -- planning with the meta
    // lock held for the whole phase (cbo_use_lock_db, or a copy-unsafe table) does -- since a hit prunes partitions
    // and tablets on the table the plan scans without any lock, which on the live one races a concurrent INSERT
    // OVERWRITE or partition change
    public boolean recordPlannedTable(QueryStatement stmt, ConnectContext session, OlapTable plannedTable,
                                      OptionalLong schemaUpdateTimeBeforePlanning) {
        OlapTable table = resolveOlapTable(stmt, session);
        if (table == null || plannedTable == null || table.getId() != plannedTable.getId() || plannedTable == table
                || schemaUpdateTimeBeforePlanning.isEmpty()) {
            return false;
        }
        this.lastSchemaUpdateTime = schemaUpdateTimeBeforePlanning.getAsLong();
>>>>>>> 3d28e7f5daf... [BugFix][POST-1966] Close the remaining planner and MV metadata lock gaps (#65384)
        this.tableId = table.getId();
    }

    public void cachePlan(ExecPlan execPlan) {
        this.execPlan = execPlan;
        this.isCached = true;
    }

    public boolean needReAnalyze(QueryStatement stmt, ConnectContext session) {
        SelectRelation selectRelation = (SelectRelation) (stmt.getQueryRelation());
        TableRelation tableRelation = (TableRelation) selectRelation.getRelation();
        QueryAnalyzer queryAnalyzer = new QueryAnalyzer(session);
        OlapTable table = (OlapTable) queryAnalyzer.resolveTable(tableRelation);
        long lastSchemaUpdateTime = table.lastSchemaUpdateTime.get();
        long tableId = table.getId();
        if (lastSchemaUpdateTime > this.lastSchemaUpdateTime) {
            return true;
        }
        if (tableId != this.tableId) {
            return true;
        }
        return false;
    }

    public void reset() {
        this.isCached = false;
        this.lastSchemaUpdateTime = -1;
        this.tableId = -1;
        this.execPlan = null;
    }
}
