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


package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.AnalyzeJob;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ShowAnalyzeJobStmt extends ShowStmt {

    public ShowAnalyzeJobStmt(Predicate predicate) {
        this(predicate, NodePosition.ZERO);
    }

    public ShowAnalyzeJobStmt(Predicate predicate, NodePosition pos) {
        super(pos);
        this.predicate = predicate;
    }

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Catalog", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Columns", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Schedule", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastWorkTime", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Reason", ScalarType.createVarchar(100)))
                    .build();

    public static List<String> showAnalyzeJobs(ConnectContext context,
                                               AnalyzeJob analyzeJob) throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", analyzeJob.getCatalogName(), "ALL", "ALL",
                "ALL", "", "", "", "", "", "");
        List<String> columns = analyzeJob.getColumns();
        row.set(0, String.valueOf(analyzeJob.getId()));

        if (!analyzeJob.isAnalyzeAllDb()) {
            String dbName = analyzeJob.getDbName();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(analyzeJob.getCatalogName(), dbName);

            if (db == null) {
                throw new MetaNotFoundException("No found database: " + dbName);
            }

            row.set(2, db.getOriginName());

            if (!analyzeJob.isAnalyzeAllTable()) {
                String tableName = analyzeJob.getTableName();
                Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(analyzeJob.getCatalogName(),
                        dbName, tableName);

                if (table == null) {
                    throw new MetaNotFoundException("No found table: " + tableName);
                }

                row.set(3, table.getName());

                // In new privilege framework(RBAC), user needs any action on the table to show analysis job on it,
                // for jobs on entire instance or entire db, we just show it directly because there isn't a specified
                // table to check privilege on.
                try {
                    Authorizer.checkAnyActionOnTableLikeObject(context, db.getFullName(), table);
                } catch (AccessDeniedException e) {
                    return null;
                }

                if (null != columns && !columns.isEmpty()
                        && (columns.size() != table.getBaseSchema().size())) {
                    String str = String.join(",", columns);
                    if (str.length() > 100) {
                        row.set(4, str.substring(0, 100) + "...");
                    } else {
                        row.set(4, str);
                    }
                }
            }
        }

        row.set(5, analyzeJob.getAnalyzeType().name());
        row.set(6, analyzeJob.getScheduleType().name());
        row.set(7, analyzeJob.getProperties() == null ? "{}" : analyzeJob.getProperties().toString());
        row.set(8, analyzeJob.getStatus().name());
        if (LocalDateTime.MIN.equals(analyzeJob.getWorkTime())) {
            row.set(9, "None");
        } else {
            row.set(9, analyzeJob.getWorkTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }

        if (null != analyzeJob.getReason()) {
            row.set(10, analyzeJob.getReason());
        }

        return row;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowAnalyzeJobStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
