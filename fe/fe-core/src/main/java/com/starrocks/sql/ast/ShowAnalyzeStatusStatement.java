// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.AnalyzeStatus;

import java.time.format.DateTimeFormatter;
import java.util.List;

public class ShowAnalyzeStatusStatement extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Schedule", ScalarType.createVarchar(20)))
                    .addColumn(new Column("updateTime", ScalarType.createVarchar(60)))
                    .build();

    public static List<String> showAnalyzeJobs(AnalyzeStatus analyzeJob) throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "ALL", "ALL", "ALL", "", "", "", "", "", "");
        long dbId = analyzeJob.getDbId();
        long tableId = analyzeJob.getTableId();

        row.set(0, String.valueOf(analyzeJob.getId()));
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        row.set(1, db.getFullName());
        Table table = db.getTable(tableId);
        row.set(2, table.getName());
        row.set(3, analyzeJob.getType().name());
        row.set(4, analyzeJob.getScheduleType().name());
        row.set(5, analyzeJob.getWorkTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        return row;
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
