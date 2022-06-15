// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.AnalyzeMeta;

import java.time.format.DateTimeFormatter;
import java.util.List;

public class
ShowAnalyzeMetaStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("UpdateTime", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Healthy", ScalarType.createVarchar(5)))
                    .build();

    public static List<String> showAnalyzeMeta(AnalyzeMeta analyzeMeta) throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "", "", "", "");
        long dbId = analyzeMeta.getDbId();
        long tableId = analyzeMeta.getTableId();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + dbId);
        }
        row.set(0, ClusterNamespace.getNameFromFullName(db.getFullName()));
        Table table = db.getTable(tableId);
        if (table == null) {
            throw new MetaNotFoundException("No found table: " + tableId);
        }
        row.set(1, table.getName());
        row.set(2, analyzeMeta.getType().name());
        row.set(3, analyzeMeta.getUpdateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        row.set(4, (int) (analyzeMeta.getHealthy() * 100) + "%");

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

