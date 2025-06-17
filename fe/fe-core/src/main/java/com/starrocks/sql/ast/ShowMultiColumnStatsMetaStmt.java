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
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
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
import com.starrocks.statistic.MultiColumnStatsMeta;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class ShowMultiColumnStatsMetaStmt extends ShowStmt {
    public ShowMultiColumnStatsMetaStmt(Predicate predicate, List<OrderByElement> orderByElements,
                                      LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.predicate = predicate;
        this.limitElement = limitElement;
        this.orderByElements = orderByElements;
    }

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Columns", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("StatisticsTypes", ScalarType.createVarchar(200)))
                    .addColumn(new Column("UpdateTime", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                    .build();

    public static List<String> showMultiColumnStatsMeta(ConnectContext context, MultiColumnStatsMeta meta)
            throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "", "", "", "", "", "");
        long dbId = meta.getDbId();
        long tableId = meta.getTableId();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + dbId);
        }
        row.set(0, db.getOriginName());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new MetaNotFoundException("No found table: " + tableId);
        }
        // In new privilege framework(RBAC), user needs any action on the table to show analysis status for it.
        try {
            Authorizer.checkAnyActionOnTableLikeObject(context, db.getFullName(), table);
        } catch (AccessDeniedException e) {
            return null;
        }

        row.set(1, table.getName());
        row.set(2, meta.getColumnIds().stream().map(id -> table.getColumnByUniqueId(id).getName()).toList().toString());
        row.set(3, meta.getAnalyzeType().name());
        row.set(4, meta.getStatsTypes().stream().map(Enum::name).collect(Collectors.joining(", ")));
        row.set(5, meta.getUpdateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        row.set(6, meta.getProperties() == null ? "{}" : meta.getProperties().toString());

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

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowMultiColumnsStatsMetaStatement(this, context);
    }
}
