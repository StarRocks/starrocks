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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.StatisticUtils;

import java.time.format.DateTimeFormatter;
import java.util.List;

public class ShowBasicStatsMetaStmt extends ShowStmt {

    public ShowBasicStatsMetaStmt(Predicate predicate) {
        this(predicate, NodePosition.ZERO);
    }

    public ShowBasicStatsMetaStmt(Predicate predicate, NodePosition pos) {
        super(pos);
        this.predicate = predicate;
    }

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Columns", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("UpdateTime", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Healthy", ScalarType.createVarchar(5)))
                    .build();

    public static List<String> showBasicStatsMeta(ConnectContext context,
                                                  BasicStatsMeta basicStatsMeta) throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "", "ALL", "", "", "", "");
        long dbId = basicStatsMeta.getDbId();
        long tableId = basicStatsMeta.getTableId();
        List<String> columns = basicStatsMeta.getColumns();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + dbId);
        }
        row.set(0, db.getOriginName());
        Table table = db.getTable(tableId);
        if (table == null) {
            throw new MetaNotFoundException("No found table: " + tableId);
        }
        row.set(1, table.getName());

        // In new privilege framework(RBAC), user needs any action on the table to show analysis status for it.
        try {
            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), new TableName(db.getOriginName(), table.getName()));
        } catch (AccessDeniedException e) {
            return null;
        }

        long totalCollectColumnsSize = StatisticUtils.getCollectibleColumns(table).size();
        if (null != columns && !columns.isEmpty() && (columns.size() != totalCollectColumnsSize)) {
            row.set(2, String.join(",", columns));
        }

        row.set(3, basicStatsMeta.getType().name());
        row.set(4, basicStatsMeta.getUpdateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        row.set(5, basicStatsMeta.getProperties() == null ? "{}" : basicStatsMeta.getProperties().toString());
        row.set(6, (int) (basicStatsMeta.getHealthy() * 100) + "%");

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
        return visitor.visitShowBasicStatsMetaStatement(this, context);
    }
}

