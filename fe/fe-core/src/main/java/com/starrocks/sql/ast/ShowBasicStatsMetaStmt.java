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
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.StatisticUtils;

import java.time.format.DateTimeFormatter;
import java.util.List;

public class ShowBasicStatsMetaStmt extends EnhancedShowStmt {

    public ShowBasicStatsMetaStmt(Predicate predicate, List<OrderByElement> orderByElements,
                                  LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.predicate = predicate;
        this.limitElement = limitElement;
        this.orderByElements = orderByElements;
    }

    public static List<String> showBasicStatsMeta(ConnectContext context,
                                                  BasicStatsMeta basicStatsMeta) throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "", "ALL", "", "", "", "", "", "", "", "");
        long dbId = basicStatsMeta.getDbId();
        long tableId = basicStatsMeta.getTableId();
        List<String> columns = basicStatsMeta.getColumns();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + dbId);
        }
        row.set(0, db.getOriginName());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new MetaNotFoundException("No found table: " + tableId);
        }
        row.set(1, table.getName());

        // In new privilege framework(RBAC), user needs any action on the table to show analysis status for it.
        try {
            Authorizer.checkAnyActionOnTableLikeObject(context, db.getFullName(), table);
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
        row.set(7, basicStatsMeta.getColumnStatsString());
        row.set(8, basicStatsMeta.getTabletStatsReportTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        row.set(9, basicStatsMeta.getTableHealthyMetrics(table).toString());
        row.set(10, StatisticUtils.getTableLastUpdateTime(table).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        return row;
    }

    public static List<String> showExternalBasicStatsMeta(ConnectContext context,
                                                          ExternalBasicStatsMeta basicStatsMeta) throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "", "ALL", "", "", "", "", "", "", "", "", "");
        String catalogName = basicStatsMeta.getCatalogName();
        String dbName = basicStatsMeta.getDbName();
        String tableName = basicStatsMeta.getTableName();

        List<String> columns = basicStatsMeta.getColumns();

        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, dbName);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + catalogName + "." + dbName);
        }
        row.set(0, catalogName + "." + dbName);
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, catalogName, dbName, tableName);
        if (table == null) {
            throw new MetaNotFoundException("No found table: " + catalogName + "." + dbName + "." + tableName);
        }
        row.set(1, tableName);

        // In new privilege framework(RBAC), user needs any action on the table to show analysis status for it.
        try {
            Authorizer.checkAnyActionOnTable(context, new TableName(catalogName, db.getOriginName(), table.getName()));
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
        row.set(7, basicStatsMeta.getColumnStatsString());

        return row;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowBasicStatsMetaStatement(this, context);
    }
}

