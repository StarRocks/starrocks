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
import com.starrocks.catalog.BasicTable;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.EnhancedShowStmt;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.format.DateTimeFormatter;
import java.util.List;

public class ShowAnalyzeStatusStmt extends EnhancedShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowAnalyzeStatusStmt.class);

    public ShowAnalyzeStatusStmt(Predicate predicate, List<OrderByElement> orderByElements,
                                 LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.predicate = predicate;
        this.limitElement = limitElement;
        this.orderByElements = orderByElements;
    }

    public static List<String> showAnalyzeStatus(ConnectContext context,
                                                 AnalyzeStatus analyzeStatus) throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "", "", "ALL", "", "", "", "", "", "", "");
        List<String> columns = analyzeStatus.getColumns();

        row.set(0, String.valueOf(analyzeStatus.getId()));
        row.set(1, analyzeStatus.getCatalogName() + "." + analyzeStatus.getDbName());
        row.set(2, analyzeStatus.getTableName());

        BasicTable table = GlobalStateMgr.getCurrentState().getMetadataMgr().getBasicTable(
                context, analyzeStatus.getCatalogName(), analyzeStatus.getDbName(), analyzeStatus.getTableName());

        if (table == null) {
            throw new MetaNotFoundException("Table " + analyzeStatus.getDbName() + "."
                    + analyzeStatus.getTableName() + " not found");
        }

        // In new privilege framework(RBAC), user needs any action on the table to show analysis status on it
        try {
            Authorizer.checkAnyActionOnTableLikeObject(context, analyzeStatus.getDbName(), table);
        } catch (AccessDeniedException e) {
            return null;
        }

        if (null != columns && !columns.isEmpty()) {
            String str = String.join(",", columns);
            row.set(3, str);
        }

        row.set(4, analyzeStatus.getType().name());
        row.set(5, analyzeStatus.getScheduleType().name());
        if (analyzeStatus.getStatus().equals(StatsConstants.ScheduleStatus.FINISH)) {
            row.set(6, "SUCCESS");
        } else {
            String status = analyzeStatus.getStatus().name();
            if (analyzeStatus.getStatus().equals(StatsConstants.ScheduleStatus.RUNNING)) {
                status += " (" + analyzeStatus.getProgress() + "%" + ")";
            }
            row.set(6, status);
        }

        row.set(7, analyzeStatus.getStartTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        if (analyzeStatus.getEndTime() != null) {
            row.set(8, analyzeStatus.getEndTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }

        row.set(9, analyzeStatus.getProperties() == null ? "{}" : analyzeStatus.getProperties().toString());

        if (analyzeStatus.getReason() != null) {
            row.set(10, analyzeStatus.getReason());
        }

        return row;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowAnalyzeStatusStatement(this, context);
    }
}
