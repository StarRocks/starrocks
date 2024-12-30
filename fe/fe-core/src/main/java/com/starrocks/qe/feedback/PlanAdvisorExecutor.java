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

package com.starrocks.qe.feedback;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.feedback.AddPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ClearPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.DelPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ShowPlanAdvisorStmt;

import java.util.List;
import java.util.UUID;

public class PlanAdvisorExecutor {

    private static final PlanAdvisorExecutorVisitor INSTANCE = new PlanAdvisorExecutorVisitor();

    private static final ShowResultSetMetaData COLUMN_META =
            ShowResultSetMetaData.builder().addColumn(new Column("message", Type.STRING)).build();

    private static final ShowResultSetMetaData SHOW_RESULT_COLUMN_META =
            ShowResultSetMetaData.builder().addColumn(new Column("query_id", Type.STRING))
                    .addColumn(new Column("query", Type.STRING))
                    .addColumn(new Column("query_time", Type.STRING))
                    .addColumn(new Column("tuning_guides", Type.STRING))
                    .addColumn(new Column("avg_tuned_query_time", Type.STRING))
                    .addColumn(new Column("optimized_query_count", Type.STRING))
                    .addColumn(new Column("is_useful", Type.STRING))
                    .addColumn(new Column("fe_node", Type.STRING))
                    .build();


    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) {
        return INSTANCE.visit(stmt, context);
    }

    public static final class PlanAdvisorExecutorVisitor implements AstVisitor<ShowResultSet, ConnectContext> {

        @Override
        public ShowResultSet visitAddPlanAdvisorStatement(AddPlanAdvisorStmt stmt, ConnectContext context) {
            boolean enablePlanAnalyzer = context.getSessionVariable().isEnablePlanAnalyzer();
            try {
                if (!enablePlanAnalyzer) {
                    context.getSessionVariable().setEnablePlanAnalyzer(true);
                }
                StmtExecutor executor = StmtExecutor.newInternalExecutor(context, stmt.getQueryStmt());
                executor.execute();
                String result;
                if (context.getState().isError()) {
                    result = String.format("Failed to Add query into plan advisor in FE(%s). Reason: %s",
                            GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName(), context.getState().getErrorMessage());
                } else {
                    result = String.format("Add query into plan advisor in FE(%s) successfully.",
                            GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName());
                }
                return new ShowResultSet(COLUMN_META, List.of(List.of(result)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                context.getState().reset();
                context.getSessionVariable().setEnablePlanAnalyzer(enablePlanAnalyzer);
            }
        }

        @Override
        public ShowResultSet visitClearPlanAdvisorStatement(ClearPlanAdvisorStmt stmt, ConnectContext context) {
            long size = PlanTuningAdvisor.getInstance().getAdvisorSize();
            PlanTuningAdvisor.getInstance().clearAllAdvisor();
            String result = String.format("Clear all plan advisor in FE(%s) successfully. Advisor size: %d",
                    GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName(), size);
            return new ShowResultSet(COLUMN_META, List.of(List.of(result)));
        }

        @Override
        public ShowResultSet visitDelPlanAdvisorStatement(DelPlanAdvisorStmt stmt, ConnectContext context) {
            PlanTuningAdvisor.getInstance().deleteTuningGuides(UUID.fromString(stmt.getAdvisorId()));
            return null;
        }

        @Override
        public ShowResultSet visitShowPlanAdvisorStatement(ShowPlanAdvisorStmt stmt, ConnectContext context) {
            List<List<String>> resultRows = PlanTuningAdvisor.getInstance().getShowResult();
            return new ShowResultSet(SHOW_RESULT_COLUMN_META, resultRows);
        }
    }

}
