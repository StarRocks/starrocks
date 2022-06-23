// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.execution;

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.AlterRoutineLoadStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.CancelLoadStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DdlExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.AlterMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;

public class DataDefinitionExecutorFactory {
    private static final ImmutableMap<Class<? extends StatementBase>, DataDefinitionExecutor> executorMap =
            new ImmutableMap.Builder<Class<? extends StatementBase>, DataDefinitionExecutor>()
                    .put(CreateDbStmt.class, new CreateDbExecutor())
                    .put(DropDbStmt.class, new DropDbExecutor())
                    .put(CreateFunctionStmt.class, new CreateFunctionExecutor())
                    .put(DropFunctionStmt.class, new DropFunctionExecutor())
                    .put(CreateTableStmt.class, new CreateTableExecutor())
                    .put(CreateTableLikeStmt.class, new CreateTableLikeExecutor())
                    .put(DropTableStmt.class, new DropTableExecutor())
                    .put(CreateMaterializedViewStmt.class, new CreateMaterializedViewExecutor())
                    .put(CreateMaterializedViewStatement.class, new CreateMaterializedViewStatementExecutor())
                    .put(DropMaterializedViewStmt.class, new DropMaterializedViewExecutor())
                    .put(AlterMaterializedViewStatement.class, new AlterMaterializedViewExecutor())
                    .put(AlterTableStmt.class, new AlterTableExecutor())
                    .put(AlterViewStmt.class, new AlterViewExecutor())
                    .put(CancelAlterTableStmt.class, new CancelAlterTableExecutor())
                    .put(LoadStmt.class, new LoadExecutor())
                    .put(CancelLoadStmt.class, new CancelLoadExecutor())
                    .put(CreateRoutineLoadStmt.class, new CreateRoutineLoadExecutor())
                    .put(PauseRoutineLoadStmt.class, new PauseRoutineLoadExecutor())
                    .put(ResumeRoutineLoadStmt.class, new ResumeRoutineLoadExecutor())
                    .put(StopRoutineLoadStmt.class, new StopRoutineLoadExecutor())
                    .put(AlterRoutineLoadStmt.class, new AlterRoutineLoadExecutor())
                    .build();

    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        DataDefinitionExecutor executor = executorMap.get(stmt.getClass());
        if (executor != null) {
            return executor.execute(stmt, context);
        } else {
            return DdlExecutor.execute(context.getGlobalStateMgr(), (DdlStmt) stmt);
        }
    }
}
