// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.StatementBase;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatsConstants;

import java.time.LocalDateTime;

public class CreateAnalyzeJobExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws DdlException {
        CreateAnalyzeJobStmt createAnalyzeJobStmt = (CreateAnalyzeJobStmt) stmt;
        AnalyzeJob analyzeJob = new AnalyzeJob(createAnalyzeJobStmt.getDbId(),
                createAnalyzeJobStmt.getTableId(),
                createAnalyzeJobStmt.getColumnNames(),
                createAnalyzeJobStmt.isSample() ? StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE,
                createAnalyzeJobStmt.getProperties(), StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        context.getGlobalStateMgr().getAnalyzeManager().addAnalyzeJob(analyzeJob);

        Thread thread = new Thread(() -> {
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            analyzeJob.run(statisticExecutor);
        });
        thread.start();

        return null;
    }
}

