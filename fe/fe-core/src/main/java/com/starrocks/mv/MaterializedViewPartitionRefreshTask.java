// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.starrocks.analysis.StatementBase;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MaterializedViewPartitionRefreshTask extends MaterializedViewRefreshTask {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewPartitionRefreshTask.class);
    private String refreshSQL;

    public void setRefreshSQL(String refreshSQL) {
        this.refreshSQL = refreshSQL;
    }

    @Override
    public void runTask() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setQueryId(UUIDUtil.genUUID());
        executeSQL(refreshSQL, ctx);
    }

    @Override
    public IMaterializedViewRefreshTask cloneTask() {
        MaterializedViewPartitionRefreshTask task = new MaterializedViewPartitionRefreshTask();
        task.setRefreshSQL(refreshSQL);
        return task;
    }

    private void executeSQL(String insertOverrideSQL, ConnectContext ctx) throws Exception {
        StatementBase insertOverrideStmt = com.starrocks.sql.parser.SqlParser.parse(insertOverrideSQL,
                ctx.getSessionVariable().getSqlMode()).get(0);
        ctx.getState().reset();
        StmtExecutor executor = new StmtExecutor(ctx, insertOverrideStmt);
        ctx.setExecutor(executor);
        executor.execute();
    }


    @Override
    public String toString() {
        return "MaterializedViewPartitionRefreshTask{" +
                "refreshSQL='" + refreshSQL + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", status=" + status +
                ", errMsg='" + errMsg + '\'' +
                '}';
    }
}
