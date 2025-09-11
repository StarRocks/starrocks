// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.scheduler;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

/**
 * Verify that submit task based SQL records client IP into audit log via SqlTaskRunProcessor
 * and that TaskRunContext propagates the remote host:port from the submitter.
 */
public class SqlTaskRunProcessorTest {

    @Test
    public void testAuditClientIpSetForSubmitTaskInsert(@Mocked StatementBase mockStmt,
                                                        @Mocked com.starrocks.mysql.MysqlChannel mockChannel) throws Exception {
        // Prepare a parent session simulating the submitter connection
        ConnectContext parentCtx = new ConnectContext(null);

        // Mock parentCtx.getMysqlChannel() to return a channel with a concrete remote host:port
        final String expectedRemoteHostPort = "10.1.2.3:7777";
        new Expectations(parentCtx) {{
            parentCtx.getMysqlChannel(); result = mockChannel; minTimes = 0;
            mockChannel.getRemoteHostPortString(); result = expectedRemoteHostPort; minTimes = 0;
            mockChannel.getRemoteIp(); result = "10.1.2.3"; minTimes = 0;
        }};

        // Build a minimal Task and TaskRun
        Task task = new Task("task_insert_select");
        task.setCatalogName("internal");
        task.setDbName("db1");
        task.setDefinition("INSERT INTO t SELECT 1");

        TaskRun taskRun = new TaskRun();
        taskRun.setTask(task);
        taskRun.setConnectContext(parentCtx);
        // Initialize properties map to avoid NPE inside buildTaskRunContext
        taskRun.setProperties(new java.util.HashMap<>());
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());

        // Build the TaskRunContext and ensure the remote host:port is propagated
        TaskRunContext trc = taskRun.buildTaskRunContext();
        Assertions.assertEquals(expectedRemoteHostPort, trc.getRemoteIp());

        // Mock parsing and execution to avoid heavy dependencies using MockUp
        new MockUp<SqlParser>() {
            @Mock
            public java.util.List<StatementBase> parse(String sql, com.starrocks.qe.SessionVariable sv) {
                return Collections.singletonList(mockStmt);
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public static StmtExecutor newInternalExecutor(ConnectContext c, StatementBase s) {
                // Return a lightweight real instance; below we mock its heavy methods to no-op
                return new StmtExecutor(c, s);
            }
            @Mock
            public void addRunningQueryDetail(StatementBase s) { }
            @Mock
            public void execute() { }
            @Mock
            public StatementBase getParsedStmt() { return null; }
            @Mock
            public com.starrocks.proto.PQueryStatistics getQueryStatisticsForAuditLog() { return null; }
        };

        // Execute the processor; it should set clientIp from TaskRunContext.remoteIp
        SqlTaskRunProcessor processor = new SqlTaskRunProcessor();
        processor.processTaskRun(trc);

        String clientIpInAudit = trc.getCtx().getAuditEventBuilder().build().clientIp;
        Assertions.assertEquals(expectedRemoteHostPort, clientIpInAudit);
    }
}
