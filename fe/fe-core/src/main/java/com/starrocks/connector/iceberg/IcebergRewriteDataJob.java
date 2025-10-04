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

package com.starrocks.connector.iceberg;

import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergRewriteData;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.IcebergRewriteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.iceberg.util.ThreadPools.newWorkerPool;

public class IcebergRewriteDataJob {
    protected static final Logger LOG = LogManager.getLogger(IcebergRewriteDataJob.class);
    private final String insertSql;
    private final boolean rewriteAll;
    private final long minFileSizeBytes;
    private final long batchSize;
    private final ConnectContext context;
    private final AlterTableStmt originAlterStmt;
    private IcebergRewriteStmt rewriteStmt;
    private ExecPlan execPlan;
    private List<IcebergScanNode> scanNodes;
    private IcebergRewriteData rewriteData;
    private long batchParallelism = 1;
    private StatementBase parsedStmt;

    public IcebergRewriteDataJob(String insertSql,
                                 boolean rewriteAll,
                                 long minFileSizeBytes,
                                 long batchSize,
                                 long batchParallelism,
                                 ConnectContext context,
                                 AlterTableStmt stmt) {
        this.insertSql = insertSql;
        this.rewriteAll = rewriteAll;
        this.minFileSizeBytes = minFileSizeBytes;
        this.batchSize = batchSize;
        this.batchParallelism = batchParallelism;
        this.context = context;
        this.originAlterStmt = stmt;
    }

    public void prepare() throws Exception {
        context.getSessionVariable().setQueryTimeoutS(259200);
        this.parsedStmt = com.starrocks.sql.parser.SqlParser
                .parse(insertSql, context.getSessionVariable())
                .get(0);

        this.rewriteStmt = new IcebergRewriteStmt((InsertStmt) parsedStmt, rewriteAll);
        this.execPlan = StatementPlanner.plan(parsedStmt, context);
        this.scanNodes = execPlan.getFragments().stream()
                .flatMap(fragment -> fragment.collectScanNodes().values().stream())
                .filter(scan -> scan instanceof IcebergScanNode && "IcebergScanNode".equals(scan.getPlanNodeName()))
                .map(scan -> (IcebergScanNode) scan)
                .collect(Collectors.toList());

        IcebergScanNode targetNode = scanNodes.stream().findFirst().orElse(null);
        if (targetNode == null) {
            LOG.info("No IcebergScanNode of table " + ((InsertStmt) parsedStmt).getTableName() + 
                        " found for rewrite, prepare becomes no-op.");
            return;
        }

        this.rewriteData = new IcebergRewriteData();
        this.rewriteData.setSource(targetNode.getSourceRange());
        this.rewriteData.setBatchSize(batchSize);
        this.rewriteData.buildNewScanNodeRange(minFileSizeBytes, rewriteAll);
    }

    public ConnectContext buildSubConnectContext(ConnectContext connectContext, boolean isFirstSubContext) {
        // Create a new ConnectContext for the sub task of cache select.
        final ConnectContext context = new ConnectContext(null);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setDatabase(connectContext.getDatabase());
        context.setQualifiedUser(connectContext.getQualifiedUser());
        context.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        context.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        context.setAuditEventBuilder(connectContext.getAuditEventBuilder());
        context.setResourceGroup(connectContext.getResourceGroup());
        context.setSessionId(connectContext.getSessionId());
        context.setRemoteIP(connectContext.getRemoteIP());
        context.setQueryId(connectContext.getQueryId());
        context.getState().reset();

        TUniqueId queryId = UUIDUtil.toTUniqueId(connectContext.getQueryId());
        TUniqueId executionId;
        if (isFirstSubContext) {
            executionId = queryId;
        } else {
            // For compute resources except the first one, generate different execution_id here.
            // We make the high part of query id unchanged to facilitate tracing problem by log.
            executionId = new TUniqueId(queryId.hi, UUIDUtil.genUUID().getLeastSignificantBits());
            LOG.debug("generate a new execution id {} for query {}", DebugUtil.printId(executionId), DebugUtil.printId(queryId));
        }
        context.setExecutionId(executionId);
        // NOTE: Ensure the thread local connect context is always the same with the newest ConnectContext.
        // NOTE: Ensure this thread local is removed after this method to avoid memory leak in JVM.
        context.setThreadLocalInfo();

        // clone an new session variable
        SessionVariable sessionVariable = (SessionVariable) connectContext.getSessionVariable().clone();
        context.setSessionVariable(sessionVariable);

        return context;
    }

    public void execute() throws Exception {
        if (scanNodes == null || scanNodes.isEmpty()) {
            LOG.info("No IcebergScanNode for empty table, rewrite data job execute skipped.");
            return;
        }

        if (rewriteStmt == null || execPlan == null || rewriteData == null) {
            throw new IllegalStateException("Must call prepare() before execute()");
        }
        ExecutorService executorService = newWorkerPool(originAlterStmt.getCatalogName() 
                + "-sr-iceberg-compaction-worker-pool", (int) batchParallelism);
        CompletionService<Void> ecs = new ExecutorCompletionService<>(executorService);
        int inFlight = 0;
        int submitted = 0;
        Exception first = null;
        try {
            int taskCount = 0;
            while (rewriteData.hasMoreTaskGroup()) {
                List<RemoteFileInfo> res = rewriteData.nextTaskGroup();
                final boolean isFirst = (taskCount == 0);
                if (inFlight >= batchParallelism + 1) { // +1 is for buffer
                    try {
                        ecs.take().get();
                    } catch (Exception ee) {
                        if (first == null && ee.getCause() instanceof Exception) {
                            first = (Exception) ee.getCause();
                        }
                        executorService.shutdownNow();
                        break;
                    } finally {
                        inFlight--;
                    }
                }
                ecs.submit(() -> {
                    ConnectContext subCtx = buildSubConnectContext(context, isFirst);

                    IcebergRewriteStmt localStmt = new IcebergRewriteStmt((InsertStmt) parsedStmt, rewriteAll);
                    ExecPlan localPlan = StatementPlanner.plan(parsedStmt, subCtx);

                    List<IcebergScanNode> localScanNodes = localPlan.getFragments().stream()
                            .flatMap(f -> f.collectScanNodes().values().stream())
                            .filter(s -> s instanceof IcebergScanNode && "IcebergScanNode".equals(s.getPlanNodeName()))
                            .map(s -> (IcebergScanNode) s)
                            .collect(Collectors.toList());

                    if (localScanNodes.isEmpty()) {
                        LOG.info("No IcebergScanNode in sub plan. Skip one task group.");
                        return null;
                    }
                    for (IcebergScanNode sn : localScanNodes) {
                        sn.rebuildScanRange(res);
                    }

                    StmtExecutor exec = StmtExecutor.newInternalExecutor(subCtx, localStmt);
                    exec.handleDMLStmt(localPlan, localStmt);
                    return null;
                });
                inFlight++;
                submitted++;
                taskCount++;
            }

            while (inFlight > 0) {
                try {
                    ecs.take().get();
                } catch (Exception ee) {
                    if (first == null && ee.getCause() instanceof Exception) {
                        first = (Exception) ee.getCause();
                    }
                    executorService.shutdownNow();
                } finally {
                    inFlight--;
                }
            }
            if (first != null) {
                throw first;
            }
        } catch (Exception e) {
            LOG.warn("Failed to rewrite iceberg table: {}, catalog: {}, db: {}, table: {}",
                    e.getMessage(), originAlterStmt.getCatalogName(), 
                    originAlterStmt.getDbName(), originAlterStmt.getTableName());
            context.getState().setError(e.getMessage());
            return;
        }
    }
}