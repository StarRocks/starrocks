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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra;
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
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class IcebergRewriteDataJob {
    private static final Logger LOG = LogManager.getLogger(IcebergRewriteDataJob.class);
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
    private final ConcurrentLinkedQueue<FinishArgs> collected = new ConcurrentLinkedQueue<>();
    public interface FinishSinkHandler {
        void finish(String catalog, String db, String table,
                List<TSinkCommitInfo> commitInfos, String branch, Object extra) throws Exception;
    }

    public class FinishArgs {
        private final String catalog;
        private final String db;
        private final String table;
        private final List<TSinkCommitInfo> commitInfos;
        private final String branch;
        private final Object extra;
    
        public FinishArgs(String catalog, String db, String table,
                          List<TSinkCommitInfo> commitInfos, String branch, Object extra) {
            this.catalog = catalog;
            this.db = db;
            this.table = table;
            this.commitInfos = Objects.requireNonNull(commitInfos, "commitInfos");
            this.branch = branch;
            this.extra = extra;
        }
    
        public String getCatalog() { 
            return catalog; 
        }
        public String getDb() { 
            return db; 
        }
        public String getTable() { 
            return table; 
        }
        public List<TSinkCommitInfo> getCommitInfos() { 
            return commitInfos; 
        }
        public String getBranch() { 
            return branch; 
        }
        public Object getExtra() { 
            return extra; 
        }
    
        @Override public String toString() {
            return "FinishArgs{" + catalog + "." + db + "." + table + ", commits=" + commitInfos.size() + "}";
        }
    }

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

    public ConnectContext buildSubConnectContext(ConnectContext connectContext) {
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
        context.setFinishSinkHandler(
                (FinishSinkHandler) (catalog, db, table, commitInfos, branch, extra) -> {
                    collected.add(new FinishArgs(catalog, db, table, commitInfos, branch, extra));
                });
        context.getState().reset();
        
        TUniqueId queryId = UUIDUtil.toTUniqueId(connectContext.getQueryId());
        TUniqueId executionId;
        executionId = new TUniqueId(queryId.hi, UUIDUtil.genUUID().getLeastSignificantBits());
        LOG.debug("generate a new execution id {} for query {}", DebugUtil.printId(executionId), DebugUtil.printId(queryId));
        context.setExecutionId(executionId);
        // NOTE: Ensure the thread local connect context is always the same with the newest ConnectContext.
        // NOTE: Ensure this thread local is removed after this method to avoid memory leak in JVM.
        context.setThreadLocalInfo();

        // clone an new session variable
        SessionVariable sessionVariable = (SessionVariable) connectContext.getSessionVariable().clone();
        sessionVariable.setQueryTimeoutS(SessionVariable.MAX_QUERY_TIMEOUT);
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
        ExecutorService executorService = Executors.newFixedThreadPool((int) batchParallelism,
                new ThreadFactoryBuilder().setNameFormat(
                        originAlterStmt.getTableName() + "-sr-iceberg-compaction-worker-%d").build());
        List<Future<?>> futures = Lists.newArrayList();
        try {
            while (rewriteData.hasMoreTaskGroup()) {
                List<RemoteFileInfo> res = rewriteData.nextTaskGroup();
                if (res.isEmpty()) {
                    continue;
                }
                futures.add(executorService.submit(() -> {
                    ConnectContext subCtx = buildSubConnectContext(context);
    
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
                    if (context.getExecutor() != null) {
                        context.getExecutor().registerSubStmtExecutor(exec);
                    }
                    try {
                        exec.handleDMLStmt(localPlan, localStmt);
                    } finally {
                        exec.addFinishedQueryDetail();
                    }
                    return null;
                }));
            }
            
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    LOG.error("Failed to compact files on [{}]", originAlterStmt.getTableName(), e);
                    throw new StarRocksConnectorException("Failed to compact files", e);
                }
            }

            if (collected.size() == 0) {
                return;
            }
            IcebergSinkExtra extra = new IcebergSinkExtra();
            List<TSinkCommitInfo> faList = Lists.newArrayList();
            for (FinishArgs fa : collected) {
                extra.addScannedDataFiles(((IcebergSinkExtra) fa.getExtra()).getScannedDataFiles());
                extra.addAppliedDeleteFiles(((IcebergSinkExtra) fa.getExtra()).getAppliedDeleteFiles());
                faList.addAll(fa.getCommitInfos());
            }
            try {
                context.getGlobalStateMgr().getMetadataMgr().finishSink(
                        collected.peek().getCatalog(), collected.peek().getDb(), collected.peek().getTable(),
                        faList, collected.peek().getBranch(), extra);
            } catch (Exception e) {
                LOG.error("Failed to commit iceberg rewrite on [{}]", originAlterStmt.getTableName(), e);
                throw new StarRocksConnectorException("Failed to commit iceberg rewrite", e);
            }
        } catch (Exception e) {
            LOG.warn("Failed to rewrite iceberg table: {}, catalog: {}, db: {}, table: {}",
                    e.getMessage(), originAlterStmt.getCatalogName(),
                    originAlterStmt.getDbName(), originAlterStmt.getTableName());
            context.getState().setError(e.getMessage());
            throw e;
        } finally {
            executorService.shutdownNow();
        }
    }
}