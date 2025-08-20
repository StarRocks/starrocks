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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergRewriteData;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.IcebergRewriteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

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

    public IcebergRewriteDataJob(String insertSql,
                                 boolean rewriteAll,
                                 long minFileSizeBytes,
                                 long batchSize,
                                 ConnectContext context,
                                 AlterTableStmt stmt) {
        this.insertSql = insertSql;
        this.rewriteAll = rewriteAll;
        this.minFileSizeBytes = minFileSizeBytes;
        this.batchSize = batchSize;
        this.context = context;
        this.originAlterStmt = stmt;
    }

    public void prepare() throws Exception {
        StatementBase parsedStmt = com.starrocks.sql.parser.SqlParser
                .parse(insertSql, context.getSessionVariable())
                .get(0);

        rewriteStmt = new IcebergRewriteStmt((InsertStmt) parsedStmt, rewriteAll);
        this.execPlan = StatementPlanner.plan(parsedStmt, context);
        this.scanNodes = execPlan.getFragments().stream()
                .flatMap(fragment -> fragment.collectScanNodes().values().stream())
                .filter(scan -> scan instanceof IcebergScanNode && "IcebergScanNode".equals(scan.getPlanNodeName()))
                .map(scan -> (IcebergScanNode) scan)
                .collect(Collectors.toList());

        IcebergScanNode targetNode = scanNodes.stream().findFirst().orElse(null);
        if (targetNode == null) {
            throw new SemanticException("No valid IcebergScanNode found for rewrite.");
        }

        this.rewriteData = new IcebergRewriteData();
        this.rewriteData.setSource(targetNode.getSourceRange());
        this.rewriteData.setBatchSize(batchSize);
        this.rewriteData.buildNewScanNodeRange(minFileSizeBytes, rewriteAll);
    }

    public void execute() throws Exception {
        if (rewriteStmt == null || execPlan == null || rewriteData == null) {
            throw new IllegalStateException("Must call prepare() before execute()");
        }

        StmtExecutor executor = StmtExecutor.newInternalExecutor(context, rewriteStmt);
        try {
            while (rewriteData.hasMoreTaskGroup()) {
                List<RemoteFileInfo> res = rewriteData.nextTaskGroup();
                for (IcebergScanNode scanNode : scanNodes) {
                    scanNode.rebuildScanRange(res);
                }
                context.setQueryId(UUIDUtil.genUUID());
                context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
                executor.handleDMLStmt(execPlan, rewriteStmt);
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