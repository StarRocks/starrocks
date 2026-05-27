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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ConvertEqualityDeletesPlanner;
import com.starrocks.sql.ast.ConvertEqualityDeletesStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import org.apache.iceberg.DeleteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

// Plans and runs one equality-delete -> position-delete conversion. Single distributed execution and
// a single atomic commit: the output is tiny (file_path + pos), the engine parallelizes scan/join
// across BEs, and the atomic swap (add position deletes, remove equality deletes) needs one commit.
public class IcebergConvertEqualityDeleteJob {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergConvertEqualityDeleteJob.class);

    private final ConnectContext context;
    private final IcebergTable icebergTable;
    private final long snapshotId;
    private final TableRef tableRef;

    public IcebergConvertEqualityDeleteJob(ConnectContext context, IcebergTable icebergTable, long snapshotId,
                                           TableRef tableRef) {
        this.context = context;
        this.icebergTable = icebergTable;
        this.snapshotId = snapshotId;
        this.tableRef = tableRef;
    }

    public ConvertMetrics execute() throws Exception {
        ConnectContext subCtx = buildSubConnectContext(context);
        try (var scope = subCtx.bindScope()) {
            ExecPlan execPlan = new ConvertEqualityDeletesPlanner().plan(icebergTable, snapshotId, subCtx);
            if (execPlan == null) {
                LOG.info("No equality-delete files for {} at snapshot {}, conversion skipped.",
                        icebergTable.getName(), snapshotId);
                return ConvertMetrics.EMPTY;
            }
            ConvertEqualityDeletesStmt carrier = new ConvertEqualityDeletesStmt(tableRef);
            StmtExecutor exec = StmtExecutor.newInternalExecutor(subCtx, carrier);
            // Register with the parent executor so cancelling the ALTER TABLE ... EXECUTE statement
            // propagates to this internal distributed scan/write.
            if (context.getExecutor() != null) {
                context.getExecutor().registerSubStmtExecutor(exec);
            }
            exec.handleDMLStmt(execPlan, carrier);
            return buildMetrics(execPlan);
        }
    }

    private ConvertMetrics buildMetrics(ExecPlan execPlan) {
        DataSink sink = execPlan.getFragments().get(0).getSink();
        if (sink instanceof IcebergDeleteSink deleteSink && deleteSink.getSinkExtraInfo() != null) {
            IcebergMetadata.IcebergSinkExtra extra = deleteSink.getSinkExtraInfo();
            Set<DeleteFile> removed = extra.getEqualityDeleteFilesToRemove();
            long equalityDeleteRows = removed.stream().mapToLong(DeleteFile::recordCount).sum();
            return new ConvertMetrics(removed.size(), equalityDeleteRows,
                    extra.getAddedPositionDeleteFiles(), extra.getPositionDeleteRows());
        }
        return ConvertMetrics.EMPTY;
    }

    private ConnectContext buildSubConnectContext(ConnectContext connectContext) {
        ConnectContext subCtx = new ConnectContext(null);
        subCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        subCtx.setDatabase(connectContext.getDatabase());
        subCtx.setQualifiedUser(connectContext.getQualifiedUser());
        subCtx.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        subCtx.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        subCtx.setAuditEventBuilder(connectContext.getAuditEventBuilder());
        subCtx.setResourceGroup(connectContext.getResourceGroup());
        subCtx.setSessionId(connectContext.getSessionId());
        subCtx.setRemoteIP(connectContext.getRemoteIP());
        subCtx.setQueryId(connectContext.getQueryId());
        subCtx.getState().reset();

        TUniqueId queryId = UUIDUtil.toTUniqueId(connectContext.getQueryId());
        TUniqueId executionId = new TUniqueId(queryId.hi, UUIDUtil.genUUID().getLeastSignificantBits());
        LOG.debug("generate execution id {} for convert equality deletes on {}",
                DebugUtil.printId(executionId), icebergTable.getName());
        subCtx.setExecutionId(executionId);

        SessionVariable sessionVariable = (SessionVariable) connectContext.getSessionVariable().clone();
        sessionVariable.setQueryTimeoutS(SessionVariable.MAX_QUERY_TIMEOUT);
        // The equality-delete scan is the small build side: ensure its runtime filter is built and that
        // the data-file scan waits for it instead of starting blind. Convert is an admin batch, so a
        // generous scan wait is cheap and worth the file pruning it buys.
        sessionVariable.setEnableGlobalRuntimeFilter(true);
        sessionVariable.setRuntimeFilterScanWaitTime(1000L);
        subCtx.setSessionVariable(sessionVariable);
        return subCtx;
    }

    public record ConvertMetrics(long removedEqualityDeleteFiles, long equalityDeleteRows,
                                 long addedPositionDeleteFiles, long positionDeleteRows) {
        public static final ConvertMetrics EMPTY = new ConvertMetrics(0, 0, 0, 0);
    }
}
