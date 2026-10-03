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

package com.starrocks.scheduler;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.iceberg.IcebergMVStorage;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Mechanism C: refresh an Iceberg-catalog (IRC) materialized view. The task definition is
 * {@code INSERT OVERWRITE <catalog>.<db>.<mv>__storage SELECT <query>}; before running it we build the Iceberg
 * {@link org.apache.iceberg.view.RefreshState} from the base tables' current snapshots and set it on the
 * {@link ConnectContext}, so {@code IcebergMetadata.finishSink} stamps it into the overwrite snapshot summary (one
 * atomic Iceberg commit). No local {@code MaterializedView} is resolved and no {@code (dbId,mvId)} lock is taken;
 * same-MV serialization comes from {@code TaskRunScheduler} (one running TaskRun per Task) and base-table
 * consistency from Iceberg snapshot isolation.
 */
public class IcebergMVRefreshProcessor extends BaseTaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(IcebergMVRefreshProcessor.class);

    // Task property keys for an ICEBERG_MV task.
    public static final String PROP_BASE_TABLES = "iceberg_mv.base_tables";
    public static final String PROP_VIEW_VERSION_ID = "iceberg_mv.view_version_id";
    // The MV's defining SELECT, stashed so RENAME can rebuild the INSERT against the renamed storage table.
    public static final String PROP_QUERY = "iceberg_mv.query_sql";
    private static final String FIELD_SEP = "\u0001";
    private static final String ENTRY_SEP = "\u0002";

    /** Encode base-table identifiers for storage in the Task properties (used by the task builder at CREATE time). */
    public static String encodeBaseTables(List<BaseTableInfo> baseTableInfos) {
        return baseTableInfos.stream()
                .map(b -> b.getCatalogName() + FIELD_SEP + b.getDbName() + FIELD_SEP + b.getTableName())
                .collect(Collectors.joining(ENTRY_SEP));
    }

    @Override
    public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
        ConnectContext ctx = context.getCtx();
        StmtExecutor executor = null;
        boolean refreshStateSet = false;
        try {
            ctx.setQuerySource(QueryDetail.QuerySource.TASK);
            ctx.getAuditEventBuilder().reset();
            ctx.getAuditEventBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp(context.getRemoteIp())
                    .setUser(ctx.getQualifiedUser())
                    .setDb(ctx.getDatabase())
                    .setCatalog(ctx.getCurrentCatalog());
            Tracers.register(ctx);
            Tracers.init(ctx, "TIMER", null);

            String refreshState = buildRefreshState(context);
            if (refreshState != null) {
                ctx.setIcebergMVRefreshState(refreshState);
                refreshStateSet = true;
            }

            StatementBase sqlStmt = SqlParser.parse(context.getDefinition(), ctx.getSessionVariable()).get(0);
            sqlStmt.setOrigStmt(new OriginStatement(context.getDefinition(), 0));
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitRelation(Relation relation, Void ctxArg) {
                    relation.setNeedRewrittenByPolicy(true);
                    return null;
                }
            }.visit(sqlStmt);

            executor = StmtExecutor.newInternalExecutor(ctx, sqlStmt);
            ctx.setExecutor(executor);
            ctx.setMultiStmt(false);
            ctx.setThreadLocalInfo();
            executor.addRunningQueryDetail(sqlStmt);
            executor.execute();
            return Constants.TaskRunState.SUCCESS;
        } finally {
            if (refreshStateSet) {
                ctx.setIcebergMVRefreshState(null);
            }
            Tracers.close();
            if (executor != null) {
                auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
                executor.addFinishedQueryDetail();
            } else {
                auditAfterExec(context, null, null);
            }
        }
    }

    // Build the Iceberg RefreshState from the base tables recorded on the Task (their current snapshots are read
    // now, at refresh time). Returns null if nothing to record, in which case the overwrite still commits (readers
    // just can't assert freshness).
    private String buildRefreshState(TaskRunContext context) {
        try {
            Map<String, String> props = context.getProperties();
            if (props == null) {
                return null;
            }
            String encoded = props.get(PROP_BASE_TABLES);
            if (encoded == null || encoded.isEmpty()) {
                return null;
            }
            List<BaseTableInfo> baseInfos = Lists.newArrayList();
            for (String entry : encoded.split(ENTRY_SEP, -1)) {
                if (entry.isEmpty()) {
                    continue;
                }
                String[] f = entry.split(FIELD_SEP, -1);
                if (f.length >= 3) {
                    // tableIdentifier best-effort = table name; external iceberg tables resolve by catalog.db.table.
                    baseInfos.add(new BaseTableInfo(f[0], f[1], f[2], f[2]));
                }
            }
            int viewVersionId = 1;
            String v = props.get(PROP_VIEW_VERSION_ID);
            if (v != null) {
                try {
                    viewVersionId = Integer.parseInt(v);
                } catch (NumberFormatException ignored) {
                    // keep default
                }
            }
            return IcebergMVStorage.serializeRefreshState(IcebergMVStorage.buildRefreshState(baseInfos, viewVersionId));
        } catch (Exception e) {
            LOG.warn("Failed to build iceberg mv refresh-state: {}", e.getMessage());
            return null;
        }
    }
}
