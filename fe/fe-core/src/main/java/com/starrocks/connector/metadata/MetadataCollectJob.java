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

package com.starrocks.connector.metadata;

import com.google.common.collect.Sets;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class MetadataCollectJob {
    private static final Logger LOG = LogManager.getLogger(MetadataCollectJob.class);
    protected static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final TResultSinkType sinkType;
    private String sql;
    private ConnectContext context;
    private final ConcurrentLinkedQueue<TResultBatch> resultQueue = new ConcurrentLinkedQueue<>();
    private final MetadataExecutor metadataExecutor = new MetadataExecutor();

    public MetadataCollectJob(String catalogName, String dbName, String tableName, TResultSinkType sinkType) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.sinkType = sinkType;
    }

    public String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }

    public void asyncCollectMetadata() {
        metadataExecutor.asyncExecuteSQL(this);
    }

    protected abstract String buildCollectMetadataSQL();

    public void init(SessionVariable sessionVariable) {
        this.sql = buildCollectMetadataSQL();
        this.context = buildConnectContext(sessionVariable);
    }

    protected String getCatalogName() {
        return catalogName;
    }

    protected String getDbName() {
        return dbName;
    }

    protected String getTableName() {
        return tableName;
    }

    public String getSql() {
        return sql;
    }

    public ConnectContext getContext() {
        return context;
    }

    public ConcurrentLinkedQueue<TResultBatch> getResultQueue() {
        return resultQueue;
    }

    public Coordinator getMetadataJobCoord() {
        return metadataExecutor.getCoordinator();
    }

    public TResultSinkType getSinkType() {
        return sinkType;
    }

    protected ConnectContext buildConnectContext(SessionVariable originSessionVariable) {
        ConnectContext context = ConnectContext.buildInner();
        context.getSessionVariable().setEnableProfile(originSessionVariable.isEnableMetadataProfile());
        context.getSessionVariable().setParallelExecInstanceNum(1);
        context.getSessionVariable().setQueryTimeoutS(originSessionVariable.getMetadataCollectQueryTimeoutS());
        context.getSessionVariable().setEnablePipelineEngine(true);
        context.getSessionVariable().setEnableIcebergColumnStatistics(originSessionVariable.enableIcebergColumnStatistics());
        context.setMetadataContext(true);
        context.setCurrentCatalog(catalogName);
        context.setDatabase(dbName);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.setQualifiedUser(UserIdentity.ROOT.getUser());
        context.setQueryId(UUIDUtil.genUUID());
        context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setStartTime();
        context.setCurrentWarehouse(originSessionVariable.getWarehouseName());

        return context;
    }

}
