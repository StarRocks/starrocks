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

package com.starrocks.connector;

import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TResultSinkType;
import io.trino.hive.$internal.com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;

// TODO(stephen): refactor and abstract metadata collect job
public class MetadataCollectJob {
    private static final Logger LOG = LogManager.getLogger(MetadataCollectJob.class);
    protected static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.Log4JLogChute");
        DEFAULT_VELOCITY_ENGINE.setProperty("runtime.log.logsystem.log4j.logger", "velocity");
    }

    private static final String ICEBERG_METADATA_TEMPLATE = "SELECT content" + // INTEGER
            ", file_path" + // VARCHAR
            ", file_format" + // VARCHAR
            ", spec_id" + // INTEGER
            ", partition_data" + // BINARY
            ", record_count" + // BIGINT
            ", file_size_in_bytes" + // BIGINT
            ", split_offsets" + // ARRAY<BIGINT>
            ", sort_id" + // INTEGER
            ", equality_ids" + // ARRAY<INTEGER>
            ", file_sequence_number" + // BIGINT
            ", data_sequence_number " + // BIGINT
            "FROM `$catalogName`.`$dbName`.`$tableName$logical_iceberg_metadata` " +
            "VERSION AS OF $snapshotId " +
            "WHERE $predicate'";

    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final Long snapshotId;
    private final String predicate;
    private final TResultSinkType sinkType;
    private String sql;
    private ConnectContext context;

    public MetadataCollectJob(String catalogName, String dbName, String tableName,
                              Long snapshotId, String predicate, TResultSinkType sinkType) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.snapshotId = snapshotId;
        this.predicate = predicate;
        this.sinkType = sinkType;
    }

    public String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }

    private String buildCollectIcebergMetadataSQL() {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();

        context.put("catalogName", catalogName);
        context.put("dbName", dbName);
        context.put("tableName", tableName);
        context.put("snapshotId", snapshotId);
        if (Strings.isNullOrEmpty(predicate)) {
            context.put("predicate", "1=1");
        } else {
            context.put("predicate", "predicate = '" + predicate + "'");
        }

        builder.append(build(context, ICEBERG_METADATA_TEMPLATE));
        return builder.toString();
    }

    public void build() {
        this.sql = buildCollectIcebergMetadataSQL();
        this.context = buildConnectContext();
        this.context.setThreadLocalInfo();
        setDefaultSessionVariable();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public Long getSnapshotId() {
        return snapshotId;
    }

    public String getPredicate() {
        return predicate;
    }

    public String getSql() {
        return sql;
    }

    public ConnectContext getContext() {
        return context;
    }

    public TResultSinkType getSinkType() {
        return sinkType;
    }

    private ConnectContext buildConnectContext() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setEnableProfile(false);
        context.getSessionVariable().setParallelExecInstanceNum(1);
        context.getSessionVariable().setQueryTimeoutS((int) Config.metadata_collect_query_timeout);
        context.getSessionVariable().setEnablePipelineEngine(true);
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

        return context;
    }

    private void setDefaultSessionVariable() {
        SessionVariable sessionVariable = context.getSessionVariable();
        sessionVariable.setEnableMaterializedViewRewrite(false);
    }

}
