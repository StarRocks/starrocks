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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/Alter.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.alter;

import com.starrocks.common.DdlException;
import com.starrocks.meta.store.AlterJobMgrIface;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.IForwardCompatibleObject;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.ShowAlterStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class AlterJobMgr implements AlterJobMgrIface {
    private static final Logger LOG = LogManager.getLogger(AlterJobMgr.class);
    public static final String MANUAL_INACTIVE_MV_REASON = "user use alter materialized view set status to inactive";

    private final SchemaChangeHandler schemaChangeHandler;
    private final MaterializedViewHandler materializedViewHandler;
    private final SystemHandler clusterHandler;

    public AlterJobMgr(SchemaChangeHandler schemaChangeHandler,
                       MaterializedViewHandler materializedViewHandler,
                       SystemHandler systemHandler) {
        this.schemaChangeHandler = schemaChangeHandler;
        this.materializedViewHandler = materializedViewHandler;
        this.clusterHandler = systemHandler;
    }

    public void start() {
        schemaChangeHandler.start();
        materializedViewHandler.start();
        clusterHandler.start();
    }

    public void stop() {
        schemaChangeHandler.setStop();
        materializedViewHandler.setStop();
        clusterHandler.setStop();
    }

    /*
     * used for handling CancelAlterStmt (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    public void cancelAlter(CancelAlterTableStmt stmt, String reason) throws DdlException {
        if (stmt.getAlterType() == ShowAlterStmt.AlterType.ROLLUP) {
            materializedViewHandler.cancel(stmt, reason);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.COLUMN
                || stmt.getAlterType() == ShowAlterStmt.AlterType.OPTIMIZE) {
            schemaChangeHandler.cancel(stmt, reason);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            materializedViewHandler.cancelMV(stmt);
        } else {
            throw new DdlException("Cancel " + stmt.getAlterType() + " does not implement yet");
        }
    }

    public SchemaChangeHandler getSchemaChangeHandler() {
        return this.schemaChangeHandler;
    }

    public MaterializedViewHandler getMaterializedViewHandler() {
        return this.materializedViewHandler;
    }

    public SystemHandler getClusterHandler() {
        return this.clusterHandler;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        Map<Long, AlterJobV2> schemaChangeAlterJobs = schemaChangeHandler.getAlterJobsV2();
        Map<Long, AlterJobV2> materializedViewAlterJobs = materializedViewHandler.getAlterJobsV2();

        int cnt = 1 + schemaChangeAlterJobs.size() + 1 + materializedViewAlterJobs.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.ALTER_MGR, cnt);

        writer.writeInt(schemaChangeAlterJobs.size());
        for (AlterJobV2 alterJobV2 : schemaChangeAlterJobs.values()) {
            writer.writeJson(alterJobV2);
        }

        writer.writeInt(materializedViewAlterJobs.size());
        for (AlterJobV2 alterJobV2 : materializedViewAlterJobs.values()) {
            writer.writeJson(alterJobV2);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int schemaChangeJobSize = reader.readInt();
        for (int i = 0; i != schemaChangeJobSize; ++i) {
            AlterJobV2 alterJobV2 = reader.readJson(AlterJobV2.class);
            if (alterJobV2 instanceof IForwardCompatibleObject) {
                LOG.warn("Ignore unknown alterJobV2(id: {}) from the future version!", alterJobV2.getJobId());
                continue;
            }
            schemaChangeHandler.addAlterJobV2(alterJobV2);

            // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpoint
            // to prevent TabletInvertedIndex data loss,
            // So just use AlterJob.replay() instead of AlterHandler.replay().
            if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                alterJobV2.replay(alterJobV2);
                LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
            }
        }

        int materializedViewJobSize = reader.readInt();
        for (int i = 0; i != materializedViewJobSize; ++i) {
            AlterJobV2 alterJobV2 = reader.readJson(AlterJobV2.class);
            if (alterJobV2 instanceof IForwardCompatibleObject) {
                LOG.warn("Ignore unknown MV job(id: {}) from the future version!", alterJobV2.getJobId());
                continue;
            }
            materializedViewHandler.addAlterJobV2(alterJobV2);

            // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpoint
            // to prevent TabletInvertedIndex data loss,
            // So just use AlterJob.replay() instead of AlterHandler.replay().
            if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                alterJobV2.replay(alterJobV2);
                LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
            }
        }
    }

    @Override
    public void logAlterJob(AlterJobV2 alterJob) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(alterJob);
    }

    @Override
    public void finishRollupJob(RollupJobV2 rollupJob) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(rollupJob);
    }

    @Override
    public void cancelRollupJob(RollupJobV2 rollupJob) {
        rollupJob.cancelInternal();
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(rollupJob);
    }
}
