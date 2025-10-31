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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.LakeTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.warehouse.Warehouse;

import java.util.List;
import java.util.Set;

public class LakeTableAlterMetaJob extends LakeTableAlterMetaJobBase {
    @SerializedName(value = "metaType")
    private TTabletMetaType metaType;

    @SerializedName(value = "metaValue")
    private boolean metaValue;

    @SerializedName(value = "persistentIndexType")
    private String persistentIndexType;

    @SerializedName(value = "enableFileBundling")
    private boolean enableFileBundling;

    @SerializedName(value = "compactionStrategy")
    private String compactionStrategy;

    // for deserialization
    public LakeTableAlterMetaJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public LakeTableAlterMetaJob(long jobId, long dbId, long tableId, String tableName,
                                 long timeoutMs, TTabletMetaType metaType, boolean metaValue,
                                 String persistentIndexType) {
        this(jobId, dbId, tableId, tableName, timeoutMs, metaType, metaValue, persistentIndexType,
                false, "DEFAULT");
    }

    public LakeTableAlterMetaJob(long jobId, long dbId, long tableId, String tableName,
                                 long timeoutMs, TTabletMetaType metaType, boolean metaValue,
                                 String persistentIndexType,
                                 boolean enableFileBundling,
                                 String compactionStrategy) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        this.metaType = metaType;
        this.metaValue = metaValue;
        this.persistentIndexType = persistentIndexType;
        this.enableFileBundling = enableFileBundling;
        this.compactionStrategy = compactionStrategy;
    }

    @Override
    protected TabletMetadataUpdateAgentTask createTask(PhysicalPartition partition,
            MaterializedIndex index, long nodeId, Set<Long> tablets) {
        if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            return TabletMetadataUpdateAgentTaskFactory.createLakePersistentIndexUpdateTask(nodeId, tablets,
                        metaValue, persistentIndexType);
        }
        if (metaType == TTabletMetaType.ENABLE_FILE_BUNDLING) {
            return TabletMetadataUpdateAgentTaskFactory.createUpdateFileBundlingTask(nodeId, tablets,
                        enableFileBundling);
        }
        if (metaType == TTabletMetaType.COMPACTION_STRATEGY) {
            return TabletMetadataUpdateAgentTaskFactory.createUpdateCompactionStrategyTask(nodeId, tablets,
                        compactionStrategy);
        }
        return null;
    }

    @Override
    protected boolean enableFileBundling() {
        return metaType == TTabletMetaType.ENABLE_FILE_BUNDLING && enableFileBundling;
    }

    @Override
    protected boolean disableFileBundling() {
        return metaType == TTabletMetaType.ENABLE_FILE_BUNDLING && !enableFileBundling;
    }

    @Override
    protected void updateCatalog(Database db, LakeTable table) {
        if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            // re-use ENABLE_PERSISTENT_INDEX for both enable index and index's type.
            table.getTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX,
                    String.valueOf(metaValue));
            table.getTableProperty().buildEnablePersistentIndex();
            table.getTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE,
                    String.valueOf(persistentIndexType));
            table.getTableProperty().buildPersistentIndexType();
        }
        if (metaType == TTabletMetaType.ENABLE_FILE_BUNDLING) {
            table.setFileBundling(enableFileBundling);
        }
        if (metaType == TTabletMetaType.COMPACTION_STRATEGY) {
            table.getTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY,
                    String.valueOf(compactionStrategy));
            table.getTableProperty().buildCompactionStrategy();
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && getBatchTask() != null) {
            progress = getBatchTask().getFinishedTaskNum() + "/" + getBatchTask().getTaskNum();
        }

        for (MaterializedIndex index : getPhysicalPartitionIndexMap().values()) {
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeString(createTimeMs));
            info.add(TimeUtils.longToTimeString(finishedTimeMs));
            // here we just use index id as index name
            info.add(index.getId());
            info.add(index.getId());
            info.add(index.getId());

            // just set null
            info.add("null"); // schema version and schema hash
            info.add(getWatershedTxnId());
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
            if (warehouse == null) {
                info.add("null");
            } else {
                info.add(warehouse.getName());
            }
            infos.add(info);
        }
    }

    @Override
    protected void restoreState(LakeTableAlterMetaJobBase job) {
        LakeTableAlterMetaJob other = (LakeTableAlterMetaJob) job;
        this.metaType = other.metaType;
        this.metaValue = other.metaValue;
        this.persistentIndexType = other.persistentIndexType;
        this.enableFileBundling = other.enableFileBundling;
        this.compactionStrategy = other.compactionStrategy;
    }



}
