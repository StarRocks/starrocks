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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections4.ListUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The overall workflow to modify the table schema is as follows:
 * 1. Begin a new transaction.
 * 2. Send {@link com.starrocks.thrift.TUpdateTabletMetaInfoReq} requests to all tablet in the table.
 * 3. Get the new tablet schema from the TUpdateTabletMetaInfoReq and writes it to the txn log for each tablet.
 * 4. Commit transaction.
 * 5. Send {@link com.starrocks.proto.PublishVersionRequest} requests to all tablets.
 * 6. Apply txn log on each tablet to create a new version of the tablet metadata with the new tablet schema.
 * 7. Modify the table schema and visible version in the FE catalog
 * 8. Finish the transaction
 */
public class LakeTableAsyncFastSchemaChangeJob extends LakeTableAlterMetaJobBase implements GsonPostProcessable {
    // shadow index id -> index schema
    @SerializedName(value = "schemaInfos")
    private List<IndexSchemaInfo> schemaInfos;
    private Set<String> partitionsWithSchemaFile = new HashSet<>();

    // for deserialization
    public LakeTableAsyncFastSchemaChangeJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    LakeTableAsyncFastSchemaChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        schemaInfos = new ArrayList<>();
    }

    LakeTableAsyncFastSchemaChangeJob(LakeTableAsyncFastSchemaChangeJob other) {
        this(other.getJobId(), other.getDbId(), other.getTableId(), other.getTableName(), other.getTimeoutMs());
        for (IndexSchemaInfo indexSchemaInfo : other.schemaInfos) {
            setIndexTabletSchema(indexSchemaInfo.indexId, indexSchemaInfo.indexName, indexSchemaInfo.schemaInfo);
        }
        partitionsWithSchemaFile.addAll(other.partitionsWithSchemaFile);
    }

    public void setIndexTabletSchema(long indexId, String indexName, SchemaInfo schemaInfo) {
        schemaInfos.add(new IndexSchemaInfo(indexId, indexName, schemaInfo));
    }

    @Override
    protected TabletMetadataUpdateAgentTask createTask(PhysicalPartition partition, MaterializedIndex index, long nodeId,
                                                       Set<Long> tablets) {
        String tag = String.format("%d_%d", partition.getId(), index.getId());
        TabletMetadataUpdateAgentTask task = null;
        boolean needUpdateSchema = false;
        for (IndexSchemaInfo info : schemaInfos) {
            if (info.indexId == index.getId()) {
                needUpdateSchema = true;
                // `Set.add()` returns true means this set did not already contain the specified element
                boolean createSchemaFile = partitionsWithSchemaFile.add(tag);
                task = TabletMetadataUpdateAgentTaskFactory.createTabletSchemaUpdateTask(nodeId,
                        new ArrayList<>(tablets), info.schemaInfo.toTabletSchema(), createSchemaFile);
                break;
            }
        }

        // if the index is not in schemaInfos, it means the schema of index are not needed to be modified,
        // but we still need to update the tablet meta to improve the meta version
        if (!needUpdateSchema) {
            task = TabletMetadataUpdateAgentTaskFactory.createTabletSchemaUpdateTask(nodeId,
                    new ArrayList<>(tablets), null, false);
        }

        return task;
    }

    @Override
    protected void updateCatalog(Database db, LakeTable table) {
        updateCatalogUnprotected(db, table);
    }

    private void updateCatalogUnprotected(Database db, LakeTable table) {
        Set<String> droppedOrModifiedColumns = Sets.newHashSet();
        boolean hasMv = !table.getRelatedMaterializedViews().isEmpty();
        for (IndexSchemaInfo indexSchemaInfo : schemaInfos) {
            SchemaInfo schemaInfo = indexSchemaInfo.schemaInfo;
            long indexId = indexSchemaInfo.indexId;
            MaterializedIndexMeta indexMeta = requireNonNull(table.getIndexMetaByIndexId(indexId)).shallowCopy();
            List<Column> oldColumns = indexMeta.getSchema();

            Preconditions.checkState(Objects.equals(indexMeta.getKeysType(), schemaInfo.getKeysType()));
            Preconditions.checkState(Objects.equals(ListUtils.emptyIfNull(indexMeta.getSortKeyUniqueIds()),
                    ListUtils.emptyIfNull(schemaInfo.getSortKeyUniqueIds())));
            Preconditions.checkState(schemaInfo.getVersion() > indexMeta.getSchemaVersion());
            Preconditions.checkState(Objects.equals(indexMeta.getShortKeyColumnCount(), schemaInfo.getShortKeyColumnCount()));

            if (hasMv) {
                droppedOrModifiedColumns.addAll(AlterHelper.collectDroppedOrModifiedColumns(oldColumns, schemaInfo.getColumns()));
            }

            indexMeta.setSchema(schemaInfo.getColumns());
            indexMeta.setSchemaVersion(schemaInfo.getVersion());
            indexMeta.setSchemaId(schemaInfo.getId());
            indexMeta.setSortKeyIdxes(schemaInfo.getSortKeyIndexes());

            // update the indexIdToMeta
            table.getIndexIdToMeta().put(indexId, indexMeta);
            table.setIndexes(schemaInfo.getIndexes());
            table.renameColumnNamePrefix(indexId);
        }
        table.rebuildFullSchema();

        // If modified columns are already done, inactive related mv
        AlterMVJobExecutor.inactiveRelatedMaterializedViews(db, table, droppedOrModifiedColumns);
    }

    @Override
    protected void restoreState(LakeTableAlterMetaJobBase job) {
        this.schemaInfos = new ArrayList<>(((LakeTableAsyncFastSchemaChangeJob) job).schemaInfos);
    }



    private static class IndexSchemaInfo {
        @SerializedName("indexId")
        private final long indexId;
        @SerializedName("indexName")
        private final String indexName;
        @SerializedName("schemaInfo")
        private final SchemaInfo schemaInfo;

        IndexSchemaInfo(long indexId, String indexName, SchemaInfo schemaInfo) {
            this.indexId = indexId;
            this.indexName = indexName;
            this.schemaInfo = requireNonNull(schemaInfo, "schema is null");
        }
    }

    List<SchemaInfo> getSchemaInfoList() {
        return schemaInfos.stream().map(i -> i.schemaInfo).collect(Collectors.toList());
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && getBatchTask() != null) {
            progress = getBatchTask().getFinishedTaskNum() + "/" + getBatchTask().getTaskNum();
        }

        for (IndexSchemaInfo schemaInfo : schemaInfos) {
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeString(createTimeMs));
            info.add(TimeUtils.longToTimeString(finishedTimeMs));
            info.add(schemaInfo.indexName);
            info.add(schemaInfo.indexId);
            info.add(schemaInfo.indexId);
            info.add(String.format("%d:0", schemaInfo.schemaInfo.getVersion())); // schema version and schema hash
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
    public void gsonPostProcess() throws IOException {
        partitionsWithSchemaFile = new HashSet<>();
    }
}
