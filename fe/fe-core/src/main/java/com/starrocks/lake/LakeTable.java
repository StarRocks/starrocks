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

package com.starrocks.lake;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.LakeTableAlterJobV2Builder;
import com.starrocks.backup.Status;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata for StarRocks lake table
 * todo: Rename to CloudNativeTable
 */
public class LakeTable extends OlapTable {

    private static final Logger LOG = LogManager.getLogger(LakeTable.class);

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo, TableIndexes indexes) {
        super(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo,
                GlobalStateMgr.getCurrentState().getClusterId(), indexes, TableType.CLOUD_NATIVE);
    }

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null);
    }

    private FilePathInfo getDefaultFilePathInfo() {
        return tableProperty.getStorageInfo().getFilePathInfo();
    }

    @Override
    public String getStoragePath() {
        return getDefaultFilePathInfo().getFullPath();
    }

    @Override
    public FilePathInfo getPartitionFilePathInfo() {
        return getDefaultFilePathInfo();
    }

    @Override
    public FileCacheInfo getPartitionFileCacheInfo(long partitionId) {
        FileCacheInfo cacheInfo = null;
        DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        if (dataCacheInfo == null) {
            cacheInfo = tableProperty.getStorageInfo().getCacheInfo();
        } else {
            cacheInfo = dataCacheInfo.getCacheInfo();
        }
        return cacheInfo;
    }

    @Override
    public void setStorageInfo(FilePathInfo pathInfo, DataCacheInfo dataCacheInfo) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setStorageInfo(new StorageInfo(pathInfo, dataCacheInfo.getCacheInfo()));
    }

    @Override
    public OlapTable selectiveCopy(Collection<String> reservedPartitions, boolean resetState,
                                   MaterializedIndex.IndexExtState extState) {
        LakeTable copied = DeepCopy.copyWithGson(this, LakeTable.class);
        if (copied == null) {
            LOG.warn("failed to copy lake table: {}", getName());
            return null;
        }
        return selectiveCopyInternal(copied, reservedPartitions, resetState, extState);
    }

    public static LakeTable read(DataInput in) throws IOException {
        // type is already read in Table
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeTable.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write type first
        Text.writeString(out, type.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public Runnable delete(boolean replay) {
        onErase(replay);
        return replay ? null : new DeleteLakeTableTask(this);
    }

    @Override
    public AlterJobV2Builder alterTable() {
        return new LakeTableAlterJobV2Builder(this);
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = super.getProperties();
        if (tableProperty != null) {
            StorageInfo storageInfo = tableProperty.getStorageInfo();
            if (storageInfo != null) {
                // datacache.enable
                properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE,
                        String.valueOf(storageInfo.isEnableDataCache()));

                // enable_async_write_back
                properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK,
                        String.valueOf(storageInfo.isEnableAsyncWriteBack()));
            }
        }
        return properties;
    }

    @Override
    public Status createTabletsForRestore(int tabletNum, MaterializedIndex index, GlobalStateMgr globalStateMgr,
                                          int replicationNum, long version, int schemaHash,
                                          long partitionId, long shardGroupId) {
        FilePathInfo fsInfo = getPartitionFilePathInfo();
        FileCacheInfo cacheInfo = getPartitionFileCacheInfo(partitionId);
        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(partitionId));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(index.getId()));
        List<Long> shardIds = null;
        try {
            // Ignore the parameter replicationNum
            shardIds = globalStateMgr.getStarOSAgent().createShards(tabletNum, fsInfo, cacheInfo, shardGroupId, properties);
        } catch (DdlException e) {
            LOG.error(e.getMessage());
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        for (long shardId : shardIds) {
            LakeTablet tablet = new LakeTablet(shardId);
            index.addTablet(tablet, null /* tablet meta */, false/* update inverted index */);
        }
        return Status.OK;
    }

    // used in colocate table index, return an empty list for LakeTable
    @Override
    public List<List<Long>> getArbitraryTabletBucketsSeq() throws DdlException {
        return Lists.newArrayList();
    }

    public List<Long> getShardGroupIds() {
        List<Long> shardGroupIds = new ArrayList<>();
        for (Partition p : getAllPartitions()) {
            shardGroupIds.add(p.getShardGroupId());
        }
        return shardGroupIds;
    }

    @Override
    public String getComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return comment;
        }
        return TableType.OLAP.name();
    }

    /**
     * Check if data cache is allowed for the specified partition's data:
     *  - If the partition is NOT partitioned by DATE or DATETIME, data cache is allowed
     *  - If the partition is partitioned by DATE or DATETIME:
     *    - if the partition's end value (of type DATE/DATETIME) is within the last "datacache.partition_duration"
     *      duration, allow data cache for the partition.
     *    - otherwise, disallow the data cache for the partition
     *
     * @param partition the partition to check. the partition must belong to this table.
     * @return true if the partition is enabled for the data cache, false otherwise
     */
    public boolean isEnableFillDataCache(Partition partition) {
        try {
            return isEnableFillDataCacheImpl(Objects.requireNonNull(partition, "partition is null"));
        } catch (AnalysisException ignored) {
            return true;
        }
    }

    private boolean isEnableFillDataCacheImpl(Partition partition) throws AnalysisException {
        PeriodDuration cacheDuration = getTableProperty().getDataCachePartitionDuration();
        if (cacheDuration != null && getPartitionInfo().isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) getPartitionInfo();
            Range<PartitionKey> partitionRange = rangePartitionInfo.getRange(partition.getId());
            Range<PartitionKey> dataCacheRange;
            if (rangePartitionInfo.isPartitionedBy(PrimitiveType.DATETIME)) {
                LocalDateTime upper = LocalDateTime.now();
                LocalDateTime lower = upper.minus(cacheDuration);
                dataCacheRange = Range.openClosed(PartitionKey.ofDateTime(lower), PartitionKey.ofDateTime(upper));
                return partitionRange.isConnected(dataCacheRange);
            } else if (rangePartitionInfo.isPartitionedBy(PrimitiveType.DATE)) {
                LocalDate upper = LocalDate.now();
                LocalDate lower = upper.minus(cacheDuration);
                dataCacheRange = Range.openClosed(PartitionKey.ofDate(lower), PartitionKey.ofDate(upper));
                return partitionRange.isConnected(dataCacheRange);
            } else {
                // If the table was not partitioned by DATE/DATETIME, ignore the property "datacache.partition_duration" and
                // enable data cache by default.
                return true;
            }
        }
        return true;
    }
<<<<<<< HEAD

    @Override
    public void inferDistribution(DistributionInfo info) throws DdlException {
        if (info.getBucketNum() == 0) {
            throw new DdlException("You must specify 'BUCKETS' in shared_data mode");
        }
    }
=======
>>>>>>> 4cd0521e7 ([BugFix]Update auto infer bucket num for cloudnative table, same with olap table (#27421))
}
