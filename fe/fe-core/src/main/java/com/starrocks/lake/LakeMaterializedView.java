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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for StarRocks lake materialized view
 * todo: Rename to CloudNativeMaterializedView
 */
public class LakeMaterializedView extends MaterializedView {

    private static final Logger LOG = LogManager.getLogger(LakeMaterializedView.class);

    public LakeMaterializedView() {
        this.type = TableType.CLOUD_NATIVE_MATERIALIZED_VIEW;
    }

    public LakeMaterializedView(long id, long dbId, String mvName, List<Column> baseSchema, KeysType keysType,
                                PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                                MvRefreshScheme refreshScheme) {
        super(id, dbId, mvName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, refreshScheme);
        this.type = TableType.CLOUD_NATIVE_MATERIALIZED_VIEW;
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
    public MaterializedView selectiveCopy(Collection<String> reservedPartitions, boolean resetState,
                                          MaterializedIndex.IndexExtState extState) {
        LakeMaterializedView copied = DeepCopy.copyWithGson(this, LakeMaterializedView.class);
        if (copied == null) {
            LOG.warn("failed to copy lake table: {}", getName());
            return null;
        }
        return (MaterializedView) selectiveCopyInternal(copied, reservedPartitions, resetState, extState);
    }

    public static LakeMaterializedView read(DataInput in) throws IOException {
        // type is already read in Table
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeMaterializedView.class);
    }

    @Override
    public boolean isDeleteRetryable() {
        return true;
    }

    @Override
    public boolean delete(long dbId, boolean replay) {
        return LakeTableHelper.deleteTable(dbId, this, replay);
    }

    @Override
    public boolean deleteFromRecycleBin(long dbId, boolean replay) {
        return LakeTableHelper.deleteTableFromRecycleBin(dbId, this, replay);
    }

    @Override
    public AlterJobV2Builder alterTable() {
        return LakeTableHelper.alterTable(this);
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
    protected void appendUniqueProperties(StringBuilder sb) {
        Preconditions.checkNotNull(sb);

        Map<String, String> storageProperties = getProperties();

        // datacache.enable
        sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                .append(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE).append("\" = \"");
        sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE)).append("\"");

        // allow_sync_write_back
        sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                .append(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK).append("\" = \"");
        sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK)).append("\"");

        // storage_volume
        String volume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeNameOfTable(id);
        sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(
                PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME).append("\" = \"").append(volume).append("\"");
    }

    @Override
    public String getComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return comment;
        }
        return TableType.MATERIALIZED_VIEW.name();
    }

    @Override
    public String getDisplayComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return CatalogUtils.addEscapeCharacter(comment);
        }
        return TableType.MATERIALIZED_VIEW.name();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        if (getMaxColUniqueId() <= 0) {
            setMaxColUniqueId(LakeTableHelper.restoreColumnUniqueId(this));
        }
    }
}
