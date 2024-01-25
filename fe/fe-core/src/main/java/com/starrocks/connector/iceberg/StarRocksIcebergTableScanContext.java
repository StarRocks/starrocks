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

import com.google.common.cache.Cache;
import com.starrocks.connector.PlanMode;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;

import java.util.List;
import java.util.Set;

public class StarRocksIcebergTableScanContext {
    private final PlanMode planMode;
    private String catalogName;
    private String dbName;
    private String tableName;

    private boolean dataFileCacheWithMetrics;
    private boolean forceEnableManifestCacheWithoutMetricsWithDeleteFile;
    private Cache<String, Set<DataFile>> dataFileCache;
    private Cache<String, Set<DeleteFile>> deleteFileCache;
    private int localParallelism;
    private long localPlanningMaxSlotSize;
    private List<ManifestFile> toRefreshManifests;
    private boolean onlyReadCache;

    public StarRocksIcebergTableScanContext(String catalogName, String dbName, String tableName, PlanMode planMode) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.planMode = planMode;
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

    public PlanMode getPlanMode() {
        return planMode;
    }

    public boolean isDataFileCacheWithMetrics() {
        return dataFileCacheWithMetrics;
    }

    public Cache<String, Set<DataFile>> getDataFileCache() {
        return dataFileCache;
    }

    public Cache<String, Set<DeleteFile>> getDeleteFileCache() {
        return deleteFileCache;
    }

    public int getLocalParallelism() {
        return localParallelism;
    }

    public long getLocalPlanningMaxSlotSize() {
        return localPlanningMaxSlotSize;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDataFileCacheWithMetrics(boolean dataFileCacheWithMetrics) {
        this.dataFileCacheWithMetrics = dataFileCacheWithMetrics;
    }

    public void setDataFileCache(Cache<String, Set<DataFile>> dataFileCache) {
        this.dataFileCache = dataFileCache;
    }

    public void setDeleteFileCache(Cache<String, Set<DeleteFile>> deleteFileCache) {
        this.deleteFileCache = deleteFileCache;
    }

    public void setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
    }

    public void setLocalPlanningMaxSlotSize(long localPlanningMaxSlotSize) {
        this.localPlanningMaxSlotSize = localPlanningMaxSlotSize;
    }

    public boolean isForceEnableManifestCacheWithoutMetricsWithDeleteFile() {
        return forceEnableManifestCacheWithoutMetricsWithDeleteFile;
    }

    public void setForceEnableManifestCacheWithoutMetricsWithDeleteFile(
            boolean forceEnableManifestCacheWithoutMetricsWithDeleteFile) {
        this.forceEnableManifestCacheWithoutMetricsWithDeleteFile = forceEnableManifestCacheWithoutMetricsWithDeleteFile;
    }

    public boolean isOnlyReadCache() {
        return onlyReadCache;
    }

    public void setOnlyReadCache(boolean onlyReadCache) {
        this.onlyReadCache = onlyReadCache;
    }

    public List<ManifestFile> getToRefreshManifests() {
        return toRefreshManifests;
    }

    public void setToRefreshManifests(List<ManifestFile> toRefreshManifests) {
        this.toRefreshManifests = toRefreshManifests;
    }
}
