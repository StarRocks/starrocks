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
import com.starrocks.qe.ConnectContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

import java.util.Set;

public class StarRocksIcebergTableScanContext {
    private final PlanMode planMode;
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private boolean dataFileCacheWithMetrics;
    private Cache<String, Set<DataFile>> dataFileCache;
    private Cache<String, Set<DeleteFile>> deleteFileCache;
    private boolean onlyReadCache;
    private int localParallelism;
    private long localPlanningMaxSlotSize;
    private boolean enableCacheDataFileIdentifierColumnMetrics;
    private ConnectContext connectContext;

    public StarRocksIcebergTableScanContext(String catalogName, String dbName, String tableName, PlanMode planMode) {
        this(catalogName, dbName, tableName, planMode, null);
    }

    public StarRocksIcebergTableScanContext(String catalogName, String dbName, String tableName,
                                            PlanMode planMode, ConnectContext connectContext) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.planMode = planMode;
        this.connectContext = connectContext;
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

    public boolean isDataFileCacheWithMetrics() {
        return dataFileCacheWithMetrics;
    }

    public Cache<String, Set<DataFile>> getDataFileCache() {
        return dataFileCache;
    }

    public Cache<String, Set<DeleteFile>> getDeleteFileCache() {
        return deleteFileCache;
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

    public boolean isOnlyReadCache() {
        return onlyReadCache;
    }

    public void setOnlyReadCache(boolean onlyReadCache) {
        this.onlyReadCache = onlyReadCache;
    }

    public PlanMode getPlanMode() {
        return planMode;
    }

    public int getLocalParallelism() {
        return localParallelism;
    }

    public void setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
    }

    public long getLocalPlanningMaxSlotSize() {
        return localPlanningMaxSlotSize;
    }

    public void setLocalPlanningMaxSlotSize(long localPlanningMaxSlotSize) {
        this.localPlanningMaxSlotSize = localPlanningMaxSlotSize;
    }

    public boolean isEnableCacheDataFileIdentifierColumnMetrics() {
        return enableCacheDataFileIdentifierColumnMetrics;
    }

    public void setEnableCacheDataFileIdentifierColumnMetrics(boolean enableCacheDataFileIdentifierColumnMetrics) {
        this.enableCacheDataFileIdentifierColumnMetrics = enableCacheDataFileIdentifierColumnMetrics;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }
}
