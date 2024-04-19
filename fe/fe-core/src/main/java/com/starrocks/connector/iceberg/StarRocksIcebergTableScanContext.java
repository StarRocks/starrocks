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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

import java.util.Set;

public class StarRocksIcebergTableScanContext {
    private boolean dataFileCacheWithMetrics;
    private Cache<String, Set<DataFile>> dataFileCache;
    private Cache<String, Set<DeleteFile>> deleteFileCache;

    public StarRocksIcebergTableScanContext() {
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

}
