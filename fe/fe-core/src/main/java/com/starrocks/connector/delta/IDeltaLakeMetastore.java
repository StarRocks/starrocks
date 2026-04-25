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

package com.starrocks.connector.delta;

import com.starrocks.catalog.Table;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.memory.MemoryTrackable;

import java.util.List;

public interface IDeltaLakeMetastore extends IMetastore, MemoryTrackable {
    String getCatalogName();

    default boolean isVendedCredentialsEnabled() {
        return false;
    }

    /**
     * Returns {@code true} when the operator has opted out of the catalog-level snapshot cache
     * for this metastore. {@link CachingDeltaLakeMetastore#getTable} consults this and falls
     * straight through to {@link #getTable} on the delegate, so a fresh snapshot (and fresh
     * credentials) is loaded for every query.
     */
    default boolean isSnapshotCacheBypassed() {
        return false;
    }

    /**
     * Per-table cloud configuration to attach to the SRTable when the snapshot cache returns a
     * hit. Without this hook the cached path in {@link CachingDeltaLakeMetastore#getTable} would
     * skip the per-table vended credentials that {@link DeltaLakeMetastore#getTable} normally
     * attaches via its subclass override, causing the planner to ship credential-less scan
     * ranges to the BE (resulting in S3 403s). Implementations that do not vend per-table
     * credentials simply return {@code null}.
     */
    default CloudConfiguration resolveTableCloudConfiguration(String dbName, String tableName) {
        return null;
    }

    /**
     * Drop any per-table state the implementation holds for {@code (dbName, tableName)}.
     * Invoked from the catalog-level cache layer so that {@code REFRESH EXTERNAL TABLE} also
     * flushes downstream caches (e.g. Unity Catalog vended credentials, Unity client
     * {@code TableInfo} entries). The default is a no-op for backends that do not maintain
     * their own per-table cache.
     */
    default void refreshTable(String dbName, String tableName) {
    }

    Table getTable(String dbName, String tableName);

    List<String> getPartitionKeys(String dbName, String tableName);

    DeltaLakeSnapshot getLatestSnapshot(String dbName, String tableName);
}
