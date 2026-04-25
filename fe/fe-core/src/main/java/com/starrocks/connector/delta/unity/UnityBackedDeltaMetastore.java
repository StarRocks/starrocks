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

package com.starrocks.connector.delta.unity;

import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.delta.DeltaLakeMetastore;
import com.starrocks.connector.delta.DeltaLakeSnapshot;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.credential.CloudConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link DeltaLakeMetastore} variant whose {@code IMetastore} delegate is a
 * {@link UnityMetastore}. The parent class continues to handle Delta Kernel snapshot loading,
 * partition-key extraction, and checkpoint/JSON caching; this class only re-attaches per-table
 * vended cloud credentials to the {@link DeltaLakeTable} that the planner sees.
 *
 * <p>Per-table caching of {@code TableInfo} and vended {@code TemporaryTableCredentials} is the
 * job of {@link CachingUnityCatalogClient}, not this class. We resolve through
 * {@link UnityMetastore} on every call and rely on the client decorator to absorb the REST
 * traffic.</p>
 */
public class UnityBackedDeltaMetastore extends DeltaLakeMetastore {
    private static final Logger LOG = LogManager.getLogger(UnityBackedDeltaMetastore.class);

    private final UnityMetastore unityMetastore;
    private final UnityCatalogProperties unityProperties;

    public UnityBackedDeltaMetastore(String catalogName,
                                     UnityMetastore delegate,
                                     Configuration hdfsConfiguration,
                                     DeltaLakeCatalogProperties deltaLakeCatalogProperties,
                                     UnityCatalogProperties unityProperties) {
        super(catalogName, delegate, hdfsConfiguration, deltaLakeCatalogProperties);
        this.unityMetastore = delegate;
        this.unityProperties = unityProperties;
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        return unityMetastore.getMetastoreTable(dbName, tableName);
    }

    @Override
    public CloudConfiguration resolveTableCloudConfiguration(String dbName, String tableName) {
        if (!isVendedCredentialsEnabled()) {
            return null;
        }
        try {
            return unityMetastore.resolveCloudConfiguration(
                    unityMetastore.fetchTableInfo(dbName, tableName));
        } catch (RuntimeException e) {
            LOG.warn("UC credential vending failed for {}.{}", dbName, tableName, e);
            return null;
        }
    }

    @Override
    public DeltaLakeTable getTable(String dbName, String tableName) {
        CloudConfiguration cc = resolveTableCloudConfiguration(dbName, tableName);
        DeltaLakeSnapshot snapshot = getLatestSnapshot(dbName, tableName, cc);
        DeltaLakeTable t = DeltaUtils.convertDeltaSnapshotToSRTable(getCatalogName(), snapshot);
        if (t != null && cc != null) {
            t.setCloudConfiguration(cc);
        }
        return t;
    }

    @Override
    public boolean isVendedCredentialsEnabled() {
        return unityProperties != null && unityProperties.isVendedCredentialsEnabled();
    }

    /**
     * Bypass the catalog-level snapshot cache when vended credentials are active and the
     * Unity client cache is effectively off (disabled or TTL=0). When the client cache is
     * configured, the snapshot cache TTL is clamped (in {@code DeltaLakeInternalMgr}) to the
     * Unity cache TTL so a cached snapshot's baked-in credentials never outlive the cache
     * entry; in that mode we keep the snapshot cache in play.
     */
    @Override
    public boolean isSnapshotCacheBypassed() {
        if (!isVendedCredentialsEnabled()) {
            return false;
        }
        return unityProperties == null || !unityProperties.isCacheEnabled() || unityProperties.getCacheTtlSec() == 0L;
    }

    /**
     * Drop the Unity client's cached {@code TableInfo} and vended-credentials entries for this
     * table. Called from {@link com.starrocks.connector.delta.CachingDeltaLakeMetastore#refreshTable}
     * which handles {@code REFRESH EXTERNAL TABLE}.
     */
    @Override
    public void refreshTable(String dbName, String tableName) {
        unityMetastore.invalidateTable(dbName, tableName);
    }
}
