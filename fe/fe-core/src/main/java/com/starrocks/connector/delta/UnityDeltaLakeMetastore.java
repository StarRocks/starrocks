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

import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.qe.ConnectContext;
import io.delta.kernel.Table;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Protocol;
import org.apache.hadoop.conf.Configuration;

import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/** Owns one statement's Catalog identity. Each loaded table has its own fixed storage credentials. */
public final class UnityDeltaLakeMetastore extends DeltaLakeMetastore {
    private static final Set<String> SUPPORTED_READER_FEATURES =
            Set.of("columnMapping", "deletionVectors", "timestampNtz", "v2Checkpoint");

    public UnityDeltaLakeMetastore(String catalogName, Map<String, String> properties, Configuration configuration,
                                   ConnectContext context) {
        this(catalogName, properties, configuration, new UnityCatalogClient(properties, authToken(context)));
    }

    public UnityDeltaLakeMetastore(String catalogName, Map<String, String> properties, Configuration configuration,
                                   ConnectContext context, HttpClient httpClient) {
        this(catalogName, properties, configuration, new UnityCatalogClient(properties, authToken(context), httpClient));
    }

    private UnityDeltaLakeMetastore(String catalogName, Map<String, String> properties, Configuration configuration,
                                    UnityCatalogClient client) {
        super(catalogName, client, new Configuration(configuration), uncachedProperties(properties));
    }

    private static String authToken(ConnectContext context) {
        if (context == null) {
            throw new StarRocksConnectorException("Unity Catalog requires a native authenticated session");
        }
        return context.getAuthToken();
    }

    private static DeltaLakeCatalogProperties uncachedProperties(Map<String, String> properties) {
        Map<String, String> queryProperties = new HashMap<>(properties);
        queryProperties.put(DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_TABLE_CACHE, "false");
        queryProperties.put(DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, "false");
        queryProperties.put(DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, "false");
        return new DeltaLakeCatalogProperties(queryProperties);
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        return delegate.getMetastoreTable(dbName, tableName);
    }

    @Override
    public DeltaLakeTable getTable(String dbName, String tableName) {
        DeltaLakeTable table = super.getTable(dbName, tableName);
        table.setUnityCatalogTable(true);
        return table;
    }

    @Override
    public DeltaLakeSnapshot getLatestSnapshot(String dbName, String tableName) {
        MetastoreTable table = getMetastoreTable(dbName, tableName);
        if (table == null) {
            throw new StarRocksConnectorException("Unity Catalog Delta table does not exist");
        }
        Configuration tableConfiguration = new Configuration(hdfsConfiguration);
        table.getCloudConfiguration().applyToConfiguration(tableConfiguration);
        DeltaLakeEngine engine = DeltaLakeEngine.create(tableConfiguration, properties, null, null);
        SnapshotImpl snapshot;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getSnapshot")) {
            snapshot = (SnapshotImpl) Table.forPath(engine, table.getTableLocation()).getLatestSnapshot(engine);
        } catch (RuntimeException e) {
            // Storage exceptions may include a signed URL or configuration containing the table's SAS.
            throw new StarRocksConnectorException("Unable to read Unity Catalog Delta snapshot with the fixed READ credentials");
        }
        Protocol protocol = snapshot.getProtocol();
        DeltaUtils.checkProtocolAndMetadata(protocol, snapshot.getMetadata());
        if (protocol.getMinReaderVersion() > 3
                || !SUPPORTED_READER_FEATURES.containsAll(protocol.getReaderFeatures())
                || !"parquet".equalsIgnoreCase(snapshot.getMetadata().getFormat().getProvider())) {
            throw new StarRocksConnectorException("Unsupported Unity Catalog Delta reader protocol or file format");
        }
        return new DeltaLakeSnapshot(dbName, tableName, engine, snapshot, table);
    }
}
