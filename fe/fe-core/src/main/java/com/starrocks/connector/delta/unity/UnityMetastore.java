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

import com.google.common.base.Strings;
import com.starrocks.catalog.Database;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link IMetastore} implementation backed by the Databricks Unity Catalog REST API. Used as the
 * delegate inside {@link UnityBackedDeltaMetastore} to list schemas/tables and resolve per-table
 * storage locations (with optional vended credentials).
 */
public class UnityMetastore implements IMetastore {
    private static final Logger LOG = LogManager.getLogger(UnityMetastore.class);
    private static final String DELTA_FORMAT = "DELTA";

    private final UnityCatalogClient client;
    private final UnityCatalogProperties properties;

    public UnityMetastore(UnityCatalogClient client, UnityCatalogProperties properties) {
        this.client = Objects.requireNonNull(client, "client");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    @Override
    public List<String> getAllDatabaseNames() {
        return client.listSchemas(properties.getUcCatalogName()).stream()
                .map(s -> s.name)
                .filter(n -> !Strings.isNullOrEmpty(n))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllTableNames(String dbName) {
        return client.listTables(properties.getUcCatalogName(), dbName).stream()
                .filter(t -> isDelta(t.dataSourceFormat))
                .map(t -> t.name)
                .filter(n -> !Strings.isNullOrEmpty(n))
                .collect(Collectors.toList());
    }

    @Override
    public Database getDb(String dbName) {
        // Load the single schema; Unity Catalog does not expose a bulk-get-schema-metadata call so
        // we reuse listSchemas' filtering.
        return client.listSchemas(properties.getUcCatalogName()).stream()
                .filter(s -> dbName.equalsIgnoreCase(s.name))
                .findFirst()
                .map(s -> new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                        dbName,
                        s.storageLocation == null ? "" : s.storageLocation))
                .orElse(null);
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        String fullName = fullName(dbName, tableName);
        UnityCatalogTypes.TableInfo info;
        try {
            info = client.getTable(fullName);
        } catch (StarRocksConnectorException e) {
            LOG.error("Failed to load Unity Catalog table {}", fullName, e);
            throw e;
        }
        if (info == null || Strings.isNullOrEmpty(info.storageLocation)) {
            throw new StarRocksConnectorException(
                    "Unity Catalog table %s is missing a storage_location; only external Delta tables " +
                            "with a resolvable location are supported", fullName);
        }
        if (!isDelta(info.dataSourceFormat)) {
            throw new StarRocksConnectorException(
                    "Unity Catalog table %s has unsupported data_source_format=%s; only DELTA is supported",
                    fullName, info.dataSourceFormat);
        }

        CloudConfiguration cloudConfiguration = resolveCloudConfiguration(info);
        long createTime = info.createdAt != null ? info.createdAt : 0L;
        return new MetastoreTable(dbName, tableName, info.storageLocation, createTime, cloudConfiguration);
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        return client.tableExists(fullName(dbName, tableName));
    }

    private CloudConfiguration resolveCloudConfiguration(UnityCatalogTypes.TableInfo info) {
        if (!properties.isVendedCredentialsEnabled()) {
            return null;
        }
        if (Strings.isNullOrEmpty(info.tableId)) {
            LOG.warn("Unity Catalog table {} has no table_id; cannot vend credentials, falling back " +
                    "to catalog-level credentials", info.fullName);
            return null;
        }
        try {
            UnityCatalogTypes.TemporaryTableCredentials creds =
                    client.getTemporaryTableCredentials(info.tableId, "READ");
            CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                    info.storageLocation, properties.getAwsRegion());
            // An empty/DEFAULT CloudConfiguration means translation produced nothing usable; return
            // null so the shared DeltaLakeMetastore code skips applyToConfiguration and the
            // catalog-level configuration (already baked into HdfsEnvironment) remains in effect.
            if (cc == null || cc.getCloudType() == com.starrocks.credential.CloudType.DEFAULT) {
                return null;
            }
            return cc;
        } catch (StarRocksConnectorException e) {
            LOG.warn("Failed to vend credentials for Unity Catalog table {}: {}. Falling back to " +
                    "catalog-level credentials.", info.fullName, e.getMessage());
            return null;
        }
    }

    private String fullName(String dbName, String tableName) {
        return properties.getUcCatalogName() + "." + dbName + "." + tableName;
    }

    private static boolean isDelta(String dataSourceFormat) {
        return dataSourceFormat != null && DELTA_FORMAT.equalsIgnoreCase(dataSourceFormat.trim());
    }

    // Visible for testing.
    static CloudConfiguration emptyCloudConfigurationForTest() {
        return CloudConfigurationFactory.buildCloudConfigurationForStorage(Collections.unmodifiableMap(new HashMap<>()));
    }
}
