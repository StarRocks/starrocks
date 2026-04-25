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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private final UnityCatalogApi client;
    private final UnityCatalogProperties properties;
    // The Unity Catalog metastore is pinned to a single AWS region at creation time and Databricks
    // does not support cross-region migration, so the region returned by metastore_summary is
    // immutable for the lifetime of a catalog handle. We memoize it on first successful lookup
    // and skip the REST call thereafter. {@code null} means "not fetched yet (or last attempt
    // failed)" so a transient 5xx naturally retries on the next query.
    private volatile String cachedRegion;

    public UnityMetastore(UnityCatalogApi client, UnityCatalogProperties properties) {
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
        UnityCatalogTypes.TableInfo info = fetchTableInfo(dbName, tableName);
        long createTime = info.createdAt != null ? info.createdAt : 0L;
        return new MetastoreTable(dbName, tableName, info.storageLocation, createTime);
    }

    public UnityCatalogTypes.TableInfo fetchTableInfo(String dbName, String tableName) {
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
        return info;
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        return client.tableExists(fullName(dbName, tableName));
    }

    public CloudConfiguration resolveCloudConfiguration(UnityCatalogTypes.TableInfo info) {
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
            // Region is only consumed by the AWS branch of the translator; resolving it for Azure
            // or GCP would burn a metastore_summary call we do not need and -- worse -- would
            // throw a misleading "AWS metastore" error if UC ever returned no region.
            String awsRegion = (creds != null && creds.awsTempCredentials != null) ? resolveAwsRegion() : null;
            CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                    info.storageLocation, awsRegion);
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

    /**
     * Resolve the AWS region for vended credentials.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>Operator override via {@link UnityCatalogProperties#getAwsRegionOverride()} if
     *       set -- trusted as-is, no REST call.</li>
     *   <li>Otherwise look up the region from Unity Catalog's {@code metastore_summary}
     *       endpoint exactly once per catalog handle. Region is immutable for the lifetime
     *       of a UC metastore so memoizing on first success avoids any further REST calls.
     *       Throws when UC does not expose a region: a Databricks AWS metastore always
     *       returns one, and silently defaulting to the AWS SDK's {@code us-east-1} would
     *       otherwise cause hard-to-diagnose 403s for buckets in other regions. Transient
     *       REST failures propagate too -- the outer caller treats them like a credential
     *       vend failure and {@link #cachedRegion} stays {@code null} so the next query
     *       retries.</li>
     * </ol>
     */
    private String resolveAwsRegion() {
        String override = properties.getAwsRegionOverride();
        if (override != null) {
            return override;
        }
        if (cachedRegion != null) {
            return cachedRegion;
        }
        UnityCatalogTypes.MetastoreSummary summary = client.getMetastoreSummary();
        if (summary == null || Strings.isNullOrEmpty(summary.region)) {
            throw new StarRocksConnectorException(
                    "Unity Catalog metastore_summary did not return an AWS region for catalog " +
                            "'%s'. Vended credentials require a region; either verify that the " +
                            "configured catalog points at a Databricks AWS metastore, or set " +
                            "'%s' explicitly on the catalog.",
                    properties.getUcCatalogName(), UnityCatalogProperties.UNITY_CATALOG_AWS_REGION);
        }
        cachedRegion = summary.region.trim();
        return cachedRegion;
    }

    /**
     * Drop any cached client-side state for {@code (dbName, tableName)}. Called from
     * {@link UnityBackedDeltaMetastore#refreshTable(String, String)} so a manual {@code REFRESH
     * EXTERNAL TABLE} also flushes the {@link CachingUnityCatalogClient} {@code TableInfo} and
     * vended-credentials entries. The default {@link UnityCatalogApi#invalidate(String)}
     * implementation is a no-op for the direct REST client, so this is harmless when no caching
     * decorator is in front of it.
     */
    public void invalidateTable(String dbName, String tableName) {
        client.invalidate(fullName(dbName, tableName));
    }

    private String fullName(String dbName, String tableName) {
        return properties.getUcCatalogName() + "." + dbName + "." + tableName;
    }

    private static boolean isDelta(String dataSourceFormat) {
        return dataSourceFormat != null && DELTA_FORMAT.equalsIgnoreCase(dataSourceFormat.trim());
    }

    // Visible for testing: lets wiring tests assert which UnityCatalogApi impl was injected.
    UnityCatalogApi getClientForTest() {
        return client;
    }
}
