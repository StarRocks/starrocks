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

package com.starrocks.connector.iceberg.rest;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import com.starrocks.connector.share.iceberg.IcebergAwsClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergApiConverter.convertDbNameToNamespace;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;

public class IcebergRESTCatalog implements IcebergCatalog {

    private static final Logger LOG = LogManager.getLogger(IcebergRESTCatalog.class);
    public static final String KEY_VENDED_CREDENTIALS_ENABLED = "vended-credentials-enabled";
    public static final String KEY_NESTED_NAMESPACE_ENABLED = "rest.nested-namespace-enabled";

    private static final long AUTH_RECOVERY_MIN_INTERVAL_MS = 60_000L;

    private String catalogName = null;
    private final Configuration conf;
    // swapped under synchronized on auth recovery; read lock-free everywhere else
    private final AtomicReference<RESTCatalog> delegate = new AtomicReference<>();
    private Map<String, String> restCatalogProperties;
    private final boolean nestedNamespaceEnabled;
    private final boolean authRecoveryEnabled;
    private long lastAuthRecoveryMillis;


    public IcebergRESTCatalog(String name, Configuration conf, Map<String, String> properties) {
        this.catalogName = name;
        this.conf = conf;
        Map<String, String> copiedProperties = Maps.newHashMap(properties);

        properties.forEach((key, value) -> {
            String newKey = key.toLowerCase();
            if (newKey.startsWith(ICEBERG_CUSTOM_PROPERTIES_PREFIX)) {
                newKey = newKey.substring(ICEBERG_CUSTOM_PROPERTIES_PREFIX.length());
                copiedProperties.put(newKey, value);
            }
        });

        copiedProperties.put(CatalogProperties.FILE_IO_IMPL, IcebergCachingFileIO.class.getName());
        copiedProperties.put(CatalogProperties.METRICS_REPORTER_IMPL, IcebergMetricsReporter.class.getName());

        boolean enableVendedCredentials =
                Boolean.parseBoolean(copiedProperties.getOrDefault(KEY_VENDED_CREDENTIALS_ENABLED, "true"));
        if (enableVendedCredentials) {
            copiedProperties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
        }
        copiedProperties.put(AwsProperties.CLIENT_FACTORY, IcebergAwsClientFactory.class.getName());

        nestedNamespaceEnabled = PropertyUtil.propertyAsBoolean(copiedProperties, KEY_NESTED_NAMESPACE_ENABLED, false);
        // setup oauth2
        OAuth2SecurityConfig securityConfig = OAuth2SecurityConfigBuilder.build(copiedProperties);
        OAuth2SecurityProperties securityProperties = new OAuth2SecurityProperties(securityConfig);
        copiedProperties.putAll(securityProperties.get());
        this.restCatalogProperties = copiedProperties;

        // only an OAuth2 client credential lets us mint a new token; a bare bearer token cannot be renewed by the FE
        authRecoveryEnabled = copiedProperties.containsKey(OAuth2Properties.CREDENTIAL);

        try {
            delegate.set((RESTCatalog) CatalogUtil.loadCatalog(RESTCatalog.class.getName(), name, copiedProperties, conf));
        } catch (Exception re) {
            LOG.error("Failed to rest load catalog", re);
            throw new StarRocksConnectorException("Failed to load rest catalog",
                    new RuntimeException("Failed to load rest catalog, exception: " + re.getMessage(), re));
        }

    }

    // for ut
    public IcebergRESTCatalog(RESTCatalog restCatalog, Configuration conf) {
        this.delegate.set(restCatalog);
        this.conf = conf;
        this.nestedNamespaceEnabled = false;
        this.restCatalogProperties = Maps.newHashMap();
        this.authRecoveryEnabled = false;
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.REST_CATALOG;
    }

    @Override
    public Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
        try {
            return withAuthRecovery(() ->
                    delegate.get().loadTable(TableIdentifier.of(convertDbNameToNamespace(dbName), tableName)));
        } catch (RESTException re) {
            LOG.error("Failed to load table using REST Catalog, for dbName {} tableName {}", dbName, tableName, re);
            throw new StarRocksConnectorException("Failed to load table using REST Catalog",
                    new RuntimeException("Failed to load table using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public boolean tableExists(String dbName, String tableName) throws StarRocksConnectorException {
        try {
            return withAuthRecovery(() ->
                    delegate.get().tableExists(TableIdentifier.of(convertDbNameToNamespace(dbName), tableName)));
        } catch (RESTException re) {
            LOG.error("Failed to check tableExists REST Catalog, for dbName {} tableName {}", dbName, tableName, re);
            throw new StarRocksConnectorException("Failed to check tableExists using REST Catalog",
                    new RuntimeException("Failed to check tableExists using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public List<String> listAllDatabases() {
        try {
            if (nestedNamespaceEnabled) {
                return withAuthRecovery(() -> listNamespaces(Namespace.empty()));
            } else {
                return withAuthRecovery(() -> delegate.get().listNamespaces().stream().map(ns -> ns.level(0))
                            .collect(Collectors.toList()));
            }
        } catch (RESTException re) {
            LOG.error("Failed to list databases using REST Catalog ", re);
            throw new StarRocksConnectorException("Failed to list all databases using REST Catalog",
                    new RuntimeException("Failed to list all databases using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    private List<String> listNamespaces(Namespace parent) {
        return delegate.get().listNamespaces(parent).stream().
                flatMap(child -> Stream.concat(Stream.of(child.toString()), listNamespaces(child).stream()))
                .collect(toImmutableList());
    }

    @Override
    public void createDB(String dbName, Map<String, String> properties) {
        Map<String, String> dbProperties = properties == null ? new HashMap<>() : properties;
        for (Map.Entry<String, String> entry : dbProperties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(LOCATION_PROPERTY)) {
                try {
                    URI uri = new Path(value).toUri();
                    FileSystem fileSystem = FileSystem.get(uri, conf);
                    fileSystem.exists(new Path(value));
                } catch (Exception e) {
                    LOG.error("Invalid location URI: {}", value, e);
                    throw new StarRocksConnectorException("Invalid location URI: %s. msg: %s", value, e.getMessage());
                }
            } else {
                throw new IllegalArgumentException("Unrecognized property: " + key);
            }
        }
        try {
            runWithAuthRecovery(() -> delegate.get().createNamespace(convertDbNameToNamespace(dbName), dbProperties));
        } catch (RESTException re) {
            LOG.error("Failed to list create namespace using REST Catalog, for dbName {}", dbName, re);
            throw new StarRocksConnectorException("Failed to create namespace using REST Catalog",
                    new RuntimeException("Failed to create namespace using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public void dropDB(String dbName) throws MetaNotFoundException {
        Database database;
        try {
            database = getDB(dbName);
        } catch (Exception e) {
            LOG.error("Failed to access database {}", dbName, e);
            throw new MetaNotFoundException("Failed to access database " + dbName);
        }

        if (database == null) {
            throw new MetaNotFoundException("Not found database " + dbName);
        }

        String dbLocation = database.getLocation();
        if (Strings.isNullOrEmpty(dbLocation)) {
            throw new MetaNotFoundException("Database location is empty");
        }
        try {
            runWithAuthRecovery(() -> delegate.get().dropNamespace(convertDbNameToNamespace(dbName)));
        } catch (RESTException re) {
            LOG.error("Failed to drop database using REST Catalog, for dbName {}", dbName, re);
            throw new StarRocksConnectorException("Failed to drop database using REST Catalog",
                    new RuntimeException("Failed to drop database using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public Database getDB(String dbName) {
        try {
            Map<String, String> dbMeta = withAuthRecovery(() ->
                    delegate.get().loadNamespaceMetadata(convertDbNameToNamespace(dbName)));
            return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName, dbMeta.get(LOCATION_PROPERTY));
        } catch (RESTException re) {
            LOG.error("Failed to get database using REST Catalog, for dbName {}", dbName, re);
            throw new StarRocksConnectorException("Failed to get database using REST Catalog",
                    new RuntimeException("Failed to get database using REST Catalog exception:" + re.getMessage(), re));
        }
    }

    @Override
    public List<String> listTables(String dbName) {
        Namespace ns = convertDbNameToNamespace(dbName);
        List<TableIdentifier> tableIdentifiers = null;
        try {
            tableIdentifiers = withAuthRecovery(() -> delegate.get().listTables(ns));
        } catch (RESTException re) {
            LOG.error("Failed to list tables using REST Catalog, for dbName {}", dbName, re);
            // creating a new RuntimeException with the custom message, as in the upstream code, it picks the `exception.getCause().getMessage()`
            // and keeping the re, so that Stacktrace will have the upstream exception.
            throw new StarRocksConnectorException("Failed to list tables using REST Catalog",
                    new RuntimeException("Failed to list tables using REST Catalog, exception:" + re.getMessage(), re));
        }

        List<TableIdentifier> viewIdentifiers = new ArrayList<>();
        try {
            viewIdentifiers = withAuthRecovery(() -> delegate.get().listViews(ns));
        } catch (BadRequestException e) {
            LOG.warn("Failed to list views from {} namespace. Perhaps the server side does not implement the interface. " +
                    "Ask the user to check it", ns, e);
        } catch (RESTException re) {
            LOG.error("Failed to list views using REST Catalog, for dbName {}", dbName, re);
            throw new StarRocksConnectorException("Failed to list views using REST Catalog",
                    new RuntimeException("Failed to list views using REST Catalog, exception: " + re.getMessage(), re));
        }

        final List<TableIdentifier> finalIdentifiers = ImmutableList.<TableIdentifier>builder()
                .addAll(tableIdentifiers)
                .addAll(viewIdentifiers)
                .build();

        return finalIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public boolean createTable(
            String dbName,
            String tableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties) {

        Table nativeTable = null;
        try {
            nativeTable = withAuthRecovery(() ->
                    delegate.get().buildTable(TableIdentifier.of(convertDbNameToNamespace(dbName), tableName), schema)
                            .withLocation(location)
                            .withPartitionSpec(partitionSpec)
                            .withProperties(properties)
                            .create());
        } catch (RESTException re) {
            LOG.error("Failed to create table using REST Catalog, for dbName {} schema {} tableName {}",
                    dbName, schema, tableName, re);
            throw new StarRocksConnectorException("Failed to create table using REST catalog",
                    new RuntimeException("Failed to create table using REST catalog, exception: " + re.getMessage(), re));
        }

        return nativeTable != null;
    }

    @Override
    public boolean dropTable(String dbName, String tableName, boolean purge) {
        try {
            return withAuthRecovery(() ->
                    delegate.get().dropTable(TableIdentifier.of(convertDbNameToNamespace(dbName), tableName), purge));
        } catch (RESTException re) {
            LOG.error("Failed to drop table using REST Catalog, for dbName {} tableName {}", dbName, tableName, re);
            throw new StarRocksConnectorException("Failed to drop table using REST Catalog",
                    new RuntimeException("Failed to drop table using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public void renameTable(String dbName, String tblName, String newTblName) throws StarRocksConnectorException {
        Namespace ns = convertDbNameToNamespace(dbName);

        try {
            runWithAuthRecovery(() ->
                    delegate.get().renameTable(TableIdentifier.of(ns, tblName), TableIdentifier.of(ns, newTblName)));
        } catch (RESTException re) {
            LOG.error("Failed to rename table using REST Catalog, for dbName {} tableName {} newTblName {}",
                    dbName, tblName, newTblName, re);
            throw new StarRocksConnectorException("Failed to rename table using REST Catalog",
                    new RuntimeException("Failed to rename table using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public ViewBuilder getViewBuilder(TableIdentifier identifier) {
        return delegate.get().buildView(identifier);
    }

    // the view DDL default methods run the terminal ViewBuilder.create()/createOrReplace() outside this class,
    // so route the whole flow through withAuthRecovery; the retry re-runs getViewBuilder on the rebuilt delegate
    @Override
    public boolean createView(String catalogName, ConnectorViewDefinition definition, boolean replace) {
        return withAuthRecovery(() -> IcebergCatalog.super.createView(catalogName, definition, replace));
    }

    @Override
    public boolean alterView(View currentView, ConnectorViewDefinition definition) {
        return withAuthRecovery(() -> IcebergCatalog.super.alterView(currentView, definition));
    }

    @Override
    public boolean dropView(String dbName, String viewName) {
        try {
            return withAuthRecovery(() ->
                    delegate.get().dropView(TableIdentifier.of(convertDbNameToNamespace(dbName), viewName)));
        } catch (RESTException re) {
            LOG.error("Failed to drop view using REST Catalog, for dbName {} viewName {}", dbName, viewName, re);
            throw new StarRocksConnectorException("Failed to drop view using REST Catalog",
                    new RuntimeException("Failed to drop view using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public View getView(String dbName, String viewName) {
        try {
            return withAuthRecovery(() ->
                    delegate.get().loadView(TableIdentifier.of(convertDbNameToNamespace(dbName), viewName)));
        } catch (RESTException re) {
            LOG.error("Failed to rename table using REST Catalog, for dbName {} viewName {}", dbName, viewName, re);
            throw new StarRocksConnectorException("Failed to rename table using REST Catalog",
                    new RuntimeException("Failed to rename table using REST Catalog, exception: " + re.getMessage(), re));
        }
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        if (fileLocations.isEmpty()) {
            return;
        }

        URI uri = new Path(fileLocations.get(0)).toUri();
        try {
            FileSystem fileSystem = FileSystem.get(uri, conf);
            for (String location : fileLocations) {
                Path path = new Path(location);
                fileSystem.delete(path, false);
            }
        } catch (Exception e) {
            LOG.error("Failed to delete uncommitted files", e);
        }
    }

    public String toString() {
        return delegate.get().toString();
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace ns) {
        try {
            return ImmutableMap.copyOf(withAuthRecovery(() -> delegate.get().loadNamespaceMetadata(ns)));
        } catch (RESTException re) {
            LOG.error("Failed to load table metadata using REST Catalog, for namespace {}", ns, re);
            throw new StarRocksConnectorException("Failed to load table metadata using REST Catalog/defaultTableLocation",
                    new RuntimeException(
                            "Failed to load table metadata using REST Catalog/defaultTableLocation, exception: " +
                                    re.getMessage(), re));
        }
    }

    @Override
    public String defaultTableLocation(Namespace ns, String tableName) {
        // iceberg rest catalog doesn't require location property, and could choose the default location.
        return null;
    }

    private <T> T withAuthRecovery(Supplier<T> action) {
        try {
            return action.get();
        } catch (RuntimeException e) {
            if (!causedByNotAuthorized(e) || !tryRecoverAuthSession(e)) {
                throw e;
            }
            return action.get();
        }
    }

    private void runWithAuthRecovery(Runnable action) {
        withAuthRecovery(() -> {
            action.run();
            return null;
        });
    }

    // the view DDL default methods wrap the 401 in StarRocksConnectorException, so walk the cause chain
    private static boolean causedByNotAuthorized(RuntimeException failure) {
        return Throwables.getCausalChain(failure).stream().anyMatch(NotAuthorizedException.class::isInstance);
    }

    private synchronized boolean tryRecoverAuthSession(RuntimeException cause) {
        if (!authRecoveryEnabled) {
            return false;
        }
        if (System.currentTimeMillis() - lastAuthRecoveryMillis < AUTH_RECOVERY_MIN_INTERVAL_MS) {
            // a rebuild ran or failed within the cooldown; retry on the current delegate rather than rebuild again
            return true;
        }
        LOG.warn("REST catalog {} rejected the current OAuth2 token; rebuilding the catalog client to re-authenticate",
                catalogName, cause);
        try {
            RESTCatalog rebuilt = (RESTCatalog) CatalogUtil.loadCatalog(
                    RESTCatalog.class.getName(), catalogName, restCatalogProperties, conf);
            // don't close the old delegate: tables it produced may still be cached upstream
            // (CachingIcebergCatalog holds them up to its table-cache TTL) and use its FileIO; leave it for GC
            delegate.set(rebuilt);
            return true;
        } catch (Exception rebuildError) {
            LOG.warn("Failed to rebuild REST catalog {} to re-authenticate", catalogName, rebuildError);
            return false;
        } finally {
            // start the cooldown from completion so a slow rebuild doesn't immediately admit another
            lastAuthRecoveryMillis = System.currentTimeMillis();
        }
    }
}
