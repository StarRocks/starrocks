// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.iceberg.IcebergCatalog;
import com.starrocks.external.iceberg.IcebergCatalogType;
import com.starrocks.external.iceberg.IcebergUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG;
import static com.starrocks.catalog.IcebergTable.ICEBERG_IMPL;
import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;
import static com.starrocks.external.iceberg.IcebergUtil.getIcebergCustomCatalog;
import static com.starrocks.external.iceberg.IcebergUtil.getIcebergHiveCatalog;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private String metastoreURI;
    private String catalogType;
    private String catalogImpl;
    private IcebergCatalog icebergCatalog;
    private Map<String, String> customProperties;
    private LoadingCache<String, Database> dbCache;
    private LoadingCache<TableIdentifier, Table> tableCache;
    private static final long MAX_TABLE_CACHE_SIZE = 1000L;

    public IcebergMetadata(Map<String, String> properties) {
        if (IcebergCatalogType.HIVE_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG))) {
            catalogType = properties.get(ICEBERG_CATALOG);
            metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
            icebergCatalog = getIcebergHiveCatalog(metastoreURI, properties);
            Util.validateMetastoreUris(metastoreURI);
        } else if (IcebergCatalogType.CUSTOM_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG))) {
            catalogType = properties.get(ICEBERG_CATALOG);
            catalogImpl = properties.get(ICEBERG_IMPL);
            icebergCatalog = getIcebergCustomCatalog(catalogImpl, properties);
            properties.remove(ICEBERG_CATALOG);
            properties.remove(ICEBERG_IMPL);
            customProperties = properties;
        } else {
            throw new RuntimeException(String.format("Property %s is missing or not supported now.", ICEBERG_CATALOG));
        }
        tableCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE).build(new CacheLoader<TableIdentifier, Table>() {
            @Override
            public Table load(TableIdentifier ti) throws Exception {
                return loadTable(ti);
            }
        });
        dbCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE).build(new CacheLoader<String, Database>() {
            @Override
            public Database load(String key) throws Exception {
                return loadDatabase(key);
            }
        });
    }

    /**
     * Currently we only support either refreshAfterWrite or automatic refresh by events.
     */
    private static CacheBuilder<Object, Object> newCacheBuilder(long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.expireAfterWrite(Config.hive_meta_cache_ttl_s, SECONDS);
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    @Override
    public List<String> listDbNames() throws DdlException {
        return icebergCatalog.listAllDatabases();
    }

    @Override
    public Database getDb(String dbName) {
        try {
            return dbCache.get(dbName);
        } catch (ExecutionException e) {
            LOG.error("Failed to get iceberg database " + dbName, e);
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) throws DdlException {
        List<TableIdentifier> tableIdentifiers = icebergCatalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    public Table loadTable(TableIdentifier ti) throws DdlException {
        org.apache.iceberg.Table icebergTable
                = icebergCatalog.loadTable(ti);
        String dbName = ti.namespace().level(0);
        String tblName = ti.name();
        if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.CUSTOM_CATALOG)) {
            return IcebergUtil.convertCustomCatalogToSRTable(icebergTable, catalogImpl, dbName, tblName,
                    customProperties);
        }
        return IcebergUtil.convertHiveCatalogToSRTable(icebergTable, metastoreURI, dbName, tblName);
    }

    public Database loadDatabase(String dbName) throws TException, InterruptedException {
        return icebergCatalog.getDB(dbName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        TableIdentifier ti = IcebergUtil.getIcebergTableIdentifier(dbName, tblName);
        try {
            return tableCache.get(ti);
        } catch (ExecutionException e) {
            LOG.error("Failed to get iceberg table " + ti, e);
            return null;
        }
    }
}
