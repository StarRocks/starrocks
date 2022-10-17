// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.iceberg.IcebergCatalog;
import com.starrocks.external.iceberg.IcebergCatalogType;
import com.starrocks.external.iceberg.IcebergUtil;
import com.starrocks.external.iceberg.StarRocksIcebergException;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG;
import static com.starrocks.catalog.IcebergTable.ICEBERG_IMPL;
import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;
import static com.starrocks.external.iceberg.IcebergUtil.getIcebergCustomCatalog;
import static com.starrocks.external.iceberg.IcebergUtil.getIcebergHiveCatalog;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private String metastoreURI;
    private String catalogType;
    private String catalogImpl;
    private IcebergCatalog icebergCatalog;
    private Map<String, String> customProperties;

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
    }

    @Override
    public List<String> listDbNames() {
        return icebergCatalog.listAllDatabases();
    }

    @Override
    public Database getDb(String dbName) {
        try {
            return icebergCatalog.getDB(dbName);
        } catch (InterruptedException | TException e) {
            LOG.error("Failed to get iceberg database " + dbName, e);
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        List<TableIdentifier> tableIdentifiers = icebergCatalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            org.apache.iceberg.Table icebergTable
                    = icebergCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
            if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.CUSTOM_CATALOG)) {
                return IcebergUtil.convertCustomCatalogToSRTable(icebergTable, catalogImpl, dbName, tblName, customProperties);
            }
            return IcebergUtil.convertHiveCatalogToSRTable(icebergTable, metastoreURI, dbName, tblName);
        } catch (DdlException e) {
            LOG.error("Failed to get iceberg table " + IcebergUtil.getIcebergTableIdentifier(dbName, tblName), e);
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        org.apache.iceberg.Table icebergTable
                = icebergCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
        if (!IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.HIVE_CATALOG)) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from catalog type: " + catalogType);
        }
        if (icebergTable.spec().fields().stream().anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from No-Identity partition transform now");
        }

        return IcebergUtil.getIdentityPartitionNames(icebergTable);
    }
}
