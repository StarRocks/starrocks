// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.server.GlobalStateMgr;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_TYPE;
import static com.starrocks.catalog.IcebergTable.ICEBERG_IMPL;
import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergCustomCatalog;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergGlueCatalog;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergHiveCatalog;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private String metastoreURI;
    private String catalogType;
    private String catalogImpl;
    private final String catalogName;
    private IcebergCatalog icebergCatalog;
    private Map<String, String> customProperties;

    public IcebergMetadata(String catalogName, Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;

        if (IcebergCatalogType.HIVE_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
            Util.validateMetastoreUris(metastoreURI);
            icebergCatalog = getIcebergHiveCatalog(metastoreURI, properties, hdfsEnvironment);
        } else if (IcebergCatalogType.CUSTOM_CATALOG ==
                IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            catalogImpl = properties.get(ICEBERG_IMPL);
            icebergCatalog = getIcebergCustomCatalog(catalogImpl, properties, hdfsEnvironment);
            properties.remove(ICEBERG_CATALOG_TYPE);
            properties.remove(ICEBERG_IMPL);
            customProperties = properties;
        } else if (IcebergCatalogType.GLUE_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            icebergCatalog = getIcebergGlueCatalog(catalogName, properties, hdfsEnvironment);
        } else {
            throw new RuntimeException(String.format("Property %s is missing or not supported now.",
                    ICEBERG_CATALOG_TYPE));
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
            // Submit a future task for refreshing
            GlobalStateMgr.getCurrentState().getIcebergRepository().refreshTable(icebergTable);
            if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.CUSTOM_CATALOG)) {
                return IcebergUtil.convertCustomCatalogToSRTable(icebergTable, catalogImpl, catalogName, dbName,
                        tblName, customProperties);
            } else if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.GLUE_CATALOG)) {
                return IcebergUtil.convertGlueCatalogToSRTable(icebergTable, catalogName, dbName, tblName);
            } else {
                return IcebergUtil.convertHiveCatalogToSRTable(icebergTable, metastoreURI, catalogName, dbName, tblName);
            }
        } catch (DdlException e) {
            LOG.error("Failed to get iceberg table " + IcebergUtil.getIcebergTableIdentifier(dbName, tblName), e);
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        org.apache.iceberg.Table icebergTable
                = icebergCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
        if (!IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.HIVE_CATALOG)
                && !IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.GLUE_CATALOG)) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from catalog type: " + catalogType);
        }
        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from No-Identity partition transform now");
        }

        return IcebergUtil.getIdentityPartitionNames(icebergTable);
    }
}
