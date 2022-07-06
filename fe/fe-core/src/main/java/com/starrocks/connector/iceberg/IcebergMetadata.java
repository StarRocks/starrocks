// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.iceberg.IcebergHiveCatalog;
import com.starrocks.external.iceberg.IcebergUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.external.iceberg.IcebergUtil.getIcebergHiveCatalog;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private final String metastoreURI;
    private IcebergHiveCatalog hiveCatalog;

    public IcebergMetadata(String metastoreURI) {
        Map<String, String> properties = new HashMap<>();
        // the first phase of IcebergConnector only supports hive catalog.
        this.hiveCatalog = (IcebergHiveCatalog) getIcebergHiveCatalog(metastoreURI, properties);
        this.metastoreURI = metastoreURI;
    }

    @Override
    public List<String> listDbNames() throws DdlException {
        return hiveCatalog.listAllDatabases();
    }

    @Override
    public Database getDb(String dbName) {
        try {
            return hiveCatalog.getDB(dbName);
        } catch (InterruptedException | TException e) {
            LOG.error("Failed to get iceberg database " + dbName, e);
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) throws DdlException {
        List<TableIdentifier> tableIdentifiers = hiveCatalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            org.apache.iceberg.Table icebergTable = hiveCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
            return IcebergUtil.convertToSRTable(icebergTable, metastoreURI, dbName, tblName);
        } catch (DdlException e) {
            LOG.error("Failed to get iceberg table " + IcebergUtil.getIcebergTableIdentifier(dbName, tblName), e);
            return null;
        }
    }
}
