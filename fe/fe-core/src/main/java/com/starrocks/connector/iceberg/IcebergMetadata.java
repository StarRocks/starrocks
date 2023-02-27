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


package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
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

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private final String catalogName;
    private IcebergCatalog icebergCatalog;
    private String metastoreURI;
    private String catalogImpl;
    private String catalogType;
    private Map<String, String> customProperties;

    public IcebergMetadata(String catalogName, IcebergCatalog icebergCatalog,
                           String metastoreURI, String catalogImpl, String catalogType, Map<String, String> customProperties) {
        this.catalogName = catalogName;
        this.icebergCatalog = icebergCatalog;
        this.metastoreURI = metastoreURI;
        this.catalogImpl = catalogImpl;
        this.catalogType = catalogType;
        this.customProperties = customProperties;
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
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
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
            } else if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.REST_CATALOG)) {
                return IcebergUtil.convertRESTCatalogToSRTable(icebergTable, catalogName, dbName, tblName);
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
                && !IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.REST_CATALOG)
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
