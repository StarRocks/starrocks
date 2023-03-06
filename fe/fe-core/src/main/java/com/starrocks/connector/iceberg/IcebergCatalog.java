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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Interface for Iceberg catalogs.
 * // TODO: add interface for creating iceberg table
 */
public interface IcebergCatalog {

    IcebergCatalogType getIcebergCatalogType();

    /**
     * Loads a native Iceberg table based on the information in 'feTable'.
     */
    Table loadTable(IcebergTable table) throws StarRocksConnectorException;

    /**
     * Loads a native Iceberg table based on the information in 'feTable'.
     */
    Table loadTable(TableIdentifier tableIdentifier) throws StarRocksConnectorException;

    /**
     * Loads a native Iceberg table based on 'tableId' or 'tableLocation'.
     *
     * @param tableId       is the Iceberg table identifier to load the table via the catalog
     *                      interface, e.g. HiveCatalog.
     * @param tableLocation is the filesystem path to load the table via the HadoopTables
     *                      interface.
     * @param properties    provides information for table loading when Iceberg Catalogs
     *                      is being used.
     */
    Table loadTable(TableIdentifier tableId,
                    String tableLocation,
                    Map<String, String> properties) throws StarRocksConnectorException;

    List<String> listAllDatabases();

    Database getDB(String dbName) throws InterruptedException, TException;

    List<TableIdentifier> listTables(Namespace of);
}
