// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
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
    Table loadTable(IcebergTable table) throws StarRocksIcebergException;

    /**
     * Loads a native Iceberg table based on the information in 'feTable'.
     */
    Table loadTable(TableIdentifier tableIdentifier) throws StarRocksIcebergException;

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
                    Map<String, String> properties) throws StarRocksIcebergException;

    List<String> listAllDatabases();

    Database getDB(String dbName) throws InterruptedException, TException;

    List<TableIdentifier> listTables(Namespace of);
}
