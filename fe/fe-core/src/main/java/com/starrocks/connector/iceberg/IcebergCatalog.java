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
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface IcebergCatalog {

    IcebergCatalogType getIcebergCatalogType();

    Table loadTable(IcebergTable table) throws StarRocksConnectorException;

    Table loadTable(TableIdentifier tableIdentifier) throws StarRocksConnectorException;

    Table loadTable(TableIdentifier tableIdentifier, Optional<IcebergMetricsReporter> metricsReporter)
            throws StarRocksConnectorException;

    Table loadTable(TableIdentifier tableId,
                    String tableLocation,
                    Map<String, String> properties) throws StarRocksConnectorException;

    List<String> listAllDatabases();

    default void createDb(String dbName, Map<String, String> properties) {
    }

    default void dropDb(String dbName) throws MetaNotFoundException {
    }

    Database getDB(String dbName) throws InterruptedException, TException;

    List<TableIdentifier> listTables(Namespace of);

    default Transaction newCreateTableTransaction(
            String dbName,
            String tableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties) {
        throw new StarRocksConnectorException("This catalog doesn't support creating tables");
    }

    default boolean dropTable(TableIdentifier identifier, boolean purge) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping tables");
    }

    default String defaultTableLocation(String dbName, String tblName) {
        return "";
    }

    default void deleteUncommittedDataFiles(List<String> fileLocations) {
    }

}
