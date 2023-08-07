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
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;

public interface IcebergCatalog {

    IcebergCatalogType getIcebergCatalogType();

    List<String> listAllDatabases();

    default void createDb(String dbName, Map<String, String> properties) {
    }

    default void dropDb(String dbName) throws MetaNotFoundException {
    }

    Database getDB(String dbName);

    List<String> listTables(String dbName);

    default boolean createTable(String dbName,
                                String tableName,
                                Schema schema,
                                PartitionSpec partitionSpec,
                                String location,
                                Map<String, String> properties) {
        return false;
    }

    default boolean dropTable(String dbName, String tableName, boolean purge) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping tables");
    }

    Table getTable(String dbName, String tableName) throws StarRocksConnectorException;


    default void deleteUncommittedDataFiles(List<String> fileLocations) {
    }

}
