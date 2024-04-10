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
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.Table;

import java.util.List;

public class MockIcebergCatalog implements IcebergCatalog {
    public MockIcebergCatalog() {}

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return null;
    }

    @Override
    public List<String> listAllDatabases() {
        return null;
    }

    @Override
    public Database getDB(String dbName) {
        return null;
    }

    @Override
    public List<String> listTables(String dbName) {
        return null;
    }

    @Override
    public void renameTable(String dbName, String tblName, String newTblName) throws StarRocksConnectorException {
    }

    @Override
    public Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
        return null;
    }
}
