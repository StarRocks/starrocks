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

package com.starrocks.connector.adbc;

import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.starrocks.catalog.Table.TableType.ADBC;

public class MockedADBCMetadata implements ConnectorMetadata {
    private final AtomicLong idGen = new AtomicLong(0L);
    public static final String MOCKED_ADBC_CATALOG_NAME = "adbc0";
    public static final String MOCKED_DB_NAME = "test_db0";
    public static final String MOCKED_TABLE_NAME0 = "tbl0";

    private Map<String, String> properties;
    private Map<String, ADBCTable> tables = new HashMap<>();

    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MockedADBCMetadata(Map<String, String> properties) {
        readLock();
        try {
            this.properties = properties;
        } finally {
            readUnlock();
        }
    }

    private Table getADBCTable(String tblName) {
        readLock();
        try {
            if (tables.get(tblName) == null) {
                if (tblName.equals(MOCKED_TABLE_NAME0)) {
                    tables.put(tblName, new ADBCTable(100000, MOCKED_TABLE_NAME0, getSchema(tblName),
                            MOCKED_DB_NAME, MOCKED_ADBC_CATALOG_NAME, properties));
                } else {
                    tables.put(tblName, new ADBCTable(tblName.hashCode(), tblName, getSchema(tblName),
                            MOCKED_DB_NAME, MOCKED_ADBC_CATALOG_NAME, properties));
                }
            }
            return tables.get(tblName);
        } finally {
            readUnlock();
        }
    }

    private List<Column> getSchema(String tblName) {
        readLock();
        try {
            return Arrays.asList(new Column("a", VarcharType.VARCHAR), new Column("b", VarcharType.VARCHAR),
                    new Column("c", IntegerType.INT), new Column("d", VarcharType.VARCHAR));
        } finally {
            readUnlock();
        }
    }

    @Override
    public Table.TableType getTableType() {
        return ADBC;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        readLock();
        try {
            return getADBCTable(tblName);
        } finally {
            readUnlock();
        }
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        readLock();
        try {
            return new Database(idGen.getAndIncrement(), dbName);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName,
                                           ConnectorMetadatRequestContext requestContext) {
        return Collections.emptyList();
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        readLock();
        try {
            return Arrays.asList(MOCKED_TABLE_NAME0);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        readLock();
        try {
            return Arrays.asList(MOCKED_DB_NAME);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return Collections.emptyList();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }
}
