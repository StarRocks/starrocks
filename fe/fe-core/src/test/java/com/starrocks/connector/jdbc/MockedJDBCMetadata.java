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

package com.starrocks.connector.jdbc;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockedJDBCMetadata implements ConnectorMetadata {
    private final AtomicLong idGen = new AtomicLong(0L);
    public static final String MOCKED_JDBC_CATALOG_NAME = "jdbc0";
    public static final String MOCKED_PARTITIONED_DB_NAME = "partitioned_db0";
    public static final String MOCKED_PARTITIONED_TABLE_NAME = "tbl0";
    private Map<String, String> properties;

    private List<String> partitionNames = Arrays.asList("20230801", "20230802", "20230803");
    private List<PartitionInfo> partitions = Arrays.asList(new Partition("d", 1690819200L),
            new Partition("d", 1690819200L),
            new Partition("d", 1690819200L));

    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MockedJDBCMetadata(Map<String, String> properties) {
        readLock();
        try {
            this.properties = properties;
        } finally {
            readUnlock();
        }
    }

    private Table getJDBCTable() {
        readLock();
        try {
            return new JDBCTable(100000, MOCKED_PARTITIONED_TABLE_NAME, getSchema(),
                    getPartitionColumns(), MOCKED_PARTITIONED_DB_NAME, MOCKED_JDBC_CATALOG_NAME, properties);
        } catch (DdlException e) {
            e.printStackTrace();
        } finally {
            readUnlock();
        }
        return null;
    }

    private List<Column> getSchema() {
        readLock();
        try {
            return Arrays.asList(new Column("a", Type.VARCHAR), new Column("b", Type.VARCHAR),
                    new Column("c", Type.VARCHAR), new Column("d", Type.INT));
        } finally {
            readUnlock();
        }
    }

    private List<Column> getPartitionColumns() {
        readLock();
        try {
            return Arrays.asList(new Column("d", Type.INT));
        } finally {
            readUnlock();
        }
    }

    @Override
    public com.starrocks.catalog.Table getTable(String dbName, String tblName) {
        readLock();
        try {
            return getJDBCTable();
        } finally {
            readUnlock();
        }
    }

    @Override
    public Database getDb(String dbName) {
        readLock();
        try {
            return new Database(idGen.getAndIncrement(), dbName);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName) {
        readLock();
        try {
            return partitionNames;
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        readLock();
        try {
            return Arrays.asList(MOCKED_PARTITIONED_TABLE_NAME);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listDbNames() {
        readLock();
        try {
            return Arrays.asList(MOCKED_PARTITIONED_DB_NAME);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(com.starrocks.catalog.Table table, List<String> partitionNames) {
        readLock();
        try {
            return partitions;
        } finally {
            readUnlock();
        }
    }

    public void addPartitions() {
        readLock();
        try {
            partitionNames = Arrays.asList("20230802", "20230803", "20230804", "20230805");
            partitions = Arrays.asList(new Partition("d", 1690819200L),
                            new Partition("d", 1690819200L),
                            new Partition("d", 1690819200L),
                            new Partition("d", 1690819200L));
        } finally {
            readUnlock();
        }
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

}
