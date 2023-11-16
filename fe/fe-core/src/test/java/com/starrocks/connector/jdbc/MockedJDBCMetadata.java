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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockedJDBCMetadata implements ConnectorMetadata {
    private final AtomicLong idGen = new AtomicLong(0L);
    public static final String MOCKED_JDBC_CATALOG_NAME = "jdbc0";
    public static final String MOCKED_JDBC_PG_CATALOG_NAME = "jdbc_postgres";
    public static final String MOCKED_PARTITIONED_DB_NAME = "partitioned_db0";
    public static final String MOCKED_PARTITIONED_TABLE_NAME0 = "tbl0";
    public static final String MOCKED_PARTITIONED_TABLE_NAME1 = "tbl1";
    public static final String MOCKED_PARTITIONED_TABLE_NAME2 = "tbl2";
    public static final String MOCKED_PARTITIONED_TABLE_NAME3 = "tbl3";
    public static final String MOCKED_PARTITIONED_TABLE_NAME5 = "tbl5";

    // string partition table
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME1 = "part_tbl1";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME2 = "part_tbl2";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME3 = "part_tbl3";
    private Map<String, String> properties;
    private Map<String, JDBCTable> tables = new HashMap<>();

    private List<String> partitionNames = Arrays.asList("20230801", "20230802", "20230803", "MAXVALUE");
    private List<PartitionInfo> partitions = Arrays.asList(new Partition("d", 1690819200L),
            new Partition("d", 1690819200L),
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

    private Table getJDBCTable(String tblName) {
        readLock();
        try {
            if (tables.get(tblName) == null) {
                if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME0)) {
                    tables.put(tblName, new JDBCTable(100000, MOCKED_PARTITIONED_TABLE_NAME0, getSchema(tblName),
                            getPartitionColumns(tblName), MOCKED_PARTITIONED_DB_NAME, MOCKED_JDBC_CATALOG_NAME, properties));
                } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
                    tables.put(tblName, new JDBCTable(100001, MOCKED_PARTITIONED_TABLE_NAME1, getSchema(tblName),
                            getPartitionColumns(tblName), MOCKED_PARTITIONED_DB_NAME, MOCKED_JDBC_CATALOG_NAME, properties));
                } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME2)) {
                    tables.put(tblName, new JDBCTable(100002, MOCKED_PARTITIONED_TABLE_NAME2, getSchema(tblName),
                            getPartitionColumns(tblName), MOCKED_PARTITIONED_DB_NAME, MOCKED_JDBC_CATALOG_NAME, properties));
                } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME3)) {
                    tables.put(tblName, new JDBCTable(100003, MOCKED_PARTITIONED_TABLE_NAME3, getSchema(tblName),
                            getPartitionColumns(tblName), MOCKED_PARTITIONED_DB_NAME, MOCKED_JDBC_CATALOG_NAME, properties));
                } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME5)) {
                    tables.put(tblName, new JDBCTable(100005, MOCKED_PARTITIONED_TABLE_NAME5, getSchema(tblName),
                            getPartitionColumns(tblName), MOCKED_PARTITIONED_DB_NAME, MOCKED_JDBC_CATALOG_NAME, properties));
                } else {
                    tables.put(tblName, new JDBCTable(tblName.hashCode(), tblName, getSchema(tblName),
                            getPartitionColumns(tblName), MOCKED_PARTITIONED_DB_NAME, MOCKED_JDBC_CATALOG_NAME, properties));
                }
            }
            return tables.get(tblName);
        } catch (DdlException e) {
            e.printStackTrace();
        } finally {
            readUnlock();
        }
        return null;
    }

    private List<Column> getSchema(String tblName) {
        readLock();
        try {
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME0)) {
                return Arrays.asList(new Column("a", Type.VARCHAR), new Column("b", Type.VARCHAR),
                        new Column("c", Type.INT), new Column("d", Type.INT));
            } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME3)) {
                return Arrays.asList(new Column("a", Type.VARCHAR), new Column("b", Type.VARCHAR),
                        new Column("c", Type.INT), new Column("d", Type.CHAR));
            } else {
                return Arrays.asList(new Column("a", Type.VARCHAR), new Column("b", Type.VARCHAR),
                        new Column("c", Type.INT), new Column("d", Type.VARCHAR));
            }
        } finally {
            readUnlock();
        }
    }

    private List<Column> getPartitionColumns(String tblName) {
        readLock();
        try {
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME0)) {
                return Arrays.asList(new Column("d", Type.INT));
            }
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME3)) {
                return Arrays.asList(new Column("d", Type.CHAR));
            } else {
                return Arrays.asList(new Column("d", Type.VARCHAR));
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public com.starrocks.catalog.Table getTable(String dbName, String tblName) {
        readLock();
        try {
            return getJDBCTable(tblName);
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
            if (tableName.equals(MOCKED_PARTITIONED_TABLE_NAME2)) {
                return Arrays.asList("1234567", "1234568", "1234569");
            } else if (tableName.equals(MOCKED_PARTITIONED_TABLE_NAME3)
                    || tableName.equals(MOCKED_PARTITIONED_TABLE_NAME5)) {
                return Arrays.asList("20230801", "20230802", "20230803");
            } else {
                return partitionNames;
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        readLock();
        try {
            return Arrays.asList(MOCKED_PARTITIONED_TABLE_NAME0, MOCKED_PARTITIONED_TABLE_NAME1,
                    MOCKED_PARTITIONED_TABLE_NAME2, MOCKED_PARTITIONED_TABLE_NAME3,
                    MOCKED_STRING_PARTITIONED_TABLE_NAME1, MOCKED_STRING_PARTITIONED_TABLE_NAME2,
                    MOCKED_STRING_PARTITIONED_TABLE_NAME3);
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

    public void initPartitions() {
        readLock();
        try {
            partitionNames = Arrays.asList("20230801", "20230802", "20230803", "MAXVALUE");
            partitions = Arrays.asList(new Partition("d", 1690819200L),
                    new Partition("d", 1690819200L),
                    new Partition("d", 1690819200L),
                    new Partition("d", 1690819200L));
        } finally {
            readUnlock();
        }
    }

    public void addPartitions() {
        readLock();
        try {
            partitionNames = Arrays.asList("20230802", "20230803", "20230804", "20230805", "MAXVALUE");
            partitions = Arrays.asList(new Partition("d", 1690819200L),
                    new Partition("d", 1690819200L),
                    new Partition("d", 1690819200L),
                    new Partition("d", 1690819200L),
                    new Partition("d", 1690819200L));
        } finally {
            readUnlock();
        }
    }

    public void refreshPartitions() {
        readLock();
        try {
            partitions = Arrays.asList(new Partition("d", 1690819300L),
                    new Partition("d", 1690819300L),
                    new Partition("d", 1690819300L),
                    new Partition("d", 1690819300L));
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
