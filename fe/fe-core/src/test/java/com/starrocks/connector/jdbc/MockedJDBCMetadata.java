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

import com.mockrunner.mock.jdbc.MockResultSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.CharType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.starrocks.catalog.Table.TableType.JDBC;

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
    public static final String MOCKED_CLICKHOUSE_AGG_DB_NAME = "ck_db";
    public static final String MOCKED_CLICKHOUSE_AGG_TABLE_NAME = "ck_agg_table";

    // string partition table
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME1 = "part_tbl1";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME2 = "part_tbl2";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME3 = "part_tbl3";
    private Map<String, String> properties;
    private Map<String, JDBCTable> tables = new HashMap<>();

    private List<String> partitionNames = Arrays.asList("20230801", "20230802", "20230803", "MAXVALUE");
    private List<PartitionInfo> partitions = Arrays.asList(
            new Partition("d", 1690819200L),
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
                return Arrays.asList(new Column("a", VarcharType.VARCHAR), new Column("b", VarcharType.VARCHAR),
                        new Column("c", IntegerType.INT), new Column("d", IntegerType.INT));
            } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME3)) {
                return Arrays.asList(new Column("a", VarcharType.VARCHAR), new Column("b", VarcharType.VARCHAR),
                        new Column("c", IntegerType.INT), new Column("d", CharType.CHAR));
            } else if (tblName.equals(MOCKED_CLICKHOUSE_AGG_TABLE_NAME)) {
                try {
                    MockResultSet columnResult = new MockResultSet("columns");
                    columnResult.addColumn("DATA_TYPE", Arrays.asList(
                            Types.INTEGER, // id
                            Types.OTHER,   // clicks (sum)
                            Types.OTHER,   // max_price (max)
                            Types.OTHER,   // min_price (min)
                            Types.OTHER,   // total_count (count)
                            Types.OTHER,    // avg_score (avg)
                            Types.OTHER,    // simple_sum (simple sum)
                            Types.OTHER  // decimal_col (sum Decimal)
                    ));
                    columnResult.addColumn("TYPE_NAME", Arrays.asList(
                            "Int32", 
                            "AggregateFunction(sum, Int32)",
                            "AggregateFunction(max, Float64)",
                            "AggregateFunction(min, Float64)",
                            "AggregateFunction(count, Int32)",
                            "AggregateFunction(avg, Float64)",
                            "SimpleAggregateFunction(sum, Int32)",
                            "AggregateFunction(sum, Decimal(18, 4))"
                    ));
                    columnResult.addColumn("COLUMN_SIZE", Arrays.asList(4, 0, 0, 0, 0, 0, 0, 18));
                    columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 0, 0, 0, 0, 0, 4));
                    columnResult.addColumn("COLUMN_NAME", Arrays.asList(
                            "id", "clicks", "max_price", "min_price", "total_count", "avg_score", "simple_sum", "decimal_col"));
                    columnResult.addColumn("IS_NULLABLE", Arrays.asList("NO", "YES", "YES", "YES", "YES", "YES", "YES", "YES"));
                    columnResult.addColumn("REMARKS", Arrays.asList("", "", "", "", "", "", "", ""));


                    ClickhouseSchemaResolver resolver = new ClickhouseSchemaResolver(properties);
                    return resolver.convertToSRTable(columnResult);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to resolve ClickHouse aggregate table schema", e);
                }
            } else {
                return Arrays.asList(new Column("a", VarcharType.VARCHAR), new Column("b", VarcharType.VARCHAR),
                        new Column("c", IntegerType.INT), new Column("d", VarcharType.VARCHAR));
            }
        } finally {
            readUnlock();
        }
    }

    private List<Column> getPartitionColumns(String tblName) {
        readLock();
        try {
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME0)) {
                return Arrays.asList(new Column("d", IntegerType.INT));
            }
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME3)) {
                return Arrays.asList(new Column("d", CharType.CHAR));
            } else {
                return Arrays.asList(new Column("d", VarcharType.VARCHAR));
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public Table.TableType getTableType() {
        return JDBC;
    }

    @Override
    public com.starrocks.catalog.Table getTable(ConnectContext context, String dbName, String tblName) {
        readLock();
        try {
            return getJDBCTable(tblName);
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
    public List<String> listPartitionNames(String dbName, String tableName, ConnectorMetadatRequestContext requestContext) {
        readLock();
        try {
            if (tableName.equals(MOCKED_PARTITIONED_TABLE_NAME2)) {
                return Arrays.asList("1234567", "1234568", "1234569", "1234570");
            } else if (tableName.equals(MOCKED_PARTITIONED_TABLE_NAME3)
                    || tableName.equals(MOCKED_PARTITIONED_TABLE_NAME5)) {
                return Arrays.asList("20230801", "20230802", "20230803", "20230804");
            } else {
                return partitionNames;
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        readLock();
        try {
            return Arrays.asList(MOCKED_PARTITIONED_TABLE_NAME0, MOCKED_PARTITIONED_TABLE_NAME1,
                    MOCKED_PARTITIONED_TABLE_NAME2, MOCKED_PARTITIONED_TABLE_NAME3,
                    MOCKED_STRING_PARTITIONED_TABLE_NAME1, MOCKED_STRING_PARTITIONED_TABLE_NAME2,
                    MOCKED_STRING_PARTITIONED_TABLE_NAME3, MOCKED_CLICKHOUSE_AGG_TABLE_NAME);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        readLock();
        try {
            return Arrays.asList(MOCKED_PARTITIONED_DB_NAME, MOCKED_CLICKHOUSE_AGG_DB_NAME);
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
