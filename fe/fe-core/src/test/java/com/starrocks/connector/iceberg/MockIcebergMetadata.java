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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.required;

public class MockIcebergMetadata implements ConnectorMetadata {
    private static final Map<String, Map<String, IcebergTableInfo>> MOCK_TABLE_MAP = new CaseInsensitiveMap<>();
    private final AtomicLong idGen = new AtomicLong(0L);
    public static final String MOCKED_ICEBERG_CATALOG_NAME = "iceberg0";
    public static final String MOCKED_UNPARTITIONED_DB_NAME = "unpartitioned_db";
    public static final String MOCKED_PARTITIONED_DB_NAME = "partitioned_db";

    public static final String MOCKED_UNPARTITIONED_TABLE_NAME0 = "t0";
    public static final String MOCKED_PARTITIONED_TABLE_NAME1 = "t1";

    // string partition table
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME1 = "part_tbl1";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME2 = "part_tbl2";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME3 = "part_tbl3";

    private static final List<String> PARTITION_TABLE_NAMES = ImmutableList.of(MOCKED_PARTITIONED_TABLE_NAME1,
            MOCKED_STRING_PARTITIONED_TABLE_NAME1, MOCKED_STRING_PARTITIONED_TABLE_NAME2,
            MOCKED_STRING_PARTITIONED_TABLE_NAME3);

    private static final List<String> PARTITION_NAMES_0 = Lists.newArrayList("date=2020-01-01",
            "date=2020-01-02",
            "date=2020-01-03",
            "date=2020-01-04");
    private static final List<String> PARTITION_NAMES_1 = Lists.newArrayList("d=2023-08-01",
            "d=2023-08-02",
            "d=2023-08-03");
    public static String getStarRocksHome() throws IOException {
        String starRocksHome = System.getenv("STARROCKS_HOME");
        if (Strings.isNullOrEmpty(starRocksHome)) {
            starRocksHome = Files.createTempDirectory("STARROCKS_HOME").toAbsolutePath().toString();
        }
        return starRocksHome;
    }

    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    static {
        try {
            mockUnPartitionedTable();
            mockPartitionedTable();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void mockUnPartitionedTable() throws IOException {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_UNPARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, IcebergTableInfo> icebergTableInfoMap = MOCK_TABLE_MAP.get(MOCKED_UNPARTITIONED_DB_NAME);

        List<Column> schemas = ImmutableList.of(new Column("id", Type.INT, true),
                new Column("data", Type.STRING, true),
                new Column("date", Type.STRING, true));

        Schema schema =
                new Schema(required(3, "id", Types.IntegerType.get()),
                        required(4, "data", Types.StringType.get()),
                        required(5, "date", Types.StringType.get()));
        PartitionSpec spec =
                PartitionSpec.builderFor(schema).build();
        TestTables.TestTable baseTable = TestTables.create(
                new File(getStarRocksHome() + "/" + MOCKED_UNPARTITIONED_DB_NAME + "/" +
                        MOCKED_UNPARTITIONED_TABLE_NAME0), MOCKED_UNPARTITIONED_TABLE_NAME0,
                schema, spec, 1);

        String tableIdentifier = Joiner.on(":").join(MOCKED_UNPARTITIONED_TABLE_NAME0, UUID.randomUUID());
        MockIcebergTable mockIcebergTable = new MockIcebergTable(1, MOCKED_UNPARTITIONED_TABLE_NAME0,
                MOCKED_ICEBERG_CATALOG_NAME, null, MOCKED_UNPARTITIONED_DB_NAME,
                MOCKED_UNPARTITIONED_TABLE_NAME0, schemas, baseTable, null,
                tableIdentifier);

        Map<String, ColumnStatistic> columnStatisticMap;
        List<String> colNames = schemas.stream().map(Column::getName).collect(Collectors.toList());
        columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                col -> ColumnStatistic.unknown()));

        icebergTableInfoMap.put(MOCKED_UNPARTITIONED_TABLE_NAME0,
                new IcebergTableInfo(mockIcebergTable, Lists.newArrayList(), 100, columnStatisticMap));
    }

    public static void mockPartitionedTable() throws IOException {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, IcebergTableInfo> icebergTableInfoMap = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_DB_NAME);

        for (String tblName : PARTITION_TABLE_NAMES) {
            List<Column> columns = getSchema(tblName);
            MockIcebergTable icebergTable = getIcebergTable(tblName, columns);
            Map<String, ColumnStatistic> columnStatisticMap;
            List<String> colNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                    col -> ColumnStatistic.unknown()));
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
                icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, PARTITION_NAMES_0, 100,
                        columnStatisticMap));
            } else {
                icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, PARTITION_NAMES_1, 100,
                        columnStatisticMap));
            }
        }
    }

    private static List<Column> getSchema(String tblName) {
        if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
            return ImmutableList.of(new Column("id", Type.INT, true),
                    new Column("data", Type.STRING, true),
                    new Column("date", Type.STRING, true));
        } else {
            return Arrays.asList(new Column("a", Type.VARCHAR), new Column("b", Type.VARCHAR),
                    new Column("c", Type.INT), new Column("d", Type.VARCHAR));
        }
    }

    private static Schema getIcebergSchema(String tblName) {
        if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
            return new Schema(required(3, "id", Types.IntegerType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "date", Types.StringType.get()));
        } else {
            return new Schema(required(3, "a", Types.StringType.get()),
                    required(4, "b", Types.StringType.get()),
                    required(5, "c", Types.StringType.get()),
                    required(6, "d", Types.StringType.get()));
        }
    }

    private static TestTables.TestTable getTestTable(String tblName, Schema schema) throws IOException {
        if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema).identity("date").build();
            return  TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_DB_NAME + "/"
                            + MOCKED_PARTITIONED_TABLE_NAME1), MOCKED_PARTITIONED_TABLE_NAME1,
                    schema, spec, 1);

        } else {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema).identity("d").build();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + tblName + "/" + tblName),
                    tblName,
                    schema, spec, 1);
        }
    }

    public static MockIcebergTable getIcebergTable(String tblName, List<Column> schemas) throws IOException {
        Schema schema = getIcebergSchema(tblName);
        TestTables.TestTable baseTable = getTestTable(tblName, schema);

        String tableIdentifier = Joiner.on(":").join(tblName, UUID.randomUUID());
        return new MockIcebergTable(tblName.hashCode(), tblName, MOCKED_ICEBERG_CATALOG_NAME,
                null, MOCKED_PARTITIONED_DB_NAME, tblName, schemas, baseTable, null,
                tableIdentifier);
    }

    @Override
    public Database getDb(String dbName) {
        return new Database(idGen.getAndIncrement(), dbName);
    }

    @Override
    public com.starrocks.catalog.Table getTable(String dbName, String tblName) {
        readLock();
        try {
            return MOCK_TABLE_MAP.get(dbName).get(tblName).icebergTable;
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName) {
        readLock();
        try {
            return MOCK_TABLE_MAP.get(dbName).get(tableName).partitionNames;
        } finally {
            readUnlock();
        }
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, com.starrocks.catalog.Table table,
                                         Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        MockIcebergTable icebergTable = (MockIcebergTable) table;
        String hiveDb = icebergTable.getRemoteDbName();
        String tblName = icebergTable.getName();

        readLock();
        try {
            IcebergTableInfo info = MOCK_TABLE_MAP.get(hiveDb).get(tblName);
            Statistics.Builder builder = Statistics.builder();
            builder.setOutputRowCount(info.rowCount);
            for (ColumnRefOperator columnRefOperator : columns.keySet()) {
                ColumnStatistic columnStatistic = info.columnStatsMap.get(columnRefOperator.getName());
                builder.addColumnStatistic(columnRefOperator, columnStatistic);
            }
            return builder.build();
        } finally {
            readUnlock();
        }
    }

    public void addRowsToPartition(String dbName, String tableName, int rowCount, String partitionName) {
        IcebergTable icebergTable = MOCK_TABLE_MAP.get(dbName).get(tableName).icebergTable;
        Table nativeTable = icebergTable.getNativeTable();
        DataFile file = DataFiles.builder(nativeTable.spec())
                .withPath("/path/to/data-a.parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath(partitionName) // easy way to set partition data for now
                .withRecordCount(rowCount)
                .build();
        writeLock();
        try {
            nativeTable.newAppend().appendFile(file).commit();
        } finally {
            writeUnlock();
        }
    }

    private static class IcebergTableInfo {
        private MockIcebergTable icebergTable;
        private List<String> partitionNames;
        private long rowCount;
        private Map<String, ColumnStatistic> columnStatsMap;

        public IcebergTableInfo(MockIcebergTable icebergTable, List<String> partitionNames, long rowCount,
                                Map<String, ColumnStatistic> columnStatsMap) {
            this.icebergTable = icebergTable;
            this.partitionNames = partitionNames;
            this.rowCount = rowCount;
            this.columnStatsMap = columnStatsMap;
        }
    }

    private void writeLock() {
        lock.writeLock().lock();
    }
    private void writeUnlock() {
        lock.writeLock().unlock();
    }
    private void readLock() {
        lock.readLock().lock();
    }
    private void readUnlock() {
        lock.readLock().unlock();
    }
}
