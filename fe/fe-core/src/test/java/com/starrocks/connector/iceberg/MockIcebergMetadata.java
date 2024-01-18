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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
    public static final String MOCKED_PARTITIONED_TRANSFORMS_DB_NAME = "partitioned_transforms_db";

    public static final String MOCKED_UNPARTITIONED_TABLE_NAME0 = "t0";
    public static final String MOCKED_PARTITIONED_TABLE_NAME1 = "t1";

    // string partition table
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME1 = "part_tbl1";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME2 = "part_tbl2";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME3 = "part_tbl3";

    // partition table with transforms
    public static final String MOCKED_PARTITIONED_YEAR_TABLE_NAME = "t0_year";
    public static final String MOCKED_PARTITIONED_MONTH_TABLE_NAME = "t0_month";
    public static final String MOCKED_PARTITIONED_DAY_TABLE_NAME = "t0_day";
    public static final String MOCKED_PARTITIONED_HOUR_TABLE_NAME = "t0_hour";
    public static final String MOCKED_PARTITIONED_BUCKET_TABLE_NAME = "t0_bucket";

    private static final List<String> PARTITION_TABLE_NAMES = ImmutableList.of(MOCKED_PARTITIONED_TABLE_NAME1,
            MOCKED_STRING_PARTITIONED_TABLE_NAME1, MOCKED_STRING_PARTITIONED_TABLE_NAME2,
            MOCKED_STRING_PARTITIONED_TABLE_NAME3);

    private static final List<String> PARTITION_TRANSFORM_TABLE_NAMES =
            ImmutableList.of(MOCKED_PARTITIONED_YEAR_TABLE_NAME, MOCKED_PARTITIONED_MONTH_TABLE_NAME,
                    MOCKED_PARTITIONED_DAY_TABLE_NAME, MOCKED_PARTITIONED_HOUR_TABLE_NAME,
                    MOCKED_PARTITIONED_BUCKET_TABLE_NAME);

    private static final List<String> PARTITION_NAMES_0 = Lists.newArrayList("date=2020-01-01",
            "date=2020-01-02",
            "date=2020-01-03",
            "date=2020-01-04");
    private static final List<String> PARTITION_NAMES_1 = Lists.newArrayList("d=2023-08-01",
            "d=2023-08-02",
            "d=2023-08-03");
    private static final long PARTITION_INIT_VERSION = 100;

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
            mockPartitionTransforms();
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
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put(IcebergMetadata.FILE_FORMAT, "orc");
        icebergProperties.put(IcebergMetadata.COMPRESSION_CODEC, "gzip");
        MockIcebergTable mockIcebergTable = new MockIcebergTable(1, MOCKED_UNPARTITIONED_TABLE_NAME0,
                MOCKED_ICEBERG_CATALOG_NAME, null, MOCKED_UNPARTITIONED_DB_NAME,
                MOCKED_UNPARTITIONED_TABLE_NAME0, schemas, baseTable, icebergProperties,
                tableIdentifier);
        mockIcebergTable.setComment("unpartitioned table");

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
            List<Column> columns = getPartitionedTableSchema(tblName);
            MockIcebergTable icebergTable = getPartitionIcebergTable(tblName, columns);
            Map<String, ColumnStatistic> columnStatisticMap;
            List<String> colNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                    col -> ColumnStatistic.unknown()));
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
                icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, PARTITION_NAMES_0,
                        100, columnStatisticMap));
            } else {
                icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, PARTITION_NAMES_1,
                        100, columnStatisticMap));
            }
        }
    }

    public static void mockPartitionTransforms() throws IOException {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_TRANSFORMS_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, IcebergTableInfo> icebergTableInfoMap = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_TRANSFORMS_DB_NAME);

        for (String tblName : PARTITION_TRANSFORM_TABLE_NAMES) {
            List<Column> columns = getPartitionedTransformTableSchema(tblName);
            MockIcebergTable icebergTable = getPartitionTransformIcebergTable(tblName, columns);
            List<String> partitionNames = getTransformTablePartitionNames(tblName);
            Map<String, ColumnStatistic> columnStatisticMap;
            List<String> colNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                    col -> ColumnStatistic.unknown()));
            icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, partitionNames,
                    100, columnStatisticMap));
        }
    }

    private static List<Column> getPartitionedTableSchema(String tblName) {
        if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
            return ImmutableList.of(new Column("id", Type.INT, true),
                    new Column("data", Type.STRING, true),
                    new Column("date", Type.STRING, true));
        } else {
            return Arrays.asList(new Column("a", Type.VARCHAR), new Column("b", Type.VARCHAR),
                    new Column("c", Type.INT), new Column("d", Type.VARCHAR));
        }
    }

    private static List<Column> getPartitionedTransformTableSchema(String tblName) {
        return ImmutableList.of(new Column("id", Type.INT, true),
                new Column("data", Type.STRING, true),
                new Column("ts", Type.DATETIME, true));
    }

    private static Schema getIcebergPartitionSchema(String tblName) {
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

    private static Schema getIcebergPartitionTransformSchema(String tblName) {
        return new Schema(required(3, "id", Types.IntegerType.get()),
                required(4, "data", Types.StringType.get()),
                required(5, "ts", Types.TimestampType.withoutZone()));
    }

    private static TestTables.TestTable getPartitionIdentityTable(String tblName, Schema schema) throws IOException {
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

    private static TestTables.TestTable getPartitionTransformTable(String tblName, Schema schema) throws IOException {
        switch (tblName) {
            case MOCKED_PARTITIONED_YEAR_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).year("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_YEAR_TABLE_NAME), MOCKED_PARTITIONED_YEAR_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_MONTH_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).month("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_MONTH_TABLE_NAME), MOCKED_PARTITIONED_MONTH_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_DAY_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).day("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_DAY_TABLE_NAME), MOCKED_PARTITIONED_DAY_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_HOUR_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).hour("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_HOUR_TABLE_NAME), MOCKED_PARTITIONED_HOUR_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_BUCKET_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).bucket("ts", 10).build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_BUCKET_TABLE_NAME), MOCKED_PARTITIONED_BUCKET_TABLE_NAME,
                        schema, spec, 1);
            }
        }
        return null;
    }

    public static List<String> getTransformTablePartitionNames(String tblName) {
        switch (tblName) {
            case MOCKED_PARTITIONED_YEAR_TABLE_NAME:
                return Lists.newArrayList("ts_year=2019", "ts_year=2020",
                        "ts_year=2021", "ts_year=2022", "ts_year=2023");
            case MOCKED_PARTITIONED_MONTH_TABLE_NAME:
                return Lists.newArrayList("ts_month=2022-01", "ts_month=2022-02",
                        "ts_month=2022-03", "ts_month=2022-04", "ts_month=2022-05");
            case MOCKED_PARTITIONED_DAY_TABLE_NAME:
                return Lists.newArrayList("ts_day=2022-01-01", "ts_day=2022-01-02",
                        "ts_day=2022-01-03", "ts_day=2022-01-04", "ts_day=2022-01-05");
            case MOCKED_PARTITIONED_HOUR_TABLE_NAME:
                return Lists.newArrayList("ts_hour=2022-01-01-00", "ts_hour=2022-01-01-01",
                        "ts_hour=2022-01-01-02", "ts_hour=2022-01-01-03", "ts_hour=2022-01-01-04");
            case MOCKED_PARTITIONED_BUCKET_TABLE_NAME:
                return Lists.newArrayList("ts_bucket=0", "ts_bucket=1",
                        "ts_bucket=2", "ts_bucket=3", "ts_bucket=4");
        }
        return null;
    }

    public static MockIcebergTable getPartitionIcebergTable(String tblName, List<Column> schemas) throws IOException {
        Schema schema = getIcebergPartitionSchema(tblName);
        TestTables.TestTable baseTable = getPartitionIdentityTable(tblName, schema);

        String tableIdentifier = Joiner.on(":").join(tblName, UUID.randomUUID());
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put(IcebergMetadata.FILE_FORMAT, "orc");
        icebergProperties.put(IcebergMetadata.COMPRESSION_CODEC, "gzip");
        MockIcebergTable icebergTable = new MockIcebergTable(tblName.hashCode(), tblName, MOCKED_ICEBERG_CATALOG_NAME,
                null, MOCKED_PARTITIONED_DB_NAME, tblName, schemas, baseTable, icebergProperties,
                tableIdentifier);
        icebergTable.setComment("partitioned table");
        return icebergTable;
    }

    public static MockIcebergTable getPartitionTransformIcebergTable(String tblName, List<Column> schemas)
            throws IOException {
        Schema schema = getIcebergPartitionTransformSchema(tblName);
        TestTables.TestTable baseTable = getPartitionTransformTable(tblName, schema);

        String tableIdentifier = Joiner.on(":").join(tblName, UUID.randomUUID());
        return new MockIcebergTable(tblName.hashCode(), tblName, MOCKED_ICEBERG_CATALOG_NAME,
                null, MOCKED_PARTITIONED_TRANSFORMS_DB_NAME, tblName, schemas, baseTable, null,
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
    public List<PartitionInfo> getPartitions(com.starrocks.catalog.Table table, List<String> partitionNames) {
        IcebergTable icebergTable = (IcebergTable) table;
        readLock();
        try {
            Map<String, PartitionInfo> partitionInfoMap = MOCK_TABLE_MAP.get(icebergTable.getRemoteDbName()).
                    get(icebergTable.getRemoteTableName()).partitionInfoMap;
            if (icebergTable.isUnPartitioned()) {
                return Lists.newArrayList(partitionInfoMap.get(icebergTable.getRemoteTableName()));
            } else {
                return partitionNames.stream().map(partitionInfoMap::get).collect(Collectors.toList());
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, com.starrocks.catalog.Table table,
                                         Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate, long limit) {
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

    @Override
    public List<PartitionKey> getPrunedPartitions(com.starrocks.catalog.Table table, ScalarOperator predicate, long limit) {
        return new ArrayList<>();
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

    public void updatePartitions(String dbName, String tableName, List<String> partitionNames) {
        writeLock();
        try {
            Map<String, PartitionInfo> partitionInfoMap = MOCK_TABLE_MAP.get(dbName).get(tableName).partitionInfoMap;
            for (String partitionName : partitionNames) {
                if (partitionInfoMap.containsKey(partitionName)) {
                    long modifyTime = partitionInfoMap.get(partitionName).getModifiedTime() + 1;
                    partitionInfoMap.put(partitionName, new Partition(modifyTime));
                } else {
                    partitionInfoMap.put(partitionName, new Partition(PARTITION_INIT_VERSION));
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private static class IcebergTableInfo {
        private MockIcebergTable icebergTable;
        private final List<String> partitionNames;
        private final Map<String, PartitionInfo> partitionInfoMap;
        private final long rowCount;
        private final Map<String, ColumnStatistic> columnStatsMap;

        public IcebergTableInfo(MockIcebergTable icebergTable, List<String> partitionNames,
                                long rowCount, Map<String, ColumnStatistic> columnStatsMap) {
            this.icebergTable = icebergTable;
            this.partitionNames = partitionNames;
            this.partitionInfoMap = Maps.newHashMap();
            this.rowCount = rowCount;
            this.columnStatsMap = columnStatsMap;
            initPartitionInfos(partitionNames);
        }

        private void initPartitionInfos(List<String> partitionNames) {
            if (partitionNames.isEmpty()) {
                partitionInfoMap.put(icebergTable.getRemoteTableName(), new Partition(PARTITION_INIT_VERSION));
            } else {
                for (String partitionName : partitionNames) {
                    partitionInfoMap.put(partitionName, new Partition(PARTITION_INIT_VERSION));
                }
            }
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
