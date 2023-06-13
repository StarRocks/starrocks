// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.DateUtils;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.common.util.DateUtils.DATE_FORMATTER_UNIX;
import static com.starrocks.connector.hive.CachingHiveMetastore.createCatalogLevelInstance;
import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class MockedHiveMetadata implements ConnectorMetadata {
    // db -> tableName -> table
    private static final Map<String, Map<String, HiveTableInfo>> MOCK_TABLE_MAP = new CaseInsensitiveMap<>();
    private final AtomicLong idGen = new AtomicLong(0L);
    private static final List<RemoteFileInfo> MOCKED_FILES = ImmutableList.of(
            new RemoteFileInfo(RemoteFileInputFormat.ORC, ImmutableList.of(), null));
    public static final String MOCKED_HIVE_CATALOG_NAME = "hive0";
    public static final String MOCKED_TPCH_DB_NAME = "tpch";
    public static final String MOCKED_PARTITIONED_DB_NAME = "partitioned_db";
    public static final String MOCKED_PARTITIONED_DB_NAME2 = "partitioned_db2";

    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    static {
        mockTPCHTable();
        mockPartitionTable();
    }

    @Override
    public com.starrocks.catalog.Table getTable(String dbName, String tblName) {
        readLock();
        try {
            return MOCK_TABLE_MAP.get(dbName).get(tblName).table;
        } finally {
            readUnlock();
        }
    }

    @Override
    public Database getDb(String dbName) {
        return new Database(idGen.getAndIncrement(), dbName);
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
    public Statistics getTableStatistics(OptimizerContext session,
                                         com.starrocks.catalog.Table table,
                                         List<ColumnRefOperator> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
        String hiveDb = hmsTable.getDbName();
        String tblName = hmsTable.getTableName();

        readLock();
        try {
            HiveTableInfo info = MOCK_TABLE_MAP.get(hiveDb).get(tblName);
            Statistics.Builder builder = Statistics.builder();
            builder.setOutputRowCount(info.rowCount);
            for (ColumnRefOperator columnRefOperator : columns) {
                ColumnStatistic columnStatistic = info.columnStatsMap.get(columnRefOperator.getName());
                builder.addColumnStatistic(columnRefOperator, columnStatistic);
            }
            return builder.build();
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(com.starrocks.catalog.Table table, List<PartitionKey> partitionKeys,
                                                   ScalarOperator predicate, List<String> fieldNames) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        int size = partitionKeys.size();
        readLock();
        try {
            return MOCK_TABLE_MAP.get(hmsTbl.getDbName()).get(hmsTbl.getTableName()).remoteFileInfos.subList(0, size);
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(com.starrocks.catalog.Table table, List<String> partitionNames) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        readLock();
        try {
            Map<String, PartitionInfo> partitionInfoMap =
                    MOCK_TABLE_MAP.get(hmsTbl.getDbName()).get(hmsTbl.getTableName()).
                            partitionInfoMap;
            if (hmsTbl.isUnPartitioned()) {
                return Lists.newArrayList(partitionInfoMap.get(hmsTbl.getTableName()));
            } else {
                return partitionNames.stream().map(partitionInfoMap::get).collect(Collectors.toList());
            }
        } finally {
            readUnlock();
        }
    }

    public void dropPartition(String dbName, String tableName, String partitionName) {
        MOCK_TABLE_MAP.get(dbName).get(tableName).partitionNames.remove(partitionName);
    }

    public void addPartition(String dbName, String tableName, String partitionName) {
        HiveTableInfo hiveTableInfo = MOCK_TABLE_MAP.get(dbName).get(tableName);
        if (hiveTableInfo == null) {
            return;
        }
        hiveTableInfo.partitionNames.add(partitionName);
        hiveTableInfo.remoteFileInfos.add(new RemoteFileInfo(RemoteFileInputFormat.ORC, ImmutableList.of(), null));
        hiveTableInfo.partitionInfoMap.put(partitionName, new Partition(ImmutableMap.of(Partition.TRANSIENT_LAST_DDL_TIME,
                String.valueOf(System.currentTimeMillis() / 1000)), null, null, null, false));
    }

    public void updatePartitions(String dbName, String tableName, List<String> partitionNames) {
        writeLock();
        try {
            Map<String, PartitionInfo> partitionInfoMap = MOCK_TABLE_MAP.get(dbName).get(tableName).partitionInfoMap;
            for (String partitionName : partitionNames) {
                if (partitionInfoMap.containsKey(partitionName)) {
                    long modifyTime = partitionInfoMap.get(partitionName).getModifiedTime() + 1;
                    partitionInfoMap.put(partitionName, new Partition(ImmutableMap.of(Partition.TRANSIENT_LAST_DDL_TIME,
                            String.valueOf(modifyTime)), null, null, null, false));
                } else {
                    partitionInfoMap.put(partitionName, new Partition(ImmutableMap.of(Partition.TRANSIENT_LAST_DDL_TIME,
                            String.valueOf(System.currentTimeMillis() / 1000)), null, null, null, false));
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void updateTable(String dbName, String tableName) {
        writeLock();
        try {
            Map<String, PartitionInfo> partitionInfoMap = MOCK_TABLE_MAP.get(dbName).get(tableName).partitionInfoMap;
            if (partitionInfoMap.containsKey(tableName)) {
                long modifyTime = partitionInfoMap.get(tableName).getModifiedTime() + 1;
                partitionInfoMap.put(tableName, new Partition(ImmutableMap.of(Partition.TRANSIENT_LAST_DDL_TIME,
                        String.valueOf(modifyTime)), null, null, null, false));
            } else {
                partitionInfoMap.put(tableName, new Partition(ImmutableMap.of(Partition.TRANSIENT_LAST_DDL_TIME,
                        String.valueOf(System.currentTimeMillis() / 1000)), null, null, null, false));
            }
        } finally {
            writeUnlock();
        }
    }

    public static void mockTPCHTable() {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_TPCH_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, HiveTableInfo> mockTables = MOCK_TABLE_MAP.get(MOCKED_TPCH_DB_NAME);
        MOCK_TABLE_MAP.put(MOCKED_TPCH_DB_NAME, mockTables);

        // Mock table region
        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("r_regionkey", "int", null));
        cols.add(new FieldSchema("r_name", "string", null));
        cols.add(new FieldSchema("r_comment", "string", null));
        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, ColumnStatistic> regionStats = new CaseInsensitiveMap<>();
        regionStats.put("r_regionkey", new ColumnStatistic(0, 4, 0, 4, 5));
        regionStats.put("r_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 6.8, 5));
        regionStats.put("r_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 66, 5));

        Table region = new Table("region", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(region.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(region,
                MOCKED_HIVE_CATALOG_NAME), ImmutableList.of(), 5, regionStats, MOCKED_FILES));

        // Mock table nation
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("n_nationkey", "int", null));
        cols.add(new FieldSchema("n_name", "string", null));
        cols.add(new FieldSchema("n_regionkey", "int", null));
        cols.add(new FieldSchema("n_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        Map<String, ColumnStatistic> nationStats = new CaseInsensitiveMap<>();
        nationStats.put("n_nationkey", new ColumnStatistic(0, 24, 0, 4, 25));
        nationStats.put("n_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 25));
        nationStats.put("n_regionkey", new ColumnStatistic(0, 4, 0, 4, 5));
        nationStats.put("n_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 0, 25));
        Table nation = new Table("nation", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(nation.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(nation,
                MOCKED_HIVE_CATALOG_NAME), ImmutableList.of(), 25, nationStats, MOCKED_FILES));

        // Mock table supplier
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("s_suppkey", "int", null));
        cols.add(new FieldSchema("s_name", "string", null));
        cols.add(new FieldSchema("s_address", "string", null));
        cols.add(new FieldSchema("s_nationkey", "int", null));
        cols.add(new FieldSchema("s_phone", "string", null));
        cols.add(new FieldSchema("s_acctbal", "decimal(15,2)", null));
        cols.add(new FieldSchema("s_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, ColumnStatistic> supplierStats = new CaseInsensitiveMap<>();
        supplierStats.put("s_suppkey", new ColumnStatistic(1, 1000000.0, 0, 4, 1000000));
        supplierStats.put("s_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 1000000));
        supplierStats.put("s_address", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 40, 1000000));
        supplierStats.put("s_nationkey", new ColumnStatistic(0, 24, 0, 4, 25));
        supplierStats.put("s_phone", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 1000000));
        supplierStats.put("s_acctbal", new ColumnStatistic(-998.22, 9999.72, 0, 8, 656145));
        supplierStats.put("s_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 101, 984748));
        Table suppler = new Table("supplier", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(suppler.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(suppler,
                MOCKED_HIVE_CATALOG_NAME), ImmutableList.of(), 1000000, supplierStats, MOCKED_FILES));

        // Mock table part
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("p_partkey", "int", null));
        cols.add(new FieldSchema("p_name", "string", null));
        cols.add(new FieldSchema("p_mfgr", "string", null));
        cols.add(new FieldSchema("p_brand", "string", null));
        cols.add(new FieldSchema("p_type", "string", null));
        cols.add(new FieldSchema("p_size", "int", null));
        cols.add(new FieldSchema("p_container", "string", null));
        cols.add(new FieldSchema("p_retailprice", "decimal(15,2)", null));
        cols.add(new FieldSchema("p_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, ColumnStatistic> partStats = new CaseInsensitiveMap<>();
        partStats.put("p_partkey", new ColumnStatistic(1, 20000000, 0, 8, 20000000));
        partStats.put("p_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 55, 20000000));
        partStats.put("p_mfgr", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 5));
        partStats.put("p_brand", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 25));
        partStats.put("p_type", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 150));
        partStats.put("p_size", new ColumnStatistic(1, 50, 0, 4, 50));
        partStats.put("p_container", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 40));
        partStats.put("p_retailprice", new ColumnStatistic(901, 2098.99, 0, 8, 120039));
        partStats.put("p_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 0, 3927659));
        Table part = new Table("part", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        HiveTableInfo hiveTableInfo = new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(part, MOCKED_HIVE_CATALOG_NAME),
                ImmutableList.of(), 20000000, partStats, MOCKED_FILES);
        mockTables.put(part.getTableName(), hiveTableInfo);

        // Mock table partsupp
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("ps_partkey", "int", null));
        cols.add(new FieldSchema("ps_suppkey", "int", null));
        cols.add(new FieldSchema("ps_availqty", "int", null));
        cols.add(new FieldSchema("ps_supplycost", "decimal(15,2)", null));
        cols.add(new FieldSchema("ps_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, ColumnStatistic> partSuppStats = new CaseInsensitiveMap<>();
        partSuppStats.put("ps_partkey", new ColumnStatistic(1, 20000000, 0, 8, 20000000));
        partSuppStats.put("ps_suppkey", new ColumnStatistic(1, 1000000, 0, 8, 1000000));
        partSuppStats.put("ps_availqty", new ColumnStatistic(1, 9999, 0, 4, 9999));
        partSuppStats.put("ps_supplycost", new ColumnStatistic(1, 1000, 0, 8, 99864));
        partSuppStats.put("ps_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 199, 71873944));
        Table partSupp = new Table("partsupp", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(partSupp.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(partSupp,
                MOCKED_HIVE_CATALOG_NAME), ImmutableList.of(), 80000000, partSuppStats, MOCKED_FILES));

        // Mock customer table
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("c_custkey", "int", null));
        cols.add(new FieldSchema("c_name", "string", null));
        cols.add(new FieldSchema("c_address", "string", null));
        cols.add(new FieldSchema("c_nationkey", "int", null));
        cols.add(new FieldSchema("c_phone", "string", null));
        cols.add(new FieldSchema("c_acctbal", "decimal(15,2)", null));
        cols.add(new FieldSchema("c_mktsegment", "string", null));
        cols.add(new FieldSchema("c_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        Map<String, ColumnStatistic> customerStats = new CaseInsensitiveMap<>();
        customerStats.put("c_custkey", new ColumnStatistic(1, 15000000, 0, 8, 15000000));
        customerStats.put("c_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 15000000));
        customerStats.put("c_address", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 40, 15000000));
        customerStats.put("c_nationkey", new ColumnStatistic(0, 24, 0, 4, 25));
        customerStats.put("c_phone", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 15000000));
        customerStats.put("c_acctbal", new ColumnStatistic(-999.99, 9999.99, 0, 8, 1086564));
        customerStats.put("c_mktsegment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 5));
        customerStats.put("c_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 117, 14788744));
        Table customer = new Table("customer", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(customer.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(customer,
                MOCKED_HIVE_CATALOG_NAME), ImmutableList.of(), 15000000, customerStats, MOCKED_FILES));

        // Mock table orders
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("o_orderkey", "int", null));
        cols.add(new FieldSchema("o_custkey", "int", null));
        cols.add(new FieldSchema("o_orderstatus", "string", null));
        cols.add(new FieldSchema("o_totalprice", "decimal(15,2)", null));
        cols.add(new FieldSchema("o_orderdate", "date", null));
        cols.add(new FieldSchema("o_orderpriority", "string", null));
        cols.add(new FieldSchema("o_clerk", "string", null));
        cols.add(new FieldSchema("o_shippriority", "int", null));
        cols.add(new FieldSchema("o_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, ColumnStatistic> ordersStats = new CaseInsensitiveMap<>();
        ordersStats.put("o_orderkey", new ColumnStatistic(1, 600000000, 0, 8, 150000000));
        ordersStats.put("o_custkey", new ColumnStatistic(1, 150000000, 0, 8, 10031873));
        ordersStats.put("o_orderstatus", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 3));
        ordersStats.put("o_totalprice", new ColumnStatistic(811.73, 591036.15, 0, 8, 34696580));
        ordersStats.put("o_orderdate", new ColumnStatistic(getLongFromDateTime(
                DateUtils.parseStringWithDefaultHSM("1992-01-01", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-08-02",
                        DateUtils.DATE_FORMATTER)), 0, 4, 2412));
        ordersStats.put("o_orderpriority", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 5));
        ordersStats.put("o_clerk", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 100836));
        ordersStats.put("o_shippriority", new ColumnStatistic(0, 0, 0, 4, 1));
        ordersStats.put("o_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 79, 110204136));
        Table orders = new Table("orders", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(orders.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(orders,
                MOCKED_HIVE_CATALOG_NAME), ImmutableList.of(), 150000000, ordersStats, MOCKED_FILES));

        // Mock table lineitem
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("l_orderkey", "int", null));
        cols.add(new FieldSchema("l_partkey", "int", null));
        cols.add(new FieldSchema("l_suppkey", "int", null));
        cols.add(new FieldSchema("l_linenumber", "int", null));
        cols.add(new FieldSchema("l_quantity", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_extendedprice", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_discount", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_tax", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_returnflag", "string", null));
        cols.add(new FieldSchema("l_linestatus", "string", null));
        cols.add(new FieldSchema("l_shipdate", "date", null));
        cols.add(new FieldSchema("l_commitdate", "date", null));
        cols.add(new FieldSchema("l_receiptdate", "date", null));
        cols.add(new FieldSchema("l_shipinstruct", "string", null));
        cols.add(new FieldSchema("l_shipmode", "string", null));
        cols.add(new FieldSchema("l_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        Map<String, ColumnStatistic> lineitemStats = new CaseInsensitiveMap<>();
        lineitemStats.put("l_orderkey", new ColumnStatistic(1, 600000000, 0, 8, 150000000));
        lineitemStats.put("l_partkey", new ColumnStatistic(1, 20000000, 0, 8, 20000000));
        lineitemStats.put("l_suppkey", new ColumnStatistic(1, 1000000, 0, 4, 1000000));
        lineitemStats.put("l_linenumber", new ColumnStatistic(1, 7, 0, 4, 7));
        lineitemStats.put("l_quantity", new ColumnStatistic(1, 50, 0, 8, 50));
        lineitemStats.put("l_extendedprice", new ColumnStatistic(901, 104949.5, 0, 8, 3736520));
        lineitemStats.put("l_discount", new ColumnStatistic(0, 0.1, 0, 8, 11));
        lineitemStats.put("l_tax", new ColumnStatistic(0, 0.08, 0, 8, 9));
        lineitemStats.put("l_returnflag", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 3));
        lineitemStats.put("l_linestatus", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 2));
        lineitemStats.put("l_shipdate", new ColumnStatistic(getLongFromDateTime(
                DateUtils.parseStringWithDefaultHSM("1992-01-02", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-12-01", DateUtils.DATE_FORMATTER)),
                0, 4, 2526));
        lineitemStats.put("l_commitdate", new ColumnStatistic(getLongFromDateTime(
                DateUtils.parseStringWithDefaultHSM("1992-01-31", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-10-31", DateUtils.DATE_FORMATTER)),
                0, 4, 2466));
        lineitemStats.put("l_receiptdate", new ColumnStatistic(getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                "1992-01-03", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-12-31", DateUtils.DATE_FORMATTER)),
                0, 4, 2554));
        lineitemStats.put("l_shipinstruct", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 4));
        lineitemStats.put("l_shipmode", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 7));
        lineitemStats.put("l_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 44, 142089728));
        Table lineitem = new Table("lineitem", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(lineitem.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(lineitem,
                MOCKED_HIVE_CATALOG_NAME), ImmutableList.of(), 600037902, lineitemStats, MOCKED_FILES));
    }

    public static void mockPartitionTable() {
        mockLineItem();
        mockLineItemWithMultiPartitionColumns();
        mockT1();
        mockT2();
        mockT3();
        mockT1WithMultiPartitionColumns();
        mockOrders();
    }

    public static void mockOrders() {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, HiveTableInfo> mockTables = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_DB_NAME);

        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("o_orderkey", "int", null));
        cols.add(new FieldSchema("o_custkey", "int", null));
        cols.add(new FieldSchema("o_orderstatus", "string", null));
        cols.add(new FieldSchema("o_totalprice", "double", null));
        cols.add(new FieldSchema("o_orderpriority", "string", null));
        cols.add(new FieldSchema("o_clerk", "string", null));
        cols.add(new FieldSchema("o_shippriority", "int", null));
        cols.add(new FieldSchema("o_comment", "string", null));

        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());
        Table orders = new Table("orders", "partitioned_db", null, 0, 0, 0,  sd,
                ImmutableList.of(new FieldSchema("o_orderdate", "Date", null)), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");

        Column partitionColumn = new Column("o_orderdate", Type.DATE);

        List<PartitionKey> partitionKeyList = Lists.newArrayList();
        List<String> partitionNames = Lists.newArrayList();

        LocalDate startDate = LocalDate.of(1991, 1, 1);
        LocalDate endDate = LocalDate.of(1993, 12, 31);
        LocalDate curDate = startDate;
        while (!curDate.equals(endDate)) {
            partitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(curDate.getYear(), curDate.getMonthValue(),
                    curDate.getDayOfMonth())), ImmutableList.of(PrimitiveType.DATE)));
            String partitionName = "o_orderdate=" + curDate.format(DATE_FORMATTER_UNIX);
            partitionNames.add(partitionName);
            curDate = curDate.plusDays(1);
        }

        double rowCount = 150000000;
        double avgNumPerPartition = rowCount / partitionNames.size();
        Map<String, HivePartitionStats> hivePartitionStatsMap = Maps.newHashMap();
        List<String> partitionColumnNames = ImmutableList.of("o_orderdate");

        ColumnStatistic partitionColumnStats = getPartitionColumnStatistic(partitionColumn, partitionKeyList,
                partitionColumnNames, hivePartitionStatsMap, avgNumPerPartition, rowCount);

        List<RemoteFileInfo> remoteFileInfos = Lists.newArrayList();
        partitionNames.forEach(k -> remoteFileInfos.add(new RemoteFileInfo(RemoteFileInputFormat.ORC, ImmutableList.of(), null)));

        List<String> colNames = cols.stream().map(FieldSchema::getName).collect(Collectors.toList());
        Map<String, ColumnStatistic> columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                col -> ColumnStatistic.unknown()));
        columnStatisticMap.put("o_orderdate", partitionColumnStats);

        mockTables.put(orders.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(
                orders, MOCKED_HIVE_CATALOG_NAME), partitionNames, (long) rowCount, columnStatisticMap, remoteFileInfos));

    }

    public static void mockLineItem() {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, HiveTableInfo> mockTables = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_DB_NAME);

        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("l_orderkey", "int", null));
        cols.add(new FieldSchema("l_partkey", "int", null));
        cols.add(new FieldSchema("l_suppkey", "int", null));
        cols.add(new FieldSchema("l_linenumber", "int", null));
        cols.add(new FieldSchema("l_quantity", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_extendedprice", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_discount", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_tax", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_returnflag", "string", null));
        cols.add(new FieldSchema("l_linestatus", "string", null));
        cols.add(new FieldSchema("l_commitdate", "date", null));
        cols.add(new FieldSchema("l_receiptdate", "date", null));
        cols.add(new FieldSchema("l_shipinstruct", "string", null));
        cols.add(new FieldSchema("l_shipmode", "string", null));
        cols.add(new FieldSchema("l_comment", "string", null));
        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());
        Table lineItemPar = new Table("lineitem_par", "partitioned_db", null, 0, 0, 0,  sd,
                ImmutableList.of(new FieldSchema("l_shipdate", "Date", null)), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");

        Column partitionColumn = new Column("l_shipdate", Type.DATE);

        List<PartitionKey> lineitemPartitionKeyList = Lists.newArrayList();
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 1)),
                ImmutableList.of(PrimitiveType.DATE)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 2)),
                ImmutableList.of(PrimitiveType.DATE)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 3)),
                ImmutableList.of(PrimitiveType.DATE)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 4)),
                ImmutableList.of(PrimitiveType.DATE)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 5)),
                ImmutableList.of(PrimitiveType.DATE)));

        List<String> partitionNames = Lists.newArrayList();
        partitionNames.addAll(ImmutableList.of("l_shipdate=" + HiveMetaClient.PARTITION_NULL_VALUE,
                "l_shipdate=1998-01-01", "l_shipdate=1998-01-02", "l_shipdate=1998-01-03",
                "l_shipdate=1998-01-04", "l_shipdate=1998-01-05"));

        List<String> partitionColumnNames = ImmutableList.of("l_shipdate");

        double rowCount = 600037902;
        double avgNumPerPartition = rowCount / partitionNames.size();
        Map<String, HivePartitionStats> hivePartitionStatsMap = Maps.newHashMap();

        ColumnStatistic partitionColumnStats = getPartitionColumnStatistic(partitionColumn, lineitemPartitionKeyList,
                partitionColumnNames, hivePartitionStatsMap, avgNumPerPartition, rowCount);

        List<RemoteFileInfo> remoteFileInfos = Lists.newArrayList();
        partitionNames.forEach(k -> remoteFileInfos.add(new RemoteFileInfo(RemoteFileInputFormat.ORC, ImmutableList.of(), null)));

        List<String> colNames = cols.stream().map(FieldSchema::getName).collect(Collectors.toList());
        Map<String, ColumnStatistic> columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                col -> ColumnStatistic.unknown()));
        columnStatisticMap.put("l_shipdate", partitionColumnStats);

        mockTables.put(lineItemPar.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(
                lineItemPar, MOCKED_HIVE_CATALOG_NAME), partitionNames, (long) rowCount, columnStatisticMap, remoteFileInfos));
    }

    public static void mockLineItemWithMultiPartitionColumns() {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, HiveTableInfo> mockTables = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_DB_NAME);

        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("l_partkey", "int", null));
        cols.add(new FieldSchema("l_suppkey", "int", null));
        cols.add(new FieldSchema("l_linenumber", "int", null));
        cols.add(new FieldSchema("l_quantity", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_extendedprice", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_discount", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_tax", "decimal(15,2)", null));
        cols.add(new FieldSchema("l_returnflag", "string", null));
        cols.add(new FieldSchema("l_linestatus", "string", null));
        cols.add(new FieldSchema("l_commitdate", "date", null));
        cols.add(new FieldSchema("l_receiptdate", "date", null));
        cols.add(new FieldSchema("l_shipinstruct", "string", null));
        cols.add(new FieldSchema("l_shipmode", "string", null));
        cols.add(new FieldSchema("l_comment", "string", null));
        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());
        Table lineItemPar = new Table("lineitem_mul_par", "partitioned_db", null, 0, 0, 0,  sd,
                ImmutableList.of(new FieldSchema("l_shipdate", "Date", null),
                        new FieldSchema("l_orderkey", "int", null)),
                Maps.newHashMap(), null, null, "EXTERNAL_TABLE");

        Column partitionColumn1 = new Column("l_shipdate", Type.DATE);
        Column partitionColumn2 = new Column("l_orderkey", Type.INT);

        List<PartitionKey> lineitemPartitionKeyList = Lists.newArrayList();
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 1), new IntLiteral(1)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 1), new IntLiteral(2)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 1), new IntLiteral(3)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 2), new IntLiteral(2)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 2), new IntLiteral(10)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 3), new IntLiteral(5)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 4), new IntLiteral(5)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));
        lineitemPartitionKeyList.add(new PartitionKey(ImmutableList.of(new DateLiteral(1998, 1, 5), new IntLiteral(1)),
                ImmutableList.of(PrimitiveType.DATE, PrimitiveType.INT)));

        List<String> partitionNames = ImmutableList.of("l_shipdate=1998-01-01/l_orderkey=1",
                "l_shipdate=1998-01-01/l_orderkey=2",
                "l_shipdate=1998-01-01/l_orderkey=3",
                "l_shipdate=1998-01-02/l_orderkey=2",
                "l_shipdate=1998-01-02/l_orderkey=10",
                "l_shipdate=1998-01-03/l_orderkey=5",
                "l_shipdate=1998-01-04/l_orderkey=5",
                "l_shipdate=1998-01-05/l_orderkey=1");

        List<String> partitionColumnNames = ImmutableList.of("l_shipdate", "l_orderkey");

        double rowCount = 600037902;
        double avgNumPerPartition = rowCount / partitionNames.size();
        Map<String, HivePartitionStats> hivePartitionStatsMap = Maps.newHashMap();

        ColumnStatistic partitionColumnStats1 = getPartitionColumnStatistic(partitionColumn1, lineitemPartitionKeyList,
                partitionColumnNames, hivePartitionStatsMap, avgNumPerPartition, rowCount);
        ColumnStatistic partitionColumnStats2 = getPartitionColumnStatistic(partitionColumn2, lineitemPartitionKeyList,
                partitionColumnNames, hivePartitionStatsMap, avgNumPerPartition, rowCount);

        List<RemoteFileInfo> remoteFileInfos = Lists.newArrayList();
        partitionNames.forEach(k -> remoteFileInfos.add(new RemoteFileInfo(RemoteFileInputFormat.ORC, ImmutableList.of(), null)));

        List<String> colNames = cols.stream().map(FieldSchema::getName).collect(Collectors.toList());
        Map<String, ColumnStatistic> columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                col -> ColumnStatistic.unknown()));
        columnStatisticMap.put("l_shipdate", partitionColumnStats1);
        columnStatisticMap.put("l_orderkey", partitionColumnStats2);

        mockTables.put(lineItemPar.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(
                lineItemPar, MOCKED_HIVE_CATALOG_NAME), partitionNames, (long) rowCount, columnStatisticMap, remoteFileInfos));
    }

    public static void mockSimpleTable(String dbName, String tableName) {
        MOCK_TABLE_MAP.putIfAbsent(dbName, new CaseInsensitiveMap<>());
        Map<String, HiveTableInfo> mockTables = MOCK_TABLE_MAP.get(dbName);

        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("c1", "int", null));
        cols.add(new FieldSchema("c2", "string", null));
        cols.add(new FieldSchema("c3", "string", null));
        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());
        Table mockTable = new Table(tableName, dbName, null, 0, 0, 0,  sd,
                ImmutableList.of(new FieldSchema("par_col", "int", null)), Maps.newHashMap(),
                null, null, "EXTERNAL_TABLE");
        List<String> partitionNames = ImmutableList.of("par_col=0", "par_col=1", "par_col=2");
        Map<String, HivePartitionStats> hivePartitionStatsMap = Maps.newHashMap();
        double avgNumPerPartition = (double) (100 / 3);
        double rowCount = 100;

        List<PartitionKey> partitionKeyList = Lists.newArrayList();
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(0)), ImmutableList.of(PrimitiveType.INT)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(1)), ImmutableList.of(PrimitiveType.INT)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(2)), ImmutableList.of(PrimitiveType.INT)));
        Column partitionColumn = new Column("par_col", Type.INT);
        List<String> partitionColumnNames = ImmutableList.of("par_col");
        ColumnStatistic partitionColumnStats = getPartitionColumnStatistic(partitionColumn, partitionKeyList,
                partitionColumnNames, hivePartitionStatsMap, avgNumPerPartition, rowCount);

        Map<String, ColumnStatistic> columnStatisticMap;
        List<String> colNames = cols.stream().map(FieldSchema::getName).collect(Collectors.toList());
        columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                col -> ColumnStatistic.unknown()));
        columnStatisticMap.put("par_col", partitionColumnStats);

        List<RemoteFileInfo> remoteFileInfos = Lists.newArrayList();
        partitionNames.forEach(k -> remoteFileInfos.add(new RemoteFileInfo(RemoteFileInputFormat.ORC, ImmutableList.of(), null)));

        mockTables.put(mockTable.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.
                toHiveTable(mockTable, MOCKED_HIVE_CATALOG_NAME), partitionNames, (long) rowCount, columnStatisticMap,
                remoteFileInfos));
    }

    public static void mockT1() {
        mockSimpleTable(MOCKED_PARTITIONED_DB_NAME, "t1");
    }

    public static void mockT2() {
        mockSimpleTable(MOCKED_PARTITIONED_DB_NAME2, "t2");
    }

    public static void mockT3() {
        mockSimpleTable(MOCKED_PARTITIONED_DB_NAME, "t3");
    }

    public static void mockT1WithMultiPartitionColumns() {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, HiveTableInfo> mockTables = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_DB_NAME);

        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("c1", "int", null));
        cols.add(new FieldSchema("c2", "string", null));
        cols.add(new FieldSchema("c3", "string", null));
        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());
        Table t1 = new Table("t1_par", "partitioned_db", null, 0, 0, 0,  sd,
                ImmutableList.of(new FieldSchema("par_col", "int", null),
                        new FieldSchema("par_date", "date", null)), Maps.newHashMap(),
                null, null, "EXTERNAL_TABLE");
        List<String> partitionNames = ImmutableList.of("par_col=0/par_date=2020-01-01",
                "par_col=0/par_date=2020-01-02",
                "par_col=0/par_date=2020-01-03",
                "par_col=1/par_date=2020-01-02",
                "par_col=1/par_date=2020-01-03",
                "par_col=3/par_date=2020-01-04");
        Map<String, HivePartitionStats> hivePartitionStatsMap = Maps.newHashMap();
        double avgNumPerPartition = (double) (100 / 3);
        double rowCount = 100;

        List<PartitionKey> partitionKeyList = Lists.newArrayList();
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(0), new DateLiteral(2020, 1, 1)),
                ImmutableList.of(PrimitiveType.INT, PrimitiveType.DATE)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(0), new DateLiteral(2020, 1, 2)),
                ImmutableList.of(PrimitiveType.INT, PrimitiveType.DATE)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(0), new DateLiteral(2020, 1, 3)),
                ImmutableList.of(PrimitiveType.INT, PrimitiveType.DATE)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(1), new DateLiteral(2020, 1, 2)),
                ImmutableList.of(PrimitiveType.INT, PrimitiveType.DATE)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(1), new DateLiteral(2020, 1, 3)),
                ImmutableList.of(PrimitiveType.INT, PrimitiveType.DATE)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(3), new DateLiteral(2020, 1, 4)),
                ImmutableList.of(PrimitiveType.INT, PrimitiveType.DATE)));

        Column partitionColumn1 = new Column("par_col", Type.INT);
        Column partitionColumn2 = new Column("par_date", Type.DATE);

        List<String> partitionColumnNames = ImmutableList.of("par_col", "par_date");
        ColumnStatistic partitionColumnStats1 = getPartitionColumnStatistic(partitionColumn1, partitionKeyList,
                partitionColumnNames, hivePartitionStatsMap, avgNumPerPartition, rowCount);
        ColumnStatistic partitionColumnStats2 = getPartitionColumnStatistic(partitionColumn2, partitionKeyList,
                partitionColumnNames, hivePartitionStatsMap, avgNumPerPartition, rowCount);

        Map<String, ColumnStatistic> columnStatisticMap;
        List<String> colNames = cols.stream().map(FieldSchema::getName).collect(Collectors.toList());
        columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                col -> ColumnStatistic.unknown()));
        columnStatisticMap.put("par_col", partitionColumnStats1);
        columnStatisticMap.put("par_date", partitionColumnStats2);

        List<RemoteFileInfo> remoteFileInfos = Lists.newArrayList();
        partitionNames.forEach(k -> remoteFileInfos.add(new RemoteFileInfo(RemoteFileInputFormat.ORC, ImmutableList.of(), null)));

        mockTables.put(t1.getTableName(), new HiveTableInfo(HiveMetastoreApiConverter.toHiveTable(t1, MOCKED_HIVE_CATALOG_NAME),
                partitionNames, (long) rowCount, columnStatisticMap, remoteFileInfos));
    }

    public static ColumnStatistic getPartitionColumnStatistic(Column partitionColumn,
                                                              List<PartitionKey> partitionKeyList,
                                                              List<String> partitionColumnNames,
                                                              Map<String, HivePartitionStats> hivePartitionStatsMap,
                                                              double avgNumPerPartition,
                                                              double rowCount) {
        HiveMetaClient metaClient = new HiveMetaClient(new HiveConf());
        HiveMetastore metastore = new HiveMetastore(metaClient, MOCKED_HIVE_CATALOG_NAME);
        CachingHiveMetastore cachingHiveMetastore = createCatalogLevelInstance(
                metastore, Executors.newSingleThreadExecutor(), 0, 0, 0, false);
        HiveMetastoreOperations hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, false);
        RemoteFileIO remoteFileIO = new HiveRemoteFileIO(new Configuration());
        CachingRemoteFileIO cacheIO = CachingRemoteFileIO.createCatalogLevelInstance(remoteFileIO,
                Executors.newSingleThreadExecutor(), 0, 0, 0);
        RemoteFileOperations fileOps = new RemoteFileOperations(cacheIO, Executors.newSingleThreadExecutor(), false, false);

        HiveStatisticsProvider hiveStatisticsProvider = new HiveStatisticsProvider(hmsOps, fileOps);
        try {
            Method method = HiveStatisticsProvider.class.getDeclaredMethod("createPartitionColumnStatistics",
                    Column.class, List.class, Map.class, List.class, double.class, double.class);
            method.setAccessible(true);
            return (ColumnStatistic) method.invoke(hiveStatisticsProvider, partitionColumn,
                    partitionKeyList, hivePartitionStatsMap, partitionColumnNames, avgNumPerPartition, rowCount);
        } catch (Exception e) {
            throw new StarRocksConnectorException("get partition statistics failed", e);
        }
    }

    private static class HiveTableInfo {
        public final com.starrocks.catalog.Table table;
        public final List<String> partitionNames;
        public final long rowCount;
        public final Map<String, ColumnStatistic> columnStatsMap;
        private final List<RemoteFileInfo> remoteFileInfos;
        private Map<String, PartitionInfo> partitionInfoMap = Maps.newHashMap();

        public HiveTableInfo(HiveTable table,
                             List<String> partitionNames,
                             long rowCount,
                             Map<String, ColumnStatistic> columnStatsMap,
                             List<RemoteFileInfo> remoteFileInfos) {
            this.table = table;
            this.partitionNames = partitionNames;
            this.rowCount = rowCount;
            this.columnStatsMap = columnStatsMap;
            this.remoteFileInfos = remoteFileInfos;
            if (partitionNames.isEmpty()) {
                this.partitionInfoMap.put(table.getTableName(),
                        new Partition(ImmutableMap.of(Partition.TRANSIENT_LAST_DDL_TIME,
                                String.valueOf(System.currentTimeMillis() / 1000)), null, null, null, false));
            } else {
                this.partitionInfoMap = partitionNames.stream().collect(Collectors.
                        toMap(k -> k, k -> new Partition(ImmutableMap.of(Partition.TRANSIENT_LAST_DDL_TIME,
                                String.valueOf(System.currentTimeMillis() / 1000)), null, null, null, false)));
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
