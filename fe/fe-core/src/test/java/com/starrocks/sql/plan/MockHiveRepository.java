// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.clearspring.analytics.util.Lists;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.external.hive.HiveTableStats;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class MockHiveRepository extends HiveRepository {
    // repository -> db -> tableName -> table
    private static final Map<String, Map<String, Map<String, HiveTableInfo>>> MOCK_TABLE_MAP = Maps.newHashMap();
    private final AtomicLong partitionIdGen = new AtomicLong(0L);

    static {
        mockTPCHTable();
        mockPartitionTable();
    }

    private static class HiveTableInfo {
        public final Table table;
        public final List<PartitionKey> partitionKeyList;
        public final List<HivePartition> partitionList;
        public final HiveTableStats tableStats;
        public final Map<String, HiveColumnStats> columnStatsMap;

        public HiveTableInfo(Table table, List<PartitionKey> partitionKeyList, List<HivePartition> partitionList,
                             HiveTableStats tableStats) {
            this(table, partitionKeyList, partitionList, tableStats, Maps.newHashMap());
        }

        public HiveTableInfo(Table table, List<PartitionKey> partitionKeyList, List<HivePartition> partitionList,
                             HiveTableStats tableStats, Map<String, HiveColumnStats> columnStatsMap) {
            Preconditions.checkState(partitionKeyList.size() == partitionList.size());
            this.table = table;
            this.partitionKeyList = partitionKeyList;
            this.partitionList = partitionList;
            this.tableStats = tableStats;
            if (columnStatsMap.isEmpty()) {
                List<String> colNames = table.getSd().getCols().stream().map(FieldSchema::getName).
                        collect(Collectors.toList());
                columnStatsMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                        col -> HiveColumnStats.UNKNOWN));
            }
            this.columnStatsMap = columnStatsMap;
        }
    }

    @Override
    public Table getTable(String resourceName, String dbName, String tableName) throws DdlException {
        return MOCK_TABLE_MAP.get(resourceName).get(dbName).get(tableName).table;
    }

    @Override
    public ImmutableMap<PartitionKey, Long> getPartitionKeys(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        List<PartitionKey> partitionKeyList =  MOCK_TABLE_MAP.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                get(hmsTable.getTable()).partitionKeyList;
        Map<PartitionKey, Long> result = Maps.newHashMap();
        for (PartitionKey pk : partitionKeyList) {
            result.put(pk, partitionIdGen.getAndIncrement());
        }
        return ImmutableMap.copyOf(result);
    }

    @Override
    public HiveTableStats getTableStats(String resourceName, String dbName, String tableName) throws DdlException {
        return MOCK_TABLE_MAP.get(resourceName).get(dbName).get(tableName).tableStats;
    }

    @Override
    public List<HivePartition> getPartitions(HiveMetaStoreTableInfo hmsTable, List<PartitionKey> partitionKeys) {
        List<PartitionKey> partitionKeyList = MOCK_TABLE_MAP.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                get(hmsTable.getTable()).partitionKeyList;
        List<HivePartition> partitionList = MOCK_TABLE_MAP.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                get(hmsTable.getTable()).partitionList;

        List<HivePartition> result = Lists.newArrayList();
        for (PartitionKey pk : partitionKeys) {
            int index = partitionKeyList.indexOf(pk);
            result.add(partitionList.get(index));
        }
        return result;
    }

    @Override
    public ImmutableMap<String, HiveColumnStats> getTableLevelColumnStats(HiveMetaStoreTableInfo hmsTable)
            throws DdlException {
        return ImmutableMap.copyOf(MOCK_TABLE_MAP.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                get(hmsTable.getTable()).columnStatsMap);
    }

    public static void mockTPCHTable() {
        String resourceName = "hive0";
        String dbName = "tpch_100g";

        Map<String, HiveTableInfo> mockTables = Maps.newHashMap();
        Map<String, Map<String, HiveTableInfo>> mockDbTables = Maps.newHashMap();
        mockDbTables.put(dbName, mockTables);

        // Mock table region
        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("r_regionkey", "int", null));
        cols.add(new FieldSchema("r_name", "string", null));
        cols.add(new FieldSchema("r_comment", "string", null));
        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, HiveColumnStats> regionStats = new CaseInsensitiveMap<>();
        regionStats.put("r_regionkey", new HiveColumnStats(0, 4, 0, 4, 5));
        regionStats.put("r_name", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 6.8, 5));
        regionStats.put("r_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 66, 5));
        Table region = new Table("region", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(region.getTableName(), new HiveTableInfo(region, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(5, 1149), regionStats));

        // Mock table nation
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("n_nationkey", "int", null));
        cols.add(new FieldSchema("n_name", "string", null));
        cols.add(new FieldSchema("n_regionkey", "int", null));
        cols.add(new FieldSchema("n_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        Map<String, HiveColumnStats> nationStats = new CaseInsensitiveMap<>();
        nationStats.put("n_nationkey", new HiveColumnStats(0, 24, 0, 4, 25));
        nationStats.put("n_name", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 25));
        nationStats.put("n_regionkey", new HiveColumnStats(0, 4, 0, 4, 5));
        nationStats.put("n_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 0, 25));
        Table nation = new Table("nation", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(nation.getTableName(), new HiveTableInfo(nation, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(25, 3265), nationStats));

        // Mock table supplier
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("s_suppkey", "int", null));
        cols.add(new FieldSchema("s_name", "string", null));
        cols.add(new FieldSchema("s_address", "string", null));
        cols.add(new FieldSchema("s_nationkey", "int", null));
        cols.add(new FieldSchema("s_phone", "string", null));
        cols.add(new FieldSchema("s_acctbal", "decimal", null));
        cols.add(new FieldSchema("s_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, HiveColumnStats> supplierStats = new CaseInsensitiveMap<>();
        supplierStats.put("s_suppkey", new HiveColumnStats(1, 1000000.0, 0, 4, 1000000));
        supplierStats.put("s_name", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 1000000));
        supplierStats.put("s_address", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 40, 1000000));
        supplierStats.put("s_nationkey", new HiveColumnStats(0, 24, 0, 4, 25));
        supplierStats.put("s_phone", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 1000000));
        supplierStats.put("s_acctbal", new HiveColumnStats(-998.22, 9999.72, 0, 8, 656145));
        supplierStats.put("s_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 101, 984748));
        Table suppler = new Table("supplier", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(suppler.getTableName(), new HiveTableInfo(suppler, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(1000000, 148139427), supplierStats));

        // Mock table part
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("p_partkey", "int", null));
        cols.add(new FieldSchema("p_name", "string", null));
        cols.add(new FieldSchema("p_mfgr", "string", null));
        cols.add(new FieldSchema("p_brand", "string", null));
        cols.add(new FieldSchema("p_type", "string", null));
        cols.add(new FieldSchema("p_size", "int", null));
        cols.add(new FieldSchema("p_container", "string", null));
        cols.add(new FieldSchema("p_retailprice", "decimal", null));
        cols.add(new FieldSchema("p_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, HiveColumnStats> partStats = new CaseInsensitiveMap<>();
        partStats.put("p_partkey", new HiveColumnStats(1, 20000000, 0, 8, 20000000));
        partStats.put("p_name", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 55, 20000000));
        partStats.put("p_mfgr", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 5));
        partStats.put("p_brand", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 25));
        partStats.put("p_type", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 150));
        partStats.put("p_size", new HiveColumnStats(1, 50, 0, 4, 50));
        partStats.put("p_container", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 40));
        partStats.put("p_retailprice", new HiveColumnStats(901, 2098.99, 0, 8, 120039));
        partStats.put("p_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 0, 3927659));
        Table part = new Table("part", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(part.getTableName(), new HiveTableInfo(part, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(20000000, 1374639165), partStats));

        // Mock table partsupp
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("ps_partkey", "int", null));
        cols.add(new FieldSchema("ps_suppkey", "int", null));
        cols.add(new FieldSchema("ps_availqty", "int", null));
        cols.add(new FieldSchema("ps_supplycost", "decimal", null));
        cols.add(new FieldSchema("ps_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, HiveColumnStats> partSuppStats = new CaseInsensitiveMap<>();
        partSuppStats.put("ps_partkey", new HiveColumnStats(1, 20000000, 0, 8, 20000000));
        partSuppStats.put("ps_suppkey", new HiveColumnStats(1, 1000000, 0, 8, 1000000));
        partSuppStats.put("ps_availqty", new HiveColumnStats(1, 9999, 0, 4, 9999));
        partSuppStats.put("ps_supplycost", new HiveColumnStats(1, 1000, 0, 8, 99864));
        partSuppStats.put("ps_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 199, 71873944));
        Table partSupp = new Table("partsupp", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(partSupp.getTableName(), new HiveTableInfo(partSupp, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(80000000, 11484003063L), partSuppStats));

        // Mock customer table
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("c_custkey", "int", null));
        cols.add(new FieldSchema("c_name", "string", null));
        cols.add(new FieldSchema("c_address", "string", null));
        cols.add(new FieldSchema("c_nationkey", "int", null));
        cols.add(new FieldSchema("c_phone", "string", null));
        cols.add(new FieldSchema("c_acctbal", "decimal", null));
        cols.add(new FieldSchema("c_mktsegment", "string", null));
        cols.add(new FieldSchema("c_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        Map<String, HiveColumnStats> customerStats = new CaseInsensitiveMap<>();
        customerStats.put("c_custkey", new HiveColumnStats(1, 15000000, 0, 8, 15000000));
        customerStats.put("c_name", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 15000000));
        customerStats.put("c_address", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 40, 15000000));
        customerStats.put("c_nationkey", new HiveColumnStats(0, 24, 0, 4, 25));
        customerStats.put("c_phone", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 15000000));
        customerStats.put("c_acctbal", new HiveColumnStats(-999.99, 9999.99, 0, 8, 1086564));
        customerStats.put("c_mktsegment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 5));
        customerStats.put("c_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 117, 14788744));
        Table customer = new Table("customer", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(customer.getTableName(), new HiveTableInfo(customer, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(15000000, 2377664867L), customerStats));

        // Mock table orders
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("o_orderkey", "int", null));
        cols.add(new FieldSchema("o_custkey", "int", null));
        cols.add(new FieldSchema("o_orderstatus", "string", null));
        cols.add(new FieldSchema("o_totalprice", "decimal", null));
        cols.add(new FieldSchema("o_orderdate", "date", null));
        cols.add(new FieldSchema("o_orderpriority", "string", null));
        cols.add(new FieldSchema("o_clerk", "string", null));
        cols.add(new FieldSchema("o_shippriority", "int", null));
        cols.add(new FieldSchema("o_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());

        CaseInsensitiveMap<String, HiveColumnStats> ordersStats = new CaseInsensitiveMap<>();
        ordersStats.put("o_orderkey", new HiveColumnStats(1, 600000000, 0, 8, 150000000));
        ordersStats.put("o_custkey", new HiveColumnStats(1, 150000000, 0, 8, 10031873));
        ordersStats.put("o_orderstatus", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 3));
        ordersStats.put("o_totalprice", new HiveColumnStats(811.73, 591036.15, 0, 8, 34696580));
        ordersStats.put("o_orderdate", new HiveColumnStats(getLongFromDateTime(
                DateUtils.parseStringWithDefaultHSM("1992-01-01", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-08-02",
                DateUtils.DATE_FORMATTER)), 0, 4, 2412));
        ordersStats.put("o_orderpriority", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 5));
        ordersStats.put("o_clerk", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 100836));
        ordersStats.put("o_shippriority", new HiveColumnStats(0, 0, 0, 4, 1));
        ordersStats.put("o_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 79, 110204136));
        Table orders = new Table("orders", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(orders.getTableName(), new HiveTableInfo(orders, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(150000000, 13280953753L), ordersStats));

        // Mock table lineitem
        cols = Lists.newArrayList();
        cols.add(new FieldSchema("l_orderkey", "int", null));
        cols.add(new FieldSchema("l_partkey", "int", null));
        cols.add(new FieldSchema("l_suppkey", "int", null));
        cols.add(new FieldSchema("l_linenumber", "int", null));
        cols.add(new FieldSchema("l_quantity", "decimal", null));
        cols.add(new FieldSchema("l_extendedprice", "decimal", null));
        cols.add(new FieldSchema("l_discount", "decimal", null));
        cols.add(new FieldSchema("l_tax", "decimal", null));
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

        Map<String, HiveColumnStats> lineitemStats = new CaseInsensitiveMap<>();
        lineitemStats.put("l_orderkey", new HiveColumnStats(1, 600000000, 0, 8, 150000000));
        lineitemStats.put("l_partkey", new HiveColumnStats(1, 20000000, 0, 8, 20000000));
        lineitemStats.put("l_suppkey", new HiveColumnStats(1, 1000000, 0, 4, 1000000));
        lineitemStats.put("l_linenumber", new HiveColumnStats(1, 7, 0, 4, 7));
        lineitemStats.put("l_quantity", new HiveColumnStats(1, 50, 0, 8, 50));
        lineitemStats.put("l_extendedprice", new HiveColumnStats(901, 104949.5, 0, 8, 3736520));
        lineitemStats.put("l_discount", new HiveColumnStats(0, 0.1, 0, 8, 11));
        lineitemStats.put("l_tax", new HiveColumnStats(0, 0.08, 0, 8, 9));
        lineitemStats.put("l_returnflag", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 3));
        lineitemStats.put("l_linestatus", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 2));
        lineitemStats.put("l_shipdate", new HiveColumnStats(getLongFromDateTime(
                DateUtils.parseStringWithDefaultHSM("1992-01-02", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-12-01", DateUtils.DATE_FORMATTER)),
                0, 4, 2526));
        lineitemStats.put("l_commitdate", new HiveColumnStats(getLongFromDateTime(
                DateUtils.parseStringWithDefaultHSM("1992-01-31", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-10-31", DateUtils.DATE_FORMATTER)),
                0, 4, 2466));
        lineitemStats.put("l_receiptdate", new HiveColumnStats(getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                "1992-01-03", DateUtils.DATE_FORMATTER)),
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("1998-12-31", DateUtils.DATE_FORMATTER)),
                0, 4, 2554));
        lineitemStats.put("l_shipinstruct", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 4));
        lineitemStats.put("l_shipmode", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 7));
        lineitemStats.put("l_comment", new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 44, 142089728));
        Table lineitem = new Table("lineitem", "tpch", null, 0, 0, 0,  sd,
                Lists.newArrayList(), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
        mockTables.put(lineitem.getTableName(), new HiveTableInfo(lineitem, ImmutableList.of(new PartitionKey()),
                ImmutableList.of(new HivePartition(null, ImmutableList.of(), null)),
                new HiveTableStats(600037902, 45585436421L), lineitemStats));

        MOCK_TABLE_MAP.put(resourceName, mockDbTables);
    }

    public static void mockPartitionTable() {
        String resourceName = "hive0";
        String dbName = "partitioned_db";

        MOCK_TABLE_MAP.putIfAbsent(resourceName, Maps.newHashMap());
        Map<String, Map<String, HiveTableInfo>> mockDbTables = MOCK_TABLE_MAP.get(resourceName);
        mockDbTables.putIfAbsent(dbName, Maps.newHashMap());
        Map<String, HiveTableInfo> mockTables = mockDbTables.get(dbName);

        List<FieldSchema> cols = Lists.newArrayList();
        cols.add(new FieldSchema("c1", "int", null));
        cols.add(new FieldSchema("c2", "string", null));
        cols.add(new FieldSchema("c3", "string", null));
        StorageDescriptor sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());
        Table t1 = new Table("t1", "partitioned_db", null, 0, 0, 0,  sd,
                ImmutableList.of(new FieldSchema("par_col", "string", null)), Maps.newHashMap(),
                null, null, "EXTERNAL_TABLE");

        List<PartitionKey> partitionKeyList = Lists.newArrayList();
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(0)), ImmutableList.of(PrimitiveType.INT)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(1)), ImmutableList.of(PrimitiveType.INT)));
        partitionKeyList.add(new PartitionKey(ImmutableList.of(new IntLiteral(2)), ImmutableList.of(PrimitiveType.INT)));

        List<HivePartition> partitionList = Lists.newArrayList();
        partitionKeyList.forEach(key -> partitionList.add(new HivePartition(null, ImmutableList.of(), null)));

        mockTables.put(t1.getTableName(), new HiveTableInfo(t1, partitionKeyList, partitionList,
                new HiveTableStats(100, 1149)));

        cols = Lists.newArrayList();
        cols.add(new FieldSchema("l_orderkey", "int", null));
        cols.add(new FieldSchema("l_partkey", "int", null));
        cols.add(new FieldSchema("l_suppkey", "int", null));
        cols.add(new FieldSchema("l_linenumber", "int", null));
        cols.add(new FieldSchema("l_quantity", "decimal", null));
        cols.add(new FieldSchema("l_extendedprice", "decimal", null));
        cols.add(new FieldSchema("l_discount", "decimal", null));
        cols.add(new FieldSchema("l_tax", "decimal", null));
        cols.add(new FieldSchema("l_returnflag", "string", null));
        cols.add(new FieldSchema("l_linestatus", "string", null));
        cols.add(new FieldSchema("l_commitdate", "date", null));
        cols.add(new FieldSchema("l_receiptdate", "date", null));
        cols.add(new FieldSchema("l_shipinstruct", "string", null));
        cols.add(new FieldSchema("l_shipmode", "string", null));
        cols.add(new FieldSchema("l_comment", "string", null));
        sd = new StorageDescriptor(cols, "", "",  "", false, -1, null, Lists.newArrayList(),
                Lists.newArrayList(), Maps.newHashMap());
        Table lineItemPar = new Table("lineitem_par", "partitioned_db", null, 0, 0, 0,  sd,
                ImmutableList.of(new FieldSchema("l_shipdate", "Date", null)), Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
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

        List<HivePartition> lineitemPartitionList = Lists.newArrayList();
        lineitemPartitionKeyList.forEach(key -> lineitemPartitionList.add(new HivePartition(null, ImmutableList.of(), null)));

        mockTables.put(lineItemPar.getTableName(), new HiveTableInfo(lineItemPar, lineitemPartitionKeyList, lineitemPartitionList,
                new HiveTableStats(600037902, 45585436421L)));
    }
}
