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


package com.starrocks.sql.plan;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.optimizer.statistics.TableStatistic;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class MockTpchStatisticStorage implements StatisticStorage {
    private final Map<Long, TableStatistic> rowCountStats;
    private final Map<String, Map<String, ColumnStatistic>> tableStatistics;

    private final int scale;

    public MockTpchStatisticStorage(ConnectContext connectContext, int scale) {
        rowCountStats = new CaseInsensitiveMap<>();
        tableStatistics = new CaseInsensitiveMap<>();
        this.scale = scale;
        mockTpchTableStats(connectContext, scale);
        mockTpchStatistics();
    }

    private void mockTpchTableStats(ConnectContext connectContext, int scale) {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Database database = globalStateMgr.getDb("test");

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("region");
        rowCountStats.put(t0.getId(), new TableStatistic(t0.getId(), t0.getPartition("region").getId(),
                5L));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t0.getId(), null,
                        StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                        Maps.newHashMap()));

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("nation");
        rowCountStats.put(t1.getId(), new TableStatistic(t1.getId(), t1.getPartition("nation").getId(),
                25L));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t1.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("supplier");
        rowCountStats.put(t2.getId(), new TableStatistic(t2.getId(), t2.getPartition("supplier").getId(),
                10000L * scale));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t2.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        OlapTable t3 = (OlapTable) globalStateMgr.getDb("test").getTable("customer");
        rowCountStats.put(t3.getId(), new TableStatistic(t3.getId(), t3.getPartition("customer").getId(),
                150000L * scale));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t3.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        OlapTable t4 = (OlapTable) globalStateMgr.getDb("test").getTable("part");
        rowCountStats.put(t4.getId(), new TableStatistic(t4.getId(), t4.getPartition("part").getId(),
                200000L * scale));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t4.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        OlapTable t5 = (OlapTable) globalStateMgr.getDb("test").getTable("partsupp");
        rowCountStats.put(t5.getId(), new TableStatistic(t5.getId(), t5.getPartition("partsupp").getId(),
                800000L * scale));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t5.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        OlapTable t6 = (OlapTable) globalStateMgr.getDb("test").getTable("orders");
        rowCountStats.put(t6.getId(), new TableStatistic(t6.getId(), t6.getPartition("orders").getId(),
                1500000L * scale));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t6.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        OlapTable t7 = (OlapTable) globalStateMgr.getDb("test").getTable("lineitem");
        rowCountStats.put(t7.getId(), new TableStatistic(t7.getId(), t7.getPartition("lineitem").getId(),
                6000000L * scale));
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(database.getId(), t7.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));
    }

    private void mockTpchStatistics() {
        Map<String, ColumnStatistic> tableCustomer = new CaseInsensitiveMap<>();
        // C_CUSTKEY   BIGINT
        tableCustomer.put("c_custkey", new ColumnStatistic(1, 150000, 0, 8, 150000));
        // C_NAME      VARCHAR(25)
        tableCustomer.put("c_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 150000));
        // C_ADDRESS   VARCHAR(40)
        tableCustomer.put("c_address", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 40, 150000));
        // C_NATIONKEY  INT
        tableCustomer.put("c_nationkey", new ColumnStatistic(0, 24, 0, 4, 25));
        // C_PHONE     CHAR(15)
        tableCustomer.put("c_phone", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 150000));
        // C_ACCTBAL  DOUBLE
        tableCustomer.put("c_acctbal", new ColumnStatistic(-999.99, 9999.99, 0, 8, 137439));
        // C_MKTSEGMENT  CHAR(10)
        tableCustomer.put("c_mktsegment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 5));
        // C_COMMENT VARCHAR(117)
        tableCustomer.put("c_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 117, 149968));
        tableStatistics.put("customer", tableCustomer);

        Map<String, ColumnStatistic> tableLineitem = new CaseInsensitiveMap<>();
        // L_ORDERKEY      BIGINT
        tableLineitem.put("l_orderkey", new ColumnStatistic(1, 6000000, 0, 8, 1500000));
        // L_PARTKEY       BIGINT
        tableLineitem.put("l_partkey", new ColumnStatistic(1, 200000, 0, 8, 200000));
        // L_SUPPKEY       INT
        tableLineitem.put("l_suppkey", new ColumnStatistic(1, 10000, 0, 4, 10000));
        // L_LINENUMBER    INT
        tableLineitem.put("l_linenumber", new ColumnStatistic(1, 7, 0, 4, 7));
        // L_QUANTITY      DOUBLE
        tableLineitem.put("l_quantity", new ColumnStatistic(1, 50, 0, 8, 50));
        // L_EXTENDEDPRICE DOUBLE
        tableLineitem.put("l_extendedprice", new ColumnStatistic(901, 104949.5, 0, 8, 932377));
        // L_DISCOUNT      DOUBLE
        tableLineitem.put("l_discount", new ColumnStatistic(0, 0.1, 0, 8, 11));
        // L_TAX           DOUBLE
        tableLineitem.put("l_tax", new ColumnStatistic(0, 0.08, 0, 8, 9));
        // L_RETURNFLAG    CHAR(1)
        tableLineitem.put("l_returnflag", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 3));
        // L_LINESTATUS    CHAR(1)
        tableLineitem.put("l_linestatus", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 2));
        // L_SHIPDATE      DATE
        tableLineitem.put("l_shipdate", new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-02")),
                getLongFromDateTime(formatDateFromString("1998-12-01")), 0, 4, 2526));
        // L_COMMITDATE    DATE
        tableLineitem.put("l_commitdate", new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-31")),
                getLongFromDateTime(formatDateFromString("1998-10-31")), 0, 4, 2466));
        // L_RECEIPTDATE   DATE
        tableLineitem.put("l_receiptdate", new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-03")),
                getLongFromDateTime(formatDateFromString("1998-12-31")), 0, 4, 2554));
        // L_SHIPINSTRUCT  CHAR(25)
        tableLineitem.put("l_shipinstruct", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 4));
        // L_SHIPMODE      CHAR(10)
        tableLineitem.put("l_shipmode", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 7));
        // L_COMMENT       VARCHAR(44)
        tableLineitem.put("l_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 44, 4580667));
        tableStatistics.put("lineitem", tableLineitem);

        CaseInsensitiveMap<String, ColumnStatistic> tableLineitemPartition = new CaseInsensitiveMap<>();
        // L_ORDERKEY      BIGINT
        tableLineitemPartition.put("L_ORDERKEY", new ColumnStatistic(1, 6000000, 0, 8, 1500000));
        // L_PARTKEY       BIGINT
        tableLineitemPartition.put("L_PARTKEY", new ColumnStatistic(1, 200000, 0, 8, 200000));
        // L_SUPPKEY       INT
        tableLineitemPartition.put("L_SUPPKEY", new ColumnStatistic(1, 10000, 0, 4, 10000));
        // L_LINENUMBER    INT
        tableLineitemPartition.put("L_LINENUMBER", new ColumnStatistic(1, 7, 0, 4, 7));
        // L_QUANTITY      DOUBLE
        tableLineitemPartition.put("L_QUANTITY", new ColumnStatistic(1, 50, 0, 8, 50));
        // L_EXTENDEDPRICE DOUBLE
        tableLineitemPartition.put("L_EXTENDEDPRICE", new ColumnStatistic(901, 104949.5, 0, 8, 932377));
        // L_DISCOUNT      DOUBLE
        tableLineitemPartition.put("L_DISCOUNT", new ColumnStatistic(0, 0.1, 0, 8, 11));
        // L_TAX           DOUBLE
        tableLineitemPartition.put("L_TAX", new ColumnStatistic(0, 0.08, 0, 8, 9));
        // L_RETURNFLAG    CHAR(1)
        tableLineitemPartition.put("L_RETURNFLAG", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 3));
        // L_LINESTATUS    CHAR(1)
        tableLineitemPartition.put("L_LINESTATUS", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 2));
        // L_SHIPDATE      DATE
        tableLineitemPartition.put("L_SHIPDATE",
                new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-02")),
                        getLongFromDateTime(formatDateFromString("1998-12-01")), 0, 4, 2526));
        // L_COMMITDATE    DATE
        tableLineitemPartition.put("L_COMMITDATE",
                new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-31")),
                        getLongFromDateTime(formatDateFromString("1998-10-31")), 0, 4, 2466));
        // L_RECEIPTDATE   DATE
        tableLineitemPartition.put("L_RECEIPTDATE",
                new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-03")),
                        getLongFromDateTime(formatDateFromString("1998-12-31")), 0, 4, 2554));
        // L_SHIPINSTRUCT  CHAR(25)
        tableLineitemPartition.put("L_SHIPINSTRUCT",
                new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 4));
        // L_SHIPMODE      CHAR(10)
        tableLineitemPartition.put("L_SHIPMODE", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 7));
        // L_COMMENT       VARCHAR(44)
        tableLineitemPartition.put("L_COMMENT",
                new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 44, 4580667));
        tableStatistics.put("lineitem_partition", tableLineitemPartition);

        Map<String, ColumnStatistic> tableNation = new CaseInsensitiveMap<>();
        // N_NATIONKEY  INT
        tableNation.put("n_nationkey", new ColumnStatistic(0, 24, 0, 4, 25));
        // N_NAME      CHAR(25)
        tableNation.put("n_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 25));
        // N_REGIONKEY  INT
        tableNation.put("n_regionkey", new ColumnStatistic(0, 4, 0, 4, 5));
        // N_COMMENT   VARCHAR(152)
        tableNation.put("n_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 0, 25));
        tableStatistics.put("nation", tableNation);

        CaseInsensitiveMap<String, ColumnStatistic> tableOrders = new CaseInsensitiveMap<>();
        // O_ORDERKEY      BIGINT
        tableOrders.put("o_orderkey", new ColumnStatistic(1, 6000000, 0, 8, 1500000));
        // O_CUSTKEY       BIGINT
        tableOrders.put("o_custkey", new ColumnStatistic(1, 149999, 0, 8, 99996));
        // O_ORDERSTATUS   CHAR(1)
        tableOrders.put("o_orderstatus", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 3));
        // O_TOTALPRICE    DOUBLE
        tableOrders.put("o_totalprice", new ColumnStatistic(857.71, 555285.16, 0, 8, 1463406));
        // O_ORDERDATE     DATE
        tableOrders.put("o_orderdate", new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-01")),
                getLongFromDateTime(formatDateFromString("1998-08-02")), 0, 4, 2406));
        // O_ORDERPRIORITY CHAR(15)
        tableOrders.put("o_orderpriority", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 5));
        // O_CLERK         CHAR(15)
        tableOrders.put("o_clerk", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 1000));
        // O_SHIPPRIORITY  INT
        tableOrders.put("o_shippriority", new ColumnStatistic(0, 0, 0, 4, 1));
        // O_COMMENT       VARCHAR(79)
        tableOrders.put("o_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 79, 1482071));
        tableStatistics.put("orders", tableOrders);

        CaseInsensitiveMap<String, ColumnStatistic> tablePart = new CaseInsensitiveMap<>();
        // P_PARTKEY     BIGINT
        tablePart.put("p_partkey", new ColumnStatistic(1, 200000, 0, 8, 200000));
        // P_NAME        VARCHAR(55)
        tablePart.put("p_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 55, 199997));
        // P_MFGR        CHAR(25)
        tablePart.put("p_mfgr", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 5));
        // P_BRAND       CHAR(10)
        tablePart.put("p_brand", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 25));
        // P_TYPE        VARCHAR(25)
        tablePart.put("p_type", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 150));
        // P_SIZE        INT
        tablePart.put("p_size", new ColumnStatistic(1, 50, 0, 4, 50));
        // P_CONTAINER   CHAR(10)
        tablePart.put("p_container", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 10, 40));
        // P_RETAILPRICE DOUBLE
        tablePart.put("p_retailprice", new ColumnStatistic(901, 2098.99, 0, 8, 20899));
        // P_COMMENT     VARCHAR(23
        tablePart.put("p_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 0, 131753));
        tableStatistics.put("part", tablePart);

        CaseInsensitiveMap<String, ColumnStatistic> tablePartSupp = new CaseInsensitiveMap<>();
        // PS_PARTKEY    BIGINT
        tablePartSupp.put("ps_partkey", new ColumnStatistic(1, 200000, 0, 8, 200000));
        // PS_SUPPKEY    BIGINT
        tablePartSupp.put("ps_suppkey", new ColumnStatistic(1, 10000, 0, 8, 10000));
        // PS_AVAILQTY   INT
        tablePartSupp.put("ps_availqty", new ColumnStatistic(1, 9999, 0, 4, 9999));
        // PS_SUPPLYCOST DOUBLE
        tablePartSupp.put("ps_supplycost", new ColumnStatistic(1, 1000, 0, 8, 99864));
        // PS_COMMENT    VARCHAR(199)
        tablePartSupp.put("ps_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 199, 799124));
        tableStatistics.put("partsupp", tablePartSupp);

        CaseInsensitiveMap<String, ColumnStatistic> tableRegion = new CaseInsensitiveMap<>();
        // R_REGIONKEY INT
        tableRegion.put("r_regionkey", new ColumnStatistic(0, 4, 0, 4, 5));
        // R_NAME     CHAR(25)
        tableRegion.put("r_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 5));
        // R_COMMENT   VARCHAR(152)
        tableRegion.put("r_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 152, 5));
        tableStatistics.put("region", tableRegion);

        CaseInsensitiveMap<String, ColumnStatistic> tableSupplier = new CaseInsensitiveMap<>();
        // S_SUPPKEY   INT
        tableSupplier.put("s_suppkey", new ColumnStatistic(1, 10000, 0, 4, 10000));
        // S_NAME      CHAR(25)
        tableSupplier.put("s_name", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, 10000));
        // S_ADDRESS   VARCHAR(40)
        tableSupplier.put("s_address", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 40, 10000));
        // S_NATIONKEY INT
        tableSupplier.put("s_nationkey", new ColumnStatistic(0, 24, 0, 4, 25));
        // S_PHONE     CHAR(15)
        tableSupplier.put("s_phone", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 15, 10000));
        // S_ACCTBAL   DOUBLE
        tableSupplier.put("s_acctbal", new ColumnStatistic(-998.22, 9999.72, 0, 8, 9955));
        // S_COMMENT   VARCHAR(101)
        tableSupplier.put("s_comment", new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 101, 10000));
        tableStatistics.put("supplier", tableSupplier);
    }

    @Override
    public TableStatistic getTableStatistic(Long tableId, Long partitionId) {
        return rowCountStats.get(tableId);
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        if (tableStatistics.get(table.getName()) == null) {
            return ColumnStatistic.unknown();
        }
        ColumnStatistic statistic = tableStatistics.get(table.getName()).get(column);
        if (statistic != null) {
            if (table.getName().equalsIgnoreCase("region") ||
                    table.getName().equalsIgnoreCase("nation") ||
                    column.toLowerCase().contains("nationkey") ||
                    column.toLowerCase().contains("regionkey")) {
                return statistic;
            } else if (column.toLowerCase().contains("key") || column.toLowerCase().contains("name")) {
                return new ColumnStatistic(statistic.getMinValue(),
                        statistic.getMaxValue() * scale,
                        statistic.getNullsFraction(),
                        statistic.getAverageRowSize(),
                        statistic.getDistinctValuesCount() * scale);
            } else {
                return statistic;
            }
        }

        return ColumnStatistic.unknown();
    }

    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        return columns.stream().map(column -> getColumnStatistic(table, column)).collect(Collectors.toList());
    }

    private LocalDateTime formatDateFromString(String dateStr) {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(dateStr, fmt);
        return date.atStartOfDay();
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
    }
}
