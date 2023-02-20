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


package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.DateUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class MockTPCHHistogramStatisticStorage implements StatisticStorage {
    private final Map<String, Map<String, ColumnStatistic>> tableStatistics;
    private final Map<String, Histogram> histogramStatistics;

    private final int tpchScala;

    public MockTPCHHistogramStatisticStorage(int tpchScala) {
        tableStatistics = new CaseInsensitiveMap<>();
        histogramStatistics = new CaseInsensitiveMap<>();
        this.tpchScala = tpchScala;
        mockTpchStatistics();
        mockTpchHistogramStatistics();
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

    private void mockTpchHistogramStatistics() {
        addHistogramStatistis("c_mktsegment", Type.STRING, tpchScala);
        addHistogramStatistis("c_acctbal", Type.STRING, tpchScala);
        addHistogramStatistis("l_discount", Type.DECIMAL32, tpchScala);
        addHistogramStatistis("l_quantity", Type.DECIMAL32, tpchScala);
        addHistogramStatistis("l_receiptdate", Type.DATE, tpchScala);
        addHistogramStatistis("l_shipdate", Type.DATE, tpchScala);
        addHistogramStatistis("l_returnflag", Type.STRING, tpchScala);
        addHistogramStatistis("l_shipinstruct", Type.STRING, tpchScala);
        addHistogramStatistis("o_orderdate", Type.DATE, tpchScala);
        addHistogramStatistis("o_orderstatus", Type.STRING, tpchScala);
        addHistogramStatistis("p_type", Type.STRING, tpchScala);
        addHistogramStatistis("p_size", Type.INT, tpchScala);
        addHistogramStatistis("p_brand", Type.STRING, tpchScala);
        addHistogramStatistis("p_container", Type.STRING, tpchScala);
        addHistogramStatistis("r_name", Type.STRING, 1);
        addHistogramStatistis("n_nationkey", Type.INT, 1);
        addHistogramStatistis("n_name", Type.STRING, 1);
    }

    private void addHistogramStatistis(String fileName, Type type, int scala) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/tpch-histogram-cost/histogram-stats/" + fileName + ".json");

        String tempStr;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            StringBuilder histogramString = new StringBuilder();
            while ((tempStr = reader.readLine()) != null) {
                histogramString.append(tempStr);
            }
            List<Bucket> buckets = convertBuckets(histogramString.toString(), type, scala);
            Map<String, Long> mcv = convertMCV(histogramString.toString(), scala);
            histogramStatistics.put(fileName, new Histogram(buckets, mcv));
            GlobalStateMgr.getCurrentAnalyzeMgr().addHistogramStatsMeta(new HistogramStatsMeta(
                    0, 0, fileName, StatsConstants.AnalyzeType.HISTOGRAM, LocalDateTime.MIN, Maps.newHashMap()));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
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
                        statistic.getMaxValue() * tpchScala,
                        statistic.getNullsFraction(),
                        statistic.getAverageRowSize(),
                        statistic.getDistinctValuesCount() * tpchScala);
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

    @Override
    public Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
        Map<String, Histogram> histogramMap = new HashMap<>();
        for (String columnName : columns) {
            if (histogramStatistics.containsKey(columnName)) {
                Histogram histogram = histogramStatistics.get(columnName);
                histogramMap.put(columnName, histogram);
            }
        }
        return histogramMap;
    }

    private LocalDateTime formatDateFromString(String dateStr) {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(dateStr, fmt);
        return date.atStartOfDay();
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
    }

    private List<Bucket> convertBuckets(String histogramString, Type type, int scala) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();

        JsonElement jsonElement = jsonObject.get("buckets");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyList();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        List<Bucket> buckets = Lists.newArrayList();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();

            double low;
            double high;
            if (type.isDate()) {
                low = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(0).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
                high = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(1).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
            } else if (type.isDatetime()) {
                low = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(0).getAsString(), DateUtils.DATE_TIME_FORMATTER_UNIX));
                high = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(1).getAsString(), DateUtils.DATE_TIME_FORMATTER_UNIX));
            } else {
                low = Double.parseDouble(bucketJsonArray.get(0).getAsString());
                high = Double.parseDouble(bucketJsonArray.get(1).getAsString());
            }

            Bucket bucket = new Bucket(low, high,
                    Long.parseLong(bucketJsonArray.get(2).getAsString()) * scala,
                    Long.parseLong(bucketJsonArray.get(3).getAsString()) * scala);
            buckets.add(bucket);
        }
        return buckets;
    }

    private Map<String, Long> convertMCV(String histogramString, int scala) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();
        JsonElement jsonElement = jsonObject.get("mcv");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyMap();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        Map<String, Long> mcv = new HashMap<>();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();
            mcv.put(bucketJsonArray.get(0).getAsString(), Long.parseLong(bucketJsonArray.get(1).getAsString()) * scala);
        }
        return mcv;
    }
}
