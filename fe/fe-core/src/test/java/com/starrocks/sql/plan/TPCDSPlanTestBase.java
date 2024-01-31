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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.OlapTable;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TPCDSPlanTestBase extends PlanTestBase {
    private static final Map<String, Long> ROW_COUNT_MAP = ImmutableMap.<String, Long>builder()
            .put("call_center", 6L)
            .put("catalog_page", 11718L)
            .put("catalog_returns", 144067L)
            .put("catalog_sales", 1441548L)
            .put("customer", 100000L)
            .put("customer_address", 50000L)
            .put("customer_demographics", 1920800L)
            .put("date_dim", 73049L)
            .put("household_demographics", 7200L)
            .put("income_band", 20L)
            .put("inventory", 11745000L)
            .put("item", 18000L)
            .put("promotion", 300L)
            .put("reason", 35L)
            .put("ship_mode", 20L)
            .put("store", 12L)
            .put("store_returns", 287514L)
            .put("store_sales", 2880404L)
            .put("time_dim", 86400L)
            .put("warehouse", 5L)
            .put("web_page", 60L)
            .put("web_returns", 71763L)
            .put("web_sales", 719384L)
            .put("web_site", 30L)
            .build();

    // query name -> query sql
    private static final Map<String, String> SQL_MAP = new LinkedHashMap<>();

    public void setTPCDSFactor(int factor) {
        ROW_COUNT_MAP.forEach((t, v) -> {
            OlapTable table = getOlapTable(t);
            setTableStatistics(table, v * factor);
        });
    }

    public Map<String, Long> getTPCDSTableStats() {
        Map<String, Long> m = new HashMap<>();
        ROW_COUNT_MAP.forEach((t, v) -> {
            OlapTable table = getOlapTable(t);
            m.put(t, table.getRowCount());
        });
        return m;
    }

    public void setTPCDSTableStats(Map<String, Long> m) {
        ROW_COUNT_MAP.forEach((t, s) -> {
            if (m.containsKey(t)) {
                long v = m.get(t);
                OlapTable table = getOlapTable(t);
                setTableStatistics(table, v);
            }
        });
    }

    public static Map<String, String> getSqlMap() {
        return SQL_MAP;
    }

    public static String getSql(String queryName) {
        return SQL_MAP.get(queryName);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.dropTable("customer");
        TPCDSTestUtil.prepareTables(starRocksAssert);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(3);
        connectContext.getSessionVariable().setSqlMode(2);
    }

    private static String from(String name) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        String queryName = "query" + name;
        File file = new File(path + "/tpcds/" + queryName + ".sql");
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String str;
            while ((str = reader.readLine()) != null) {
                sb.append(str).append("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String sql = sb.toString();
        SQL_MAP.put(queryName, sql);
        return sql;
    }

    public static final String Q01 = from("01");
    public static final String Q02 = from("02");
    public static final String Q03 = from("03");
    public static final String Q04 = from("04");
    public static final String Q05 = from("05");
    public static final String Q06 = from("06");
    public static final String Q07 = from("07");
    public static final String Q08 = from("08");
    public static final String Q09 = from("09");
    public static final String Q10 = from("10");
    public static final String Q11 = from("11");
    public static final String Q12 = from("12");
    public static final String Q13 = from("13");
    public static final String Q14_1 = from("14-1");
    public static final String Q14_2 = from("14-2");
    public static final String Q15 = from("15");
    public static final String Q16 = from("16");
    public static final String Q17 = from("17");
    public static final String Q18 = from("18");
    public static final String Q19 = from("19");
    public static final String Q20 = from("20");
    public static final String Q21 = from("21");
    public static final String Q22 = from("22");
    public static final String Q23_1 = from("23-1");
    public static final String Q23_2 = from("23-2");
    public static final String Q24_1 = from("24-1");
    public static final String Q24_2 = from("24-2");
    public static final String Q25 = from("25");
    public static final String Q26 = from("26");
    public static final String Q27 = from("27");
    public static final String Q28 = from("28");
    public static final String Q29 = from("29");
    public static final String Q30 = from("30");
    public static final String Q31 = from("31");
    public static final String Q32 = from("32");
    public static final String Q33 = from("33");
    public static final String Q34 = from("34");
    public static final String Q35 = from("35");
    public static final String Q36 = from("36");
    public static final String Q37 = from("37");
    public static final String Q38 = from("38");
    public static final String Q39_1 = from("39-1");
    public static final String Q39_2 = from("39-2");
    public static final String Q39_1_2 = from("39-1-2");
    public static final String Q39_2_2 = from("39-2-2");
    public static final String Q40 = from("40");
    public static final String Q41 = from("41");
    public static final String Q42 = from("42");
    public static final String Q43 = from("43");
    public static final String Q44 = from("44");
    public static final String Q45 = from("45");
    public static final String Q46 = from("46");
    public static final String Q47 = from("47");
    public static final String Q48 = from("48");
    public static final String Q49 = from("49");
    public static final String Q50 = from("50");
    public static final String Q51 = from("51");
    public static final String Q52 = from("52");
    public static final String Q53 = from("53");
    public static final String Q54 = from("54");
    public static final String Q55 = from("55");
    public static final String Q56 = from("56");
    public static final String Q57 = from("57");
    public static final String Q58 = from("58");
    public static final String Q59 = from("59");
    public static final String Q60 = from("60");
    public static final String Q61 = from("61");
    public static final String Q62 = from("62");
    public static final String Q63 = from("63");
    public static final String Q64 = from("64");
    public static final String Q64_2 = from("64-2");
    public static final String Q65 = from("65");
    public static final String Q66 = from("66");
    public static final String Q67 = from("67");
    public static final String Q68 = from("68");
    public static final String Q69 = from("69");
    public static final String Q70 = from("70");
    public static final String Q71 = from("71");
    public static final String Q72 = from("72");
    public static final String Q73 = from("73");
    public static final String Q74 = from("74");
    public static final String Q75 = from("75");
    public static final String Q76 = from("76");
    public static final String Q77 = from("77");
    public static final String Q78 = from("78");
    public static final String Q79 = from("79");
    public static final String Q80 = from("80");
    public static final String Q81 = from("81");
    public static final String Q82 = from("82");
    public static final String Q83 = from("83");
    public static final String Q84 = from("84");
    public static final String Q85 = from("85");
    public static final String Q86 = from("86");
    public static final String Q87 = from("87");
    public static final String Q88 = from("88");
    public static final String Q89 = from("89");
    public static final String Q90 = from("90");
    public static final String Q91 = from("91");
    public static final String Q92 = from("92");
    public static final String Q93 = from("93");
    public static final String Q94 = from("94");
    public static final String Q95 = from("95");
    public static final String Q96 = from("96");
    public static final String Q97 = from("97");
    public static final String Q98 = from("98");
    public static final String Q99 = from("99");

    public String getTPCDS(String name) {
        Class<TPCDSPlanTestBase> clazz = TPCDSPlanTestBase.class;
        try {
            Field f = clazz.getField(name);
            return (String) f.get(this);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
