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

package com.starrocks.connector.benchmark.tpcds;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.benchmark.BenchmarkSuite;
import com.starrocks.connector.benchmark.RowCountEstimate;

import java.util.Map;

public final class TpcdsBenchmarkSuite implements BenchmarkSuite {
    public static final String NAME = "tpcds";
    private static final long DB_ID = 1L;
    private static final String SCHEMA_FILE = "tpcds_schema.json";

    private static final Map<String, Long> SCALE1_ROW_COUNTS = ImmutableMap.<String, Long>builder()
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

    private static final Map<String, Long> SCALE1000_ROW_COUNTS = ImmutableMap.<String, Long>builder()
            .put("call_center", 42L)
            .put("catalog_page", 30000L)
            .put("catalog_returns", 143996756L)
            .put("catalog_sales", 1439980416L)
            .put("customer", 12000000L)
            .put("customer_address", 6000000L)
            .put("customer_demographics", 1920800L)
            .put("date_dim", 73049L)
            .put("household_demographics", 7200L)
            .put("income_band", 20L)
            .put("inventory", 783000000L)
            .put("item", 300000L)
            .put("promotion", 1500L)
            .put("reason", 65L)
            .put("ship_mode", 20L)
            .put("store", 1002L)
            .put("store_returns", 287999764L)
            .put("store_sales", 2879987999L)
            .put("time_dim", 86400L)
            .put("warehouse", 20L)
            .put("web_page", 3000L)
            .put("web_returns", 71997522L)
            .put("web_sales", 720000376L)
            .put("web_site", 54L)
            .build();

    private static final double LOG_1000 = Math.log(1000.0);
    private static final Map<String, Double> SCALE_EXPONENTS = buildScaleExponents();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public long getDbId() {
        return DB_ID;
    }

    @Override
    public String getSchemaFileName() {
        return SCHEMA_FILE;
    }

    @Override
    public RowCountEstimate estimateRowCount(String tableName, double scaleFactor) {
        String normalized = normalizeTableName(tableName);
        long base = getValue(SCALE1_ROW_COUNTS, normalized);
        long scale1000 = getValue(SCALE1000_ROW_COUNTS, normalized);
        if (Double.compare(scaleFactor, 1.0) == 0) {
            return RowCountEstimate.known(base);
        }
        if (Double.compare(scaleFactor, 1000.0) == 0) {
            return RowCountEstimate.known(scale1000);
        }
        Double exponent = SCALE_EXPONENTS.get(normalized);
        Preconditions.checkArgument(exponent != null, "Unknown TPCDS table: %s", tableName);
        double rows = base * Math.pow(scaleFactor, exponent);
        if (rows <= 0) {
            return RowCountEstimate.known(0L);
        }
        return RowCountEstimate.known(Math.max(1L, Math.round(rows)));
    }

    private static Map<String, Double> buildScaleExponents() {
        ImmutableMap.Builder<String, Double> builder = ImmutableMap.builder();
        for (Map.Entry<String, Long> entry : SCALE1_ROW_COUNTS.entrySet()) {
            String table = entry.getKey();
            long base = entry.getValue();
            long scale1000 = getValue(SCALE1000_ROW_COUNTS, table);
            double exponent = Math.log((double) scale1000 / (double) base) / LOG_1000;
            builder.put(table, exponent);
        }
        return builder.build();
    }

    private static long getValue(Map<String, Long> map, String tableName) {
        Long value = map.get(tableName);
        Preconditions.checkArgument(value != null, "Unknown TPCDS table: %s", tableName);
        return value;
    }
}
