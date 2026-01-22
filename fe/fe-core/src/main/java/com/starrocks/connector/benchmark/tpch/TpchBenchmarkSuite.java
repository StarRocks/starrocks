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

package com.starrocks.connector.benchmark.tpch;

import com.starrocks.connector.benchmark.BenchmarkSuite;
import com.starrocks.connector.benchmark.RowCountEstimate;

public final class TpchBenchmarkSuite implements BenchmarkSuite {
    public static final String NAME = "tpch";
    private static final long DB_ID = 2L;
    private static final String SCHEMA_FILE = "tpch_schema.json";

    private static final long PART_BASE = 200000L;
    private static final long SUPPLIER_BASE = 10000L;
    private static final long CUSTOMER_BASE = 150000L;
    private static final long ORDERS_BASE = 150000L;
    private static final long ORDERS_PER_CUSTOMER = 10L;
    private static final long SUPP_PER_PART = 4L;
    // dbgen lineitem row counts at scale 1/5/10 (used for interpolation).
    private static final long LINEITEM_SCALE1 = 6001215L;
    private static final long LINEITEM_SCALE5 = 29999795L;
    private static final long LINEITEM_SCALE10 = 59986052L;

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
        switch (normalized) {
            case "part":
                return RowCountEstimate.known(scaleLinear(PART_BASE, scaleFactor));
            case "partsupp":
                return RowCountEstimate.known(scaleLinear(PART_BASE, scaleFactor) * SUPP_PER_PART);
            case "supplier":
                return RowCountEstimate.known(scaleLinear(SUPPLIER_BASE, scaleFactor));
            case "customer":
                return RowCountEstimate.known(scaleLinear(CUSTOMER_BASE, scaleFactor));
            case "orders":
                return RowCountEstimate.known(scaleLinear(ORDERS_BASE * ORDERS_PER_CUSTOMER, scaleFactor));
            case "lineitem":
                return RowCountEstimate.known(lineitemCount(scaleFactor));
            case "nation":
                return RowCountEstimate.known(25L);
            case "region":
                return RowCountEstimate.known(5L);
            default:
                throw new IllegalArgumentException("Unknown TPCH table: " + tableName);
        }
    }

    private static long scaleLinear(long base, double scaleFactor) {
        if (scaleFactor < 1.0) {
            long intScale = (long) (scaleFactor * 1000.0);
            long scaled = (intScale * base) / 1000;
            return scaled < 1 ? 1 : scaled;
        }
        return base * (long) scaleFactor;
    }

    private static long lineitemCount(double scaleFactor) {
        if (scaleFactor < 1.0) {
            return scaleLinear(LINEITEM_SCALE1, scaleFactor);
        }
        long scale = (long) scaleFactor;
        if (scale <= 0) {
            return 0;
        }
        long tens = scale / 10;
        long remainder = scale % 10;
        long count = tens * LINEITEM_SCALE10;
        if (remainder == 0) {
            return count;
        }
        if (remainder < 5) {
            long delta = LINEITEM_SCALE5 - LINEITEM_SCALE1;
            count += LINEITEM_SCALE1 + (delta * (remainder - 1)) / 4;
            return count;
        }
        if (remainder == 5) {
            return count + LINEITEM_SCALE5;
        }
        long delta = LINEITEM_SCALE10 - LINEITEM_SCALE5;
        count += LINEITEM_SCALE5 + (delta * (remainder - 5)) / 5;
        return count;
    }
}
