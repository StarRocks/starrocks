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

package com.starrocks.connector.benchmark.ssb;

import com.starrocks.connector.benchmark.BenchmarkSuite;
import com.starrocks.connector.benchmark.RowCountEstimate;

public final class SsbBenchmarkSuite implements BenchmarkSuite {
    public static final String NAME = "ssb";
    private static final long DB_ID = 3L;
    private static final String SCHEMA_FILE = "ssb_schema.json";

    private static final long CUSTOMER_BASE = 30000L;
    private static final long SUPPLIER_BASE = 2000L;
    private static final long PART_BASE = 200000L;
    private static final long DATE_BASE = 2556L;
    // dbgen lineorder row counts at scale 1/5/10 (used for interpolation).
    private static final long LINEORDER_SCALE1 = 6001215L;
    private static final long LINEORDER_SCALE5 = 29999795L;
    private static final long LINEORDER_SCALE10 = 59986052L;

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
            case "customer":
                return RowCountEstimate.known(scaleLinear(CUSTOMER_BASE, scaleFactor));
            case "supplier":
                return RowCountEstimate.known(scaleLinear(SUPPLIER_BASE, scaleFactor));
            case "part":
                return RowCountEstimate.known(scalePart(PART_BASE, scaleFactor));
            case "date":
                return RowCountEstimate.known(scaleDate(DATE_BASE, scaleFactor));
            case "lineorder":
                return RowCountEstimate.known(lineorderCount(scaleFactor));
            default:
                throw new IllegalArgumentException("Unknown SSB table: " + tableName);
        }
    }

    private static long scaleLinear(long base, double scaleFactor) {
        if (scaleFactor < 1.0) {
            double scaled = (double) base * scaleFactor;
            return scaled < 1.0 ? 1 : (long) scaled;
        }
        return base * (long) scaleFactor;
    }

    private static long scalePart(long base, double scaleFactor) {
        long scale = scaleFactor >= 1.0 ? (long) scaleFactor : 1L;
        long multiplier = partScaleMultiplier(scale);
        long scaledBase = base * multiplier;
        double baseScale = scaleFactor < 1.0 ? scaleFactor : 1.0;
        double scaled = scaledBase * baseScale;
        return scaled < 1.0 ? 1 : (long) scaled;
    }

    private static long scaleDate(long base, double scaleFactor) {
        double baseScale = scaleFactor < 1.0 ? scaleFactor : 1.0;
        double scaled = base * baseScale;
        return scaled < 1.0 ? 1 : (long) scaled;
    }

    private static long lineorderCount(double scaleFactor) {
        if (scaleFactor < 1.0) {
            return scaleLinear(LINEORDER_SCALE1, scaleFactor);
        }
        long scale = (long) scaleFactor;
        if (scale <= 0) {
            return 0;
        }
        long tens = scale / 10;
        long remainder = scale % 10;
        long count = tens * LINEORDER_SCALE10;
        if (remainder == 0) {
            return count;
        }
        if (remainder < 5) {
            long delta = LINEORDER_SCALE5 - LINEORDER_SCALE1;
            count += LINEORDER_SCALE1 + (delta * (remainder - 1)) / 4;
            return count;
        }
        if (remainder == 5) {
            return count + LINEORDER_SCALE5;
        }
        long delta = LINEORDER_SCALE10 - LINEORDER_SCALE5;
        count += LINEORDER_SCALE5 + (delta * (remainder - 5)) / 5;
        return count;
    }

    private static long partScaleMultiplier(long scale) {
        if (scale <= 1) {
            return 1;
        }
        double factor = 1.0 + (Math.log((double) scale) / Math.log(2.0));
        return (long) Math.floor(factor);
    }
}
