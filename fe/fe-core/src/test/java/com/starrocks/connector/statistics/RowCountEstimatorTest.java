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

package com.starrocks.connector.statistics;

import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RowCountEstimatorTest {

    private static Column intCol(String name) {
        return new Column(name, IntegerType.INT);
    }

    private static Column bigintCol(String name) {
        return new Column(name, IntegerType.BIGINT);
    }

    private static Column varcharCol(String name, int len) {
        return new Column(name, TypeFactory.createVarcharType(len));
    }

    // --- boundary cases ---

    @Test
    public void testZeroBytes() {
        List<Column> cols = Arrays.asList(intCol("a"), intCol("b"));
        assertEquals(1L, RowCountEstimator.estimate(0, cols, HiveStorageFormat.PARQUET));
    }

    @Test
    public void testNegativeBytes() {
        List<Column> cols = Arrays.asList(intCol("a"));
        assertEquals(1L, RowCountEstimator.estimate(-1, cols, HiveStorageFormat.PARQUET));
    }

    @Test
    public void testEmptyColumnList() {
        // No schema → falls back to Config.connector_row_size_estimate_bytes
        long rowCount = RowCountEstimator.estimate(100_000_000L, Collections.emptyList(), HiveStorageFormat.PARQUET);
        long expected = 100_000_000L / Math.max(Config.connector_row_size_estimate_bytes, 8L);
        assertEquals(expected, rowCount);
    }

    // --- format factor ---

    @Test
    public void testParquetGivesHigherRowCountThanText() {
        // Same bytes, same schema: Parquet has smaller on-disk row size → more rows
        List<Column> cols = Arrays.asList(intCol("a"), intCol("b"), intCol("c"));
        long parquetRows = RowCountEstimator.estimate(100_000_000L, cols, HiveStorageFormat.PARQUET);
        long textRows   = RowCountEstimator.estimate(100_000_000L, cols, HiveStorageFormat.TEXTFILE);
        assertTrue(parquetRows > textRows,
                "Parquet rows=" + parquetRows + " should be > text rows=" + textRows);
    }

    @Test
    public void testOrcSameAsParquet() {
        List<Column> cols = Arrays.asList(intCol("a"), bigintCol("b"));
        long parquetRows = RowCountEstimator.estimate(50_000_000L, cols, HiveStorageFormat.PARQUET);
        long orcRows     = RowCountEstimator.estimate(50_000_000L, cols, HiveStorageFormat.ORC);
        assertEquals(parquetRows, orcRows);
    }

    @Test
    public void testUnknownFormatUsesConfigDefault() {
        List<Column> cols = Arrays.asList(intCol("a"));
        // SEQUENCE falls through to null factor → uses connector_row_size_estimate_bytes
        long rowCount = RowCountEstimator.estimate(100_000_000L, cols, HiveStorageFormat.SEQUENCE);
        long expected = 100_000_000L / Math.max(Config.connector_row_size_estimate_bytes, 8L);
        assertEquals(expected, rowCount);
    }

    @Test
    public void testNullFormatUsesConfigDefault() {
        List<Column> cols = Arrays.asList(intCol("a"));
        long rowCount = RowCountEstimator.estimate(100_000_000L, cols, null);
        long expected = 100_000_000L / Math.max(Config.connector_row_size_estimate_bytes, 8L);
        assertEquals(expected, rowCount);
    }

    // --- VARCHAR capping ---

    @Test
    public void testVarcharLengthIsCapped() {
        // A very wide VARCHAR should not make row size explode; capped at MAX_STRING_COL_SIZE (64)
        Column wideVarchar = varcharCol("s", 10000);
        Column narrowVarchar = varcharCol("s", 10);

        long rowsWide   = RowCountEstimator.estimate(100_000_000L, Collections.singletonList(wideVarchar),
                HiveStorageFormat.TEXTFILE);
        long rowsNarrow = RowCountEstimator.estimate(100_000_000L, Collections.singletonList(narrowVarchar),
                HiveStorageFormat.TEXTFILE);

        // VARCHAR(10000) should not give a wildly smaller row count than VARCHAR(10)
        // because the cap at 64 bytes kicks in for wide columns
        assertTrue(rowsWide >= rowsNarrow / 10,
                "Wide VARCHAR row count=" + rowsWide + " should not be orders of magnitude below narrow=" + rowsNarrow);
    }

    // --- sanity: Parquet 100MB, 5 INT cols ---

    @Test
    public void testParquetSanityCheck() {
        // 5 INT cols, 4 bytes each = 20 raw bytes; factor 0.25 → 5 bytes/row on disk
        // 100MB / 5 = 20M rows expected
        List<Column> cols = Arrays.asList(intCol("a"), intCol("b"), intCol("c"), intCol("d"), intCol("e"));
        long rowCount = RowCountEstimator.estimate(100_000_000L, cols, HiveStorageFormat.PARQUET);
        assertTrue(rowCount > 1_000_000L,
                "Expected >> 1M rows for 100MB Parquet with 5 INT cols, got: " + rowCount);
    }
}
