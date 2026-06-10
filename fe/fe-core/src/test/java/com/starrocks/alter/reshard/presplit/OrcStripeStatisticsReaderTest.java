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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

class OrcStripeStatisticsReaderTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void readsBigintStatisticsAcrossWholeFile() throws Exception {
        // Tiny stripe size (see writeOrcFixture) coaxes the writer into multiple
        // stripes; the exact count is implementation-defined so the assertions
        // exercise aggregate invariants only.
        Path orcPath = writeOrc(
                "struct<sort_key:bigint>",
                /*rowCount=*/ 64,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertFalse(stripeStatistics.isEmpty());
        long totalRowCount = 0L;
        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        for (RowGroupStatistics stripe : stripeStatistics) {
            Assertions.assertNotNull(stripe.getMinTuple());
            Assertions.assertNotNull(stripe.getMaxTuple());
            Assertions.assertFalse(stripe.isTruncated());
            long minValue = Long.parseLong(stripe.getMinTuple().getValues().get(0).getStringValue());
            long maxValue = Long.parseLong(stripe.getMaxTuple().getValues().get(0).getStringValue());
            Assertions.assertTrue(minValue <= maxValue, "stripe min " + minValue + " > max " + maxValue);
            totalRowCount += stripe.getRowCount();
            globalMin = Math.min(globalMin, minValue);
            globalMax = Math.max(globalMax, maxValue);
        }
        Assertions.assertEquals(64L, totalRowCount);
        Assertions.assertEquals(0L, globalMin);
        Assertions.assertEquals(63L, globalMax);
    }

    @Test
    void readsIntStatistics() throws Exception {
        Path orcPath = writeOrc(
                "struct<region_id:int>",
                /*rowCount=*/ 5,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex + 100);

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                statusOf(orcPath), new Configuration(), new Column("region_id", IntegerType.INT));

        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        for (RowGroupStatistics stripe : stripeStatistics) {
            globalMin = Math.min(globalMin, Long.parseLong(stripe.getMinTuple().getValues().get(0).getStringValue()));
            globalMax = Math.max(globalMax, Long.parseLong(stripe.getMaxTuple().getValues().get(0).getStringValue()));
        }
        Assertions.assertEquals(100L, globalMin);
        Assertions.assertEquals(104L, globalMax);
    }

    @Test
    void caseInsensitiveColumnMatchResolves() throws Exception {
        // ORC field "Sort_Key" must match StarRocks column "sort_key" (names are
        // case-insensitive in StarRocks).
        Path orcPath = writeOrc(
                "struct<Sort_Key:bigint>",
                /*rowCount=*/ 4,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertFalse(stripeStatistics.isEmpty());
        Assertions.assertNotNull(stripeStatistics.get(0).getMinTuple());
    }

    @Test
    void stringColumnFallsBackToDataTier() throws Exception {
        // ORC string stats would always need data-tier fallback; v1 rejects the
        // type eagerly rather than wiring a string-stats path.
        Path orcPath = writeOrc(
                "struct<tenant:string>",
                /*rowCount=*/ 4,
                (batch, batchRow, rowIndex) -> ((BytesColumnVector) batch.cols[0])
                        .setVal(batchRow, ("tenant-" + rowIndex).getBytes(StandardCharsets.UTF_8)));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        statusOf(orcPath), new Configuration(), new Column("tenant", VarcharType.VARCHAR)));
    }

    @Test
    void unsupportedOrcTypeFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<payload:double>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) ->
                        ((DoubleColumnVector) batch.cols[0]).vector[batchRow] = rowIndex * 1.5);

        // DOUBLE is outside the meta-tier mapping window even for a numeric sort key.
        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        statusOf(orcPath), new Configuration(), new Column("payload", IntegerType.BIGINT)));
    }

    @Test
    void mismatchedStarRocksTypeFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<region_id:bigint>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        // ORC integer stats cannot route into a VARCHAR sort-key column.
        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        statusOf(orcPath), new Configuration(), new Column("region_id", VarcharType.VARCHAR)));
    }

    @Test
    void columnAbsentFromSchemaFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<other:bigint>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        statusOf(orcPath), new Configuration(), new Column("missing_sort_key", IntegerType.BIGINT)));
    }

    @Test
    void caseVariantDuplicatesFallBackToDataTier() throws Exception {
        // StarRocks column names are case-insensitive, so two ORC fields differing
        // only by case are ambiguous and the reader cannot pick one silently.
        Path orcPath = writeOrc(
                "struct<sort_key:bigint,SORT_KEY:bigint>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> {
                    ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex;
                    ((LongColumnVector) batch.cols[1]).vector[batchRow] = -rowIndex;
                });

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT)));
    }

    @Test
    void outOfRangeStatsValueFallsBackToDataTier() throws Exception {
        // ORC bigint stats (260) outside StarRocks TINYINT range; the value-conversion
        // failure must surface as a meta-tier fallback, not a hard error.
        Path orcPath = writeOrc(
                "struct<wide_value:bigint>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = 260L + rowIndex);

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        statusOf(orcPath), new Configuration(), new Column("wide_value", IntegerType.TINYINT)));
    }

    @Test
    void allNullStripeReportsAbsentStatistics() throws Exception {
        // sort_key is left null on every row (keepalive keeps the file non-empty).
        // ORC integer stats expose no presence flag, so the reader must consult
        // getNumberOfValues() and emit absent min/max rather than a bogus 0.
        Path orcPath = writeOrc(
                "struct<sort_key:bigint,keepalive:bigint>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) -> {
                    LongColumnVector sortKey = (LongColumnVector) batch.cols[0];
                    sortKey.noNulls = false;
                    sortKey.isNull[batchRow] = true;
                    ((LongColumnVector) batch.cols[1]).vector[batchRow] = rowIndex;
                });

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        long totalRowCount = 0L;
        for (RowGroupStatistics stripe : stripeStatistics) {
            Assertions.assertNull(stripe.getMinTuple());
            Assertions.assertNull(stripe.getMaxTuple());
            totalRowCount += stripe.getRowCount();
        }
        Assertions.assertEquals(3L, totalRowCount);
    }

    @Test
    void emptyFileReturnsNoStripes() throws Exception {
        Path orcPath = writeOrc(
                "struct<sort_key:bigint>",
                /*rowCount=*/ 0,
                (batch, batchRow, rowIndex) -> { });

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertTrue(stripeStatistics.isEmpty());
    }

    private Path writeOrc(
            String schemaText, int rowCount, PresplitTestSupport.OrcRowFiller rowFiller) throws IOException {
        return PresplitTestSupport.writeOrcFixture(tempDirectory, schemaText, rowCount, rowFiller);
    }

    private static FileStatus statusOf(Path path) throws IOException {
        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(path.toUri(), new Configuration());
        return fs.getFileStatus(path);
    }
}
