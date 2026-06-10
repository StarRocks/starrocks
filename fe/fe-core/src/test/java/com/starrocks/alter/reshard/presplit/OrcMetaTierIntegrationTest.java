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
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * End-to-end coverage for the ORC meta tier: a real ORC file flows through the
 * production {@link InsertFromFilesRowGroupStatisticsProvider} into the
 * format-agnostic {@link ParquetMetadataSampler}. The reader and sampler are unit
 * tested in isolation elsewhere; this verifies the composed chain a load actually
 * runs — boundaries on clean ORC data, and a clean data-tier fallback on an
 * out-of-window ORC type.
 */
class OrcMetaTierIntegrationTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void disjointOrcFilesProduceBoundaries() throws Exception {
        // Four files with disjoint ascending ranges aggregate to four non-overlapping
        // stripes regardless of intra-file stripe formation, so the sampler accepts
        // them and places quantile cuts.
        List<TBrokerFileStatus> files = List.of(
                brokerFileStatus(writeBigintOrc(/*valueOffset=*/ 0L)),
                brokerFileStatus(writeBigintOrc(/*valueOffset=*/ 1000L)),
                brokerFileStatus(writeBigintOrc(/*valueOffset=*/ 2000L)),
                brokerFileStatus(writeBigintOrc(/*valueOffset=*/ 3000L)));

        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new InsertFromFilesRowGroupStatisticsProvider());
        BoundaryPlannerResult result = sampler.tryPlan(orcSampleRequest("orc", files), /*requestedTabletCount=*/ 4);

        Assertions.assertFalse(result.isNoSplit());
        List<Tuple> boundaries = result.getBoundaries();
        Assertions.assertFalse(boundaries.isEmpty());
        long previous = Long.MIN_VALUE;
        for (Tuple boundary : boundaries) {
            long value = Long.parseLong(boundary.getValues().get(0).getStringValue());
            Assertions.assertTrue(value > previous, "boundaries must be strictly ascending");
            previous = value;
        }
    }

    @Test
    void unsupportedOrcTypeFallsBackThroughSampler() throws Exception {
        Path doublePath = writeDoubleOrc();
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new InsertFromFilesRowGroupStatisticsProvider());

        // DOUBLE is outside the meta-tier window; the reader's fallback signal must
        // propagate through the sampler so the pipeline retries with data tier.
        Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(orcSampleRequest("orc", List.of(brokerFileStatus(doublePath))), 4));
    }

    private Path writeBigintOrc(long valueOffset) throws IOException {
        return PresplitTestSupport.writeOrcFixture(
                tempDirectory,
                "struct<sort_key:bigint>",
                /*rowCount=*/ 100,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = valueOffset + rowIndex);
    }

    private Path writeDoubleOrc() throws IOException {
        return PresplitTestSupport.writeOrcFixture(
                tempDirectory,
                "struct<sort_key:double>",
                /*rowCount=*/ 4,
                (batch, batchRow, rowIndex) ->
                        ((DoubleColumnVector) batch.cols[0]).vector[batchRow] = rowIndex * 1.5);
    }

    private static SampleRequest orcSampleRequest(String format, List<TBrokerFileStatus> files) {
        TableFunctionTable sourceTable = Mockito.mock(TableFunctionTable.class);
        Mockito.when(sourceTable.getFormat()).thenReturn(format);
        Mockito.when(sourceTable.getProperties()).thenReturn(new HashMap<>());
        Mockito.when(sourceTable.loadFileList()).thenReturn(new ArrayList<>(files));
        return new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);
    }

    private static TBrokerFileStatus brokerFileStatus(Path path) throws IOException {
        long size = Files.size(java.nio.file.Path.of(path.toUri()));
        return new TBrokerFileStatus(path.toString(), /*isDir=*/ false, size, /*isSplitable=*/ true);
    }
}
