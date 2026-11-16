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

package com.starrocks.benchmark;

import com.starrocks.common.Pair;
import com.starrocks.connector.delta.DeltaLakeAddFileStatsSerDe;
import com.starrocks.connector.delta.FileScanTask;
import com.starrocks.connector.delta.ScanFileUtils;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

// Compare per-file initialization with scan-scoped reuse. Run with JMH's -prof gc for bytes/op.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
public class DeltaScanFileConversionBench {
    @Param({"10", "100", "1000"})
    public int columns;

    private Metadata metadata;
    private Row file;
    private ScanFileUtils.FileScanTaskConverter converter;

    @Setup
    public void setup() {
        StructType schema = new StructType();
        for (int i = 0; i < columns; i++) {
            schema = schema.add(new StructField("c" + i, StringType.STRING, true,
                    FieldMetadata.builder().putString("delta.columnMapping.physicalName", "physical-" + i).build()));
        }
        metadata = new Metadata("benchmark", Optional.empty(), Optional.empty(), new Format(), schema.toJson(), schema,
                VectorUtils.buildArrayValue(List.of("c0"), StringType.STRING), Optional.empty(),
                VectorUtils.stringStringMapValue(Map.of()));
        StructType addSchema = (StructType) InternalScanFileUtils.SCAN_FILE_SCHEMA_WITH_STATS.get("add").getDataType();
        Map<Integer, Object> add = new HashMap<>();
        add.put(addSchema.indexOf("path"), "physical-0=west/part-000.parquet");
        add.put(addSchema.indexOf("size"), 1048576L);
        add.put(addSchema.indexOf("modificationTime"), 1L);
        add.put(addSchema.indexOf("partitionValues"), VectorUtils.stringStringMapValue(Map.of("physical-0", "west")));
        add.put(addSchema.indexOf("stats"), "{\"numRecords\":1000}");
        file = new GenericRow(InternalScanFileUtils.SCAN_FILE_SCHEMA, Map.of(
                InternalScanFileUtils.ADD_FILE_ORDINAL, new GenericRow(addSchema, add),
                InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("tableRoot"), "s3://bucket/table"));
        converter = new ScanFileUtils.FileScanTaskConverter(metadata, 100);
    }

    @Benchmark
    public Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> perFileInitialization() {
        return ScanFileUtils.convertFromRowToFileScanTask(false, file, metadata, 100, null);
    }

    @Benchmark
    public Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> reuseScanConverter() {
        return converter.convert(false, file, null);
    }
}
