// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.loadv2.dpp;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3, batchSize = 10000)
public class ExpressionEncoderHelperBench {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ExpressionEncoderHelperBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Param({"true", "false"})
    public boolean useReflect;

    @Param({"50", "150"})
    public int fieldSize;

    private Row row;
    private ExpressionEncoder encoder;
    private ExpressionEncoderHelper encoderHelper;

    @Setup
    public void setup() {
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < fieldSize; i++) {
            StructField field = DataTypes.createStructField("c" + i, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        encoder = RowEncoder.apply(schema);
        if (useReflect) {
            encoderHelper = new ExpressionEncoderHelper(encoder);
        }
        List<Object> columnObjects = new ArrayList<>();
        for (int i = 0; i < fieldSize; i++) {
            columnObjects.add("12345678901234567890_" + i);
        }
        row = RowFactory.create(columnObjects.toArray());
    }

    @Benchmark
    public void toRowBench() {
        if (useReflect) {
            encoderHelper.toRow(row);
        } else {
            encoder.toRow(row);
        }
    }
}
