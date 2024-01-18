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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class FunctionEvaluateBench {

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(FunctionEvaluateBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    private ConstantOperator date = ConstantOperator.createDate(LocalDateTime.of(2021, 12, 31,
            23, 59, 59));

    private ConstantOperator number = ConstantOperator.createInt(1540982618);



    private Function function = Expr.getBuiltinFunction(FunctionSet.SUBDATE,
            new com.starrocks.catalog.ScalarType[] {Type.DATETIME, Type.INT}, IS_IDENTICAL);

    @Benchmark
    public void test() {
        CallOperator call = new CallOperator("subdate", Type.DATETIME, Lists.newArrayList(date, number), function);
        for (int i = 0; i < 10000; i++) {
            ScalarOperatorEvaluator.INSTANCE.evaluation(call);
        }
    }
}
