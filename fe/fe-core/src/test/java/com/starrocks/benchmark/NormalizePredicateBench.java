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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
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

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.AND;
import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.OR;

/**
 * Benchmark the MvUtils.canonizePredicateForRewrite
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(time = 1, timeUnit = TimeUnit.SECONDS)
public class NormalizePredicateBench {

    @Param({"10", "20", "40", "80", "160"})
    private int predicateSize;

    private ScalarOperator randomPredicate;
    private ScalarOperator disjunctive;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(NormalizePredicateBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        randomPredicate = generatePredicate();
        disjunctive = generateDisjunctive();
    }

    private BinaryType randomBinary() {
        int n = ThreadLocalRandom.current().nextInt(6);
        return BinaryType.values()[n];
    }

    private ColumnRefOperator randomColumn(ColumnRefFactory factory) {
        final List<String> columns = IntStream.range(1, 10)
                .mapToObj(x -> RandomStringUtils.randomAlphabetic(2))
                .collect(Collectors.toList());
        final List<Type> types = ImmutableList.of(Type.INT, Type.VARCHAR, Type.DATETIME);
        int x = ThreadLocalRandom.current().nextInt(columns.size());
        String name = columns.get(x);
        x = ThreadLocalRandom.current().nextInt(types.size());
        Type t = types.get(x);
        return factory.create(name, t, true);
    }

    private ConstantOperator randomConstant(Type type) {
        int x = ThreadLocalRandom.current().nextInt(1024);
        if (type.equals(Type.INT)) {
            return ConstantOperator.createInt(x);
        } else if (type.equals(Type.DATETIME)) {
            return ConstantOperator.createDatetime(LocalDateTime.now().plusDays(x));
        } else if (type.equals(Type.VARCHAR)) {
            return ConstantOperator.createVarchar(RandomStringUtils.randomAlphabetic(10));
        }
        throw new NotImplementedException("not supported");
    }

    private ScalarOperator generatePredicate() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ScalarOperator res = null;
        for (int i = 0; i < predicateSize; i++) {
            ColumnRefOperator ref = randomColumn(factory);
            ConstantOperator constant = randomConstant(ref.getType());
            BinaryType b = randomBinary();
            BinaryPredicateOperator predicate = new BinaryPredicateOperator(b, ref, constant);

            if (res == null) {
                res = predicate;
            } else {
                int orAnd = ThreadLocalRandom.current().nextInt(2);
                CompoundPredicateOperator.CompoundType type = orAnd == 0 ? OR : AND;
                res = new CompoundPredicateOperator(type, res, predicate);
            }
        }
        System.err.println("generate predicate: " + res.toString());
        return res;
    }

    private ScalarOperator generateDisjunctive() {
        ColumnRefFactory factory = new ColumnRefFactory();
        List<ScalarOperator> disjuntiveList = Lists.newArrayList();
        int conjunctSize = 3;
        for (int i = 0; i < predicateSize / conjunctSize; i++) {
            ScalarOperator conjunct;
            List<ScalarOperator> conjuncts = Lists.newArrayList();
            for (int j = 0; j < conjunctSize; j++) {
                ColumnRefOperator ref = randomColumn(factory);
                ConstantOperator constant = randomConstant(ref.getType());
                BinaryType b = randomBinary();
                conjuncts.add(new BinaryPredicateOperator(b, ref, constant));
            }

            disjuntiveList.add(Utils.compoundAnd(conjuncts));
        }
        return Utils.compoundOr(disjuntiveList);
    }

    @Benchmark
    public void bench_NormalizePredicate_Random() {
        ScalarOperator res = MvUtils.canonizePredicateForRewrite(null, randomPredicate);
    }

    /**
     * (a = 1 and b = 2 and c = 3)
     * OR (a = 2 and b = 3 and c = 4)
     * OR (a = 3 and b = 3 and c = 4)
     * ....
     */
    @Benchmark
    public void bench_NormalizePredicate_Disjunctive() {
        ScalarOperator res =
                MvUtils.canonizePredicateForRewrite(null, disjunctive);
    }
}
