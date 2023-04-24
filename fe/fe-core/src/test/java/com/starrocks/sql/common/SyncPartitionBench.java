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


package com.starrocks.sql.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.ast.PartitionValue;
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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Before:
 * Benchmark                          (diffRatio)  (times)  Mode  Cnt     Score       Error  Units
 * SyncPartitionBench.diffRangeBench          0.0    10000  avgt    3     9.707 ±    28.411  ms/op
 * SyncPartitionBench.diffRangeBench          0.2    10000  avgt    3  1065.355 ±  2022.494  ms/op
 * SyncPartitionBench.diffRangeBench          0.5    10000  avgt    3  3559.056 ±  3291.629  ms/op
 * SyncPartitionBench.diffRangeBench          0.8    10000  avgt    3  8779.244 ± 44517.617  ms/op
 * SyncPartitionBench.diffRangeBench          1.0    10000  avgt    3  3225.946 ±  2554.938  ms/op
 *
 * After(Optimize):
 * Benchmark                          (diffRatio)  (times)  Mode  Cnt  Score    Error  Units
 *  SyncPartitionBench.diffRangeBench          0.0    10000  avgt    3  0.539 ±  1.183  ms/op
 *  SyncPartitionBench.diffRangeBench          0.2    10000  avgt    3  0.875 ±  1.059  ms/op
 *  SyncPartitionBench.diffRangeBench          0.5    10000  avgt    3  1.007 ±  2.012  ms/op
 *  SyncPartitionBench.diffRangeBench          0.8    10000  avgt    3  1.219 ±  2.082  ms/op
 *  SyncPartitionBench.diffRangeBench          1.0    10000  avgt    3  6.453 ± 15.021  ms/op
 */

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class SyncPartitionBench {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SyncPartitionBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Param({"10000"})
    public int times;

    @Param({"0.0", "0.2", "0.5", "0.8", "1.0"})
    public float diffRatio;

    private static final Date START_DATETIME = TimeUtils.getTimeAsDate("2000-01-01 00:00:00");
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Column PARTITION_COLUMN = new Column("k1", ScalarType.DATETIME);

    private final Map<String, Range<PartitionKey>> srcRangeMap = Maps.newHashMap();
    private final Map<String, Range<PartitionKey>> dstRangeMap = Maps.newHashMap();

    private static Range<PartitionKey> createRangeImpl(PartitionValue lowerValue, PartitionValue upperValue)
            throws AnalysisException {
        PartitionKey lowerBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue),
                Collections.singletonList(PARTITION_COLUMN));
        PartitionKey upperBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                Collections.singletonList(PARTITION_COLUMN));
        return Range.closedOpen(lowerBoundPartitionKey, upperBoundPartitionKey);
    }

    private static Range<PartitionKey> createRange(String lowerBound, String upperBound) throws AnalysisException {
        PartitionValue lowerValue = new PartitionValue(lowerBound);
        PartitionValue upperValue = new PartitionValue(upperBound);
        return createRangeImpl(lowerValue, upperValue);
    }

    public static Date nextHour(Date date, int hours) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, hours);
        return calendar.getTime();
    }

    public static String dateToString(Date date) {
        return DATETIME_FORMAT.format(date);
    }

    @Setup
    public void setup() throws Exception {
        Date curDate = START_DATETIME;
        Date nextDate = nextHour(curDate, 1);
        for (int i = 0; i < times; i++) {
            String curDateFormat = dateToString(curDate);
            String nextDateFormat = dateToString(nextDate);
            srcRangeMap.put(curDateFormat, createRange(curDateFormat, nextDateFormat));
            if (((double) i / times) < diffRatio) {
                dstRangeMap.put(curDateFormat, createRange(curDateFormat, nextDateFormat));
            }

            Date tmpDate = nextHour(nextDate, 1);
            curDate = nextDate;
            nextDate = tmpDate;
        }
        System.out.println();
        System.out.println("times=" + times + ", diffRatio=" + diffRatio);
        System.out.println("srRangeMap size=" + srcRangeMap.size());
        System.out.println("dstRangeMap size =" + dstRangeMap.size());
    }

    @Benchmark
    public void diffRangeBench() {
        SyncPartitionUtils.diffRange(srcRangeMap, dstRangeMap);
    }
}
