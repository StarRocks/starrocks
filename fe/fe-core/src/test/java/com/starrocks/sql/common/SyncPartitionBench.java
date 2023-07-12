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
import com.google.common.collect.Sets;
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
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    static class RangeComparator implements Comparator<Range<PartitionKey>> {
        @Override
        public int compare(Range<PartitionKey> a, Range<PartitionKey> b) {
            int lower = a.lowerEndpoint().compareTo(b.lowerEndpoint());
            int higher = a.upperEndpoint().compareTo(b.upperEndpoint());
            if (lower < 0 && higher < 0) {
                return -1;
            } else if (lower > 0 && higher > 0) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    public static final RangeComparator RANGE_COMPARATOR = new RangeComparator();
    private final NavigableMap<Range<PartitionKey>, String> sortedSrcRangeMap = Maps.newTreeMap(RANGE_COMPARATOR);
    private final NavigableMap<Range<PartitionKey>, String> sortedDstRangeMap = Maps.newTreeMap(RANGE_COMPARATOR);

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
            sortedSrcRangeMap.put(createRange(curDateFormat, nextDateFormat), curDateFormat);
            if (((double) i / times) < diffRatio) {
                dstRangeMap.put(curDateFormat, createRange(curDateFormat, nextDateFormat));
                sortedDstRangeMap.put(createRange(curDateFormat, nextDateFormat), curDateFormat);
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
    @Benchmark
    public void diffRangeBench() {
        SyncPartitionUtils.diffRange(srcRangeMap, dstRangeMap);
    }

    /**
     * Benchmark                                          (diffRatio)  (times)  Mode  Cnt     Score      Error  Units
     * SyncPartitionBench.generatePartitionRefMapBench            0.0    10000  avgt    3     0.519 ±    0.294  ms/op
     * SyncPartitionBench.generatePartitionRefMapBench            0.2    10000  avgt    3  1151.045 ± 1253.707  ms/op
     * SyncPartitionBench.generatePartitionRefMapBench            0.5    10000  avgt    3  2930.250 ±  559.559  ms/op
     * SyncPartitionBench.generatePartitionRefMapBench            0.8    10000  avgt    3  5429.398 ± 5800.045  ms/op
     * SyncPartitionBench.generatePartitionRefMapBench            1.0    10000  avgt    3  7270.963 ± 8934.326  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV2          0.0    10000  avgt    3     0.608 ±    0.220  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV2          0.2    10000  avgt    3     9.022 ±    1.003  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV2          0.5    10000  avgt    3    12.499 ±    1.411  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV2          0.8    10000  avgt    3    29.210 ±  195.279  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV2          1.0    10000  avgt    3    26.178 ±   14.617  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV3          0.0    10000  avgt    3     9.289 ±    3.088  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV3          0.2    10000  avgt    3    15.637 ±    5.514  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV3          0.5    10000  avgt    3    53.742 ±  259.085  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV3          0.8    10000  avgt    3    55.790 ±   13.173  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV3          1.0    10000  avgt    3    43.172 ±   16.877  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV4          0.0    10000  avgt    3   0.456 ±  0.491  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV4          0.2    10000  avgt    3  19.407 ± 12.175  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV4          0.5    10000  avgt    3  23.905 ±  9.250  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV4          0.8    10000  avgt    3  28.735 ± 34.026  ms/op
     * SyncPartitionBench.generatePartitionRefMapBenchV4          1.0    10000  avgt    3  33.426 ± 18.251  ms/op
     */
    @Benchmark
    public void generatePartitionRefMapBench() {
        generatePartitionRefMap(srcRangeMap, dstRangeMap);
    }
    @Benchmark
    public void generatePartitionRefMapBenchV2() {
        generatePartitionRefMapV2(sortedSrcRangeMap, sortedDstRangeMap);
    }
    @Benchmark
    public void generatePartitionRefMapBenchV3() {
        generatePartitionRefMapV3(srcRangeMap, dstRangeMap);
    }
    @Benchmark
    public void generatePartitionRefMapBenchV4() {
        generatePartitionRefMapV4(srcRangeMap, dstRangeMap);
    }

    public static Map<String, Set<String>> generatePartitionRefMap(Map<String, Range<PartitionKey>> srcRangeMap,
                                                                   Map<String, Range<PartitionKey>> dstRangeMap) {
        Map<String, Set<String>> result = Maps.newHashMap();
        for (Map.Entry<String, Range<PartitionKey>> srcEntry : srcRangeMap.entrySet()) {
            Iterator<Map.Entry<String, Range<PartitionKey>>> dstIter = dstRangeMap.entrySet().iterator();
            result.put(srcEntry.getKey(), Sets.newHashSet());
            while (dstIter.hasNext()) {
                Map.Entry<String, Range<PartitionKey>> dstEntry = dstIter.next();
                Range<PartitionKey> dstRange = dstEntry.getValue();
                int upperLowerCmp = srcEntry.getValue().upperEndpoint().compareTo(dstRange.lowerEndpoint());
                if (upperLowerCmp <= 0) {
                    continue;
                }
                int lowerUpperCmp = srcEntry.getValue().lowerEndpoint().compareTo(dstRange.upperEndpoint());
                if (lowerUpperCmp >= 0) {
                    continue;
                }
                Set<String> dstNames = result.get(srcEntry.getKey());
                dstNames.add(dstEntry.getKey());
            }
        }
        return result;
    }

    public static Map<String, Set<String>> generatePartitionRefMapV2(NavigableMap<Range<PartitionKey>, String> srcRangeMap,
                                                                     NavigableMap<Range<PartitionKey>, String> dstRangeMap) {
        Map<String, Set<String>> result = Maps.newHashMap();
        srcRangeMap.values().stream().forEach(x -> result.put(x, Sets.newHashSet()));
        if (dstRangeMap.isEmpty()) {
            return result;
        }

        Range<PartitionKey> dstLowestKey = dstRangeMap.firstKey();
        Range<PartitionKey> dstHighestKey = dstRangeMap.lastKey();

        for (Map.Entry<Range<PartitionKey>, String> srcEntry : srcRangeMap.entrySet()) {
            Range<PartitionKey> srcRange = srcEntry.getKey();
            if (srcRange.upperEndpoint().compareTo(dstLowestKey.lowerEndpoint()) < 0 ||
                    srcRange.lowerEndpoint().compareTo(dstHighestKey.upperEndpoint()) > 0) {
                continue;
            }
            dstLowestKey = dstRangeMap.lowerKey(srcRange);
            dstHighestKey = dstRangeMap.higherKey(srcRange);
            if (dstLowestKey == null && dstHighestKey == null) {
                continue;
            }
            if (dstLowestKey == null) {
                dstLowestKey = dstRangeMap.firstKey();
            }
            if (dstHighestKey == null) {
                dstHighestKey = dstRangeMap.lastKey();
            }
            SortedMap<Range<PartitionKey>, String> subDstRangeMap =
                    dstRangeMap.subMap(dstLowestKey, true, dstHighestKey, true);
            Iterator<Map.Entry<Range<PartitionKey>, String>> dstIter = subDstRangeMap.entrySet().iterator();
            while (dstIter.hasNext()) {
                Map.Entry<Range<PartitionKey>, String> dstEntry = dstIter.next();
                Range<PartitionKey> dstRange = dstEntry.getKey();
                // NOTE: cannot use Range::isConnected yet!
                if (srcRange.upperEndpoint().compareTo(dstRange.lowerEndpoint()) > 0 &&
                        srcRange.lowerEndpoint().compareTo(dstRange.upperEndpoint()) < 0) {
                    result.computeIfAbsent(srcEntry.getValue(), x -> Sets.newHashSet())
                            .add(dstEntry.getValue());
                }
            }
        }
        return result;
    }

    public static Map<String, Set<String>> generatePartitionRefMapV3(Map<String, Range<PartitionKey>> srcRangeMap,
                                                                     Map<String, Range<PartitionKey>> dstRangeMap) {
        NavigableMap<Range<PartitionKey>, String> sortedSrcRangeMap = Maps.newTreeMap(RANGE_COMPARATOR);
        NavigableMap<Range<PartitionKey>, String> sortedDstRangeMap = Maps.newTreeMap(RANGE_COMPARATOR);
        for (Map.Entry<String, Range<PartitionKey>> e : srcRangeMap.entrySet()) {
            sortedSrcRangeMap.put(e.getValue(), e.getKey());
        }
        for (Map.Entry<String, Range<PartitionKey>> e : dstRangeMap.entrySet()) {
            sortedDstRangeMap.put(e.getValue(), e.getKey());
        }
        return generatePartitionRefMapV2(sortedSrcRangeMap, sortedDstRangeMap);
    }

    public static Map<String, Set<String>> generatePartitionRefMapV4(Map<String, Range<PartitionKey>> srcRangeMap,
                                                                     Map<String, Range<PartitionKey>> dstRangeMap) {
        Map<String, Set<String>> result = Maps.newHashMap();
        srcRangeMap.keySet().stream().forEach(x -> result.put(x, Sets.newHashSet()));
        if (dstRangeMap.isEmpty()) {
            return result;
        }

        // TODO: Callers may use `List<PartitionRange>` directly.
        List<PartitionRange> srcRanges = srcRangeMap.keySet().stream().map(name -> new PartitionRange(name,
                srcRangeMap.get(name))).collect(Collectors.toList());
        List<PartitionRange> dstRanges = dstRangeMap.keySet().stream().map(name -> new PartitionRange(name,
                dstRangeMap.get(name))).collect(Collectors.toList());
        Collections.sort(srcRanges, PartitionRange::compareTo);
        Collections.sort(dstRanges, PartitionRange::compareTo);

        for (PartitionRange srcRange : srcRanges) {
            int mid = Collections.binarySearch(dstRanges, srcRange);
            if (mid < 0) {
                continue;
            }
            Set<String> addedSet = result.get(srcRange.getPartitionName());
            addedSet.add(dstRanges.get(mid).getPartitionName());

            int lower = mid - 1;
            while (lower >= 0 && dstRanges.get(lower).isInteract(srcRange)) {
                addedSet.add(dstRanges.get(lower).getPartitionName());
                lower--;
            }

            int higher = mid + 1;
            while (higher < dstRanges.size() && dstRanges.get(higher).isInteract(srcRange)) {
                addedSet.add(dstRanges.get(higher).getPartitionName());
                higher++;
            }
        }
        return result;
    }
}
