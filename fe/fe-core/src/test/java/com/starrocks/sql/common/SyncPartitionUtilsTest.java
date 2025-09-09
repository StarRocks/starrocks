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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.mv.MVEagerRangePartitionMapper;
import com.starrocks.sql.common.mv.MVLazyRangePartitionMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.PRangeCell.toRangeMap;

public class SyncPartitionUtilsTest {

    private static final TableName TABLE_NAME = new TableName("db1", "table1");
    private static SlotRef slotRef;
    private static Column partitionColumn;

    @BeforeAll
    public static void beforeClass() throws Exception {
        slotRef = new SlotRef(TABLE_NAME, "k1");
        partitionColumn = new Column("k1", ScalarType.DATETIME);
    }

    private static Range<PartitionKey> createRangeImpl(PartitionValue lowerValue, PartitionValue upperValue)
            throws AnalysisException {
        PartitionKey lowerBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue),
                Collections.singletonList(partitionColumn));
        PartitionKey upperBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                Collections.singletonList(partitionColumn));
        return Range.closedOpen(lowerBoundPartitionKey, upperBoundPartitionKey);
    }

    private static Range<PartitionKey> createRange(DateLiteral lower, DateLiteral upper) throws AnalysisException {
        return createRange(lower.getStringValue(), upper.getStringValue());
    }

    private static Range<PartitionKey> createRange(String lowerBound, String upperBound) throws AnalysisException {
        PartitionValue lowerValue = new PartitionValue(lowerBound);
        PartitionValue upperValue = new PartitionValue(upperBound);
        return createRangeImpl(lowerValue, upperValue);
    }

    private static Range<PartitionKey> createMaxValueRange(String lowerBound) throws AnalysisException {
        PartitionValue lowerValue = new PartitionValue(lowerBound);
        PartitionValue upperValue = PartitionValue.MAX_VALUE;
        return createRangeImpl(lowerValue, upperValue);
    }

    private static Range<PartitionKey> createLessThanRange(String upperBound) throws AnalysisException {
        PartitionValue upperValue = new PartitionValue(upperBound);
        PartitionKey lowerBoundPartitionKey = PartitionKey.createInfinityPartitionKey(
                Collections.singletonList(partitionColumn), false);
        PartitionKey upperBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                Collections.singletonList(partitionColumn));
        return Range.closedOpen(lowerBoundPartitionKey, upperBoundPartitionKey);
    }

    private static FunctionCallExpr createDateTruncFunc(String granularity, PrimitiveType type) {
        List<Expr> children = new ArrayList<>();
        children.add(new StringLiteral(granularity));
        children.add(slotRef);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", children);
        functionCallExpr.setType(Type.fromPrimitiveType(type));
        return functionCallExpr;
    }

    private static PartitionDiff getRangePartitionDiffOfSlotRef(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                Map<String, Range<PartitionKey>> mvRangeMap) {
        return SyncPartitionUtils.getRangePartitionDiffOfSlotRef(baseRangeMap, mvRangeMap, null);
    }

    @Test
    public void testGeneratePartitionRefMap() throws AnalysisException {
        // normal condition
        Map<String, Range<PartitionKey>> srcRangeMap = Maps.newHashMap();
        srcRangeMap.put("p20201015_20201115", createRange("2020-10-15", "2020-11-15"));
        srcRangeMap.put("p20201115_20201215", createRange("2020-11-15", "2020-12-15"));

        Map<String, Range<PartitionKey>> dstRangeMap = Maps.newHashMap();
        dstRangeMap.put("p202010_202011", createRange("2020-10-01", "2020-11-01"));
        dstRangeMap.put("p202011_202012", createRange("2020-11-01", "2020-12-01"));
        dstRangeMap.put("p202012_202101", createRange("2020-12-01", "2021-01-01"));

        Map<String, Set<String>> partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(srcRangeMap, dstRangeMap);

        Assertions.assertTrue(partitionRefMap.get("p20201015_20201115").contains("p202010_202011"));
        Assertions.assertTrue(partitionRefMap.get("p20201015_20201115").contains("p202011_202012"));
        Assertions.assertTrue(partitionRefMap.get("p20201115_20201215").contains("p202011_202012"));
        Assertions.assertTrue(partitionRefMap.get("p20201115_20201215").contains("p202012_202101"));

        partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(dstRangeMap, srcRangeMap);

        Assertions.assertTrue(partitionRefMap.get("p202010_202011").contains("p20201015_20201115"));
        Assertions.assertTrue(partitionRefMap.get("p202011_202012").contains("p20201015_20201115"));
        Assertions.assertTrue(partitionRefMap.get("p202011_202012").contains("p20201115_20201215"));
        Assertions.assertTrue(partitionRefMap.get("p202012_202101").contains("p20201115_20201215"));

        // test border
        srcRangeMap = Maps.newHashMap();
        srcRangeMap.put("p20201015", createRange("2020-10-15", "2020-11-01"));

        dstRangeMap = Maps.newHashMap();
        dstRangeMap.put("p202011_202012", createRange("2020-11-01", "2020-12-01"));

        partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(srcRangeMap, dstRangeMap);
        Assertions.assertEquals(0, partitionRefMap.get("p20201015").size());

        partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(dstRangeMap, srcRangeMap);
        Assertions.assertEquals(0, partitionRefMap.get("p202011_202012").size());

    }

    @Test
    public void testGeneratePartitionRefMapOneByOne() throws AnalysisException {
        Map<String, Range<PartitionKey>> srcRangeMap = Maps.newHashMap();
        srcRangeMap.put("p202010_202011", createRange("2020-10-01", "2020-11-01"));
        srcRangeMap.put("p202011_202012", createRange("2020-11-01", "2020-12-01"));
        srcRangeMap.put("p202012_202101", createRange("2020-12-01", "2021-01-01"));

        Map<String, Range<PartitionKey>> dstRangeMap = Maps.newHashMap();
        dstRangeMap.put("p202010_202011", createRange("2020-10-01", "2020-11-01"));
        dstRangeMap.put("p202011_202012", createRange("2020-11-01", "2020-12-01"));
        dstRangeMap.put("p202012_202101", createRange("2020-12-01", "2021-01-01"));

        Map<String, Set<String>> partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(srcRangeMap, dstRangeMap);

        Assertions.assertEquals(1, partitionRefMap.get("p202010_202011").size());
        Assertions.assertEquals(1, partitionRefMap.get("p202011_202012").size());
        Assertions.assertEquals(1, partitionRefMap.get("p202012_202101").size());

        Assertions.assertTrue(partitionRefMap.get("p202010_202011").contains("p202010_202011"));
        Assertions.assertTrue(partitionRefMap.get("p202011_202012").contains("p202011_202012"));
        Assertions.assertTrue(partitionRefMap.get("p202012_202101").contains("p202012_202101"));
    }

    private Map<String, Range<PartitionKey>> diffRange(Map<String, Range<PartitionKey>> srcRange,
                                                       Map<String, Range<PartitionKey>> dstRange) {
        Map<String, PCell> result = RangePartitionDiffer.diffRange(srcRange, dstRange);
        return toRangeMap(result);
    }

    private Map<String, PListCell> diffList(Map<String, PCell> baseListMap,
                                            Map<String, PCell> mvListMap) {
        Map<String, PCell> result = ListPartitionDiffer.diffList(baseListMap, mvListMap,
                mvListMap.keySet().stream().collect(Collectors.toSet()));
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> (PListCell) entry.getValue()));
    }

    @Test
    public void testDiffRange() throws AnalysisException {

        // normal condition
        Map<String, Range<PartitionKey>> srcRange = Maps.newHashMap();
        srcRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        srcRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));

        Map<String, Range<PartitionKey>> dstRange = Maps.newHashMap();
        dstRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));

        Map<String, Range<PartitionKey>> diff = diffRange(srcRange, dstRange);
        Assertions.assertEquals(1, diff.size());
        Assertions.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-03 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());

        diff = diffRange(dstRange, srcRange);
        Assertions.assertEquals(0, diff.size());

        // two range
        srcRange = Maps.newHashMap();

        srcRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        srcRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));
        srcRange.put("p20200105", createRange("2020-01-05", "2020-01-06"));
        srcRange.put("p20200106", createRange("2020-01-06", "2020-01-07"));

        dstRange = Maps.newHashMap();
        dstRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        dstRange.put("p20200102", createRange("2020-01-02", "2020-01-06"));
        dstRange.put("p20200106", createRange("2020-01-06", "2020-01-07"));

        diff = diffRange(srcRange, dstRange);
        Assertions.assertEquals(2, diff.size());
        Assertions.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-03 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-05 00:00:00",
                diff.get("p20200105").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-06 00:00:00",
                diff.get("p20200105").upperEndpoint().getKeys().get(0).getStringValue());

        diff = diffRange(dstRange, srcRange);
        Assertions.assertEquals(1, diff.size());
        Assertions.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-06 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
    }

    private PListCell makeCell(String... values) {
        List<List<String>> items = Lists.newArrayList();
        for (String value : values) {
            items.add(Lists.newArrayList(value));
        }
        return new PListCell(items);
    }

    private void addIntoListPartitionMap(Map<String, PCell> map, String partitionName, String... values) {
        map.put(partitionName, makeCell(values));
    }

    @Test
    public void testDiffList() {
        // same
        Map<String, PCell> baseListMap = Maps.newHashMap();
        addIntoListPartitionMap(baseListMap, "p20230619", "2023-06-19");
        addIntoListPartitionMap(baseListMap, "p20230620", "2023-06-20");
        addIntoListPartitionMap(baseListMap, "p20230621", "2023-06-21");

        Map<String, PCell> mvListMap = Maps.newHashMap();
        addIntoListPartitionMap(mvListMap, "p20230619", "2023-06-19");
        addIntoListPartitionMap(mvListMap, "p20230621", "2023-06-21");
        addIntoListPartitionMap(mvListMap, "p20230620", "2023-06-20");

        Map<String, PListCell> diff = diffList(baseListMap, mvListMap);
        Assertions.assertEquals(0, diff.size());

        baseListMap = Maps.newHashMap();
        addIntoListPartitionMap(baseListMap, "p20230619", "2023-06-19");
        addIntoListPartitionMap(baseListMap, "p20230620", "2023-06-20");

        mvListMap = Maps.newHashMap();
        addIntoListPartitionMap(mvListMap, "p20230619", "2023-06-19");

        diff = diffList(baseListMap, mvListMap);
        Assertions.assertEquals(1, diff.size());
        Assertions.assertEquals("2023-06-20", diff.get("p20230620").getPartitionItems().iterator().next().get(0));

        baseListMap = Maps.newHashMap();
        addIntoListPartitionMap(baseListMap, "p20230619", "2023-06-19");

        mvListMap = Maps.newHashMap();
        addIntoListPartitionMap(mvListMap, "p20230619", "2023-06-19");
        addIntoListPartitionMap(mvListMap, "p20230620", "2023-06-20");

        diff = diffList(baseListMap, mvListMap);
        Assertions.assertEquals(0, diff.size());
    }

    @Test
    public void testCalcSyncSameRangePartition() throws AnalysisException {

        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        baseRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        baseRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));
        baseRange.put("p20200131", createRange("2020-01-31", "2020-02-01"));

        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();
        mvRange.put("p202001", createRange("2020-01-01", "2020-02-01"));

        PartitionDiff diff = getRangePartitionDiffOfSlotRef(baseRange, mvRange);

        Map<String, Range<PartitionKey>> adds = toRangeMap(diff.getAdds());
        Assertions.assertEquals(3, adds.size());
        Assertions.assertEquals("2020-01-01 00:00:00",
                adds.get("p20200101").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-02 00:00:00",
                adds.get("p20200101").upperEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-02 00:00:00",
                adds.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-03 00:00:00",
                adds.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-31 00:00:00",
                adds.get("p20200131").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-02-01 00:00:00",
                adds.get("p20200131").upperEndpoint().getKeys().get(0).getStringValue());

        Map<String, Range<PartitionKey>> deletes = toRangeMap(diff.getDeletes());
        Assertions.assertEquals(1, deletes.size());
        Assertions.assertEquals("2020-01-01 00:00:00",
                deletes.get("p202001").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-02-01 00:00:00",
                deletes.get("p202001").upperEndpoint().getKeys().get(0).getStringValue());

        // rolling scenario
        baseRange = Maps.newHashMap();
        baseRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));
        baseRange.put("p20200103", createRange("2020-01-03", "2020-01-04"));
        baseRange.put("p20200104", createRange("2020-01-04", "2020-01-05"));

        mvRange = Maps.newHashMap();
        mvRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        mvRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));
        mvRange.put("p20200103", createRange("2020-01-03", "2020-01-04"));

        diff = getRangePartitionDiffOfSlotRef(baseRange, mvRange);

        adds = toRangeMap(diff.getAdds());
        deletes = toRangeMap(diff.getDeletes());

        Assertions.assertEquals(1, adds.size());
        Assertions.assertEquals("2020-01-04 00:00:00",
                adds.get("p20200104").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-05 00:00:00",
                adds.get("p20200104").upperEndpoint().getKeys().get(0).getStringValue());

        Assertions.assertEquals(1, deletes.size());
        Assertions.assertEquals("2020-01-01 00:00:00",
                deletes.get("p20200101").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-01-02 00:00:00",
                deletes.get("p20200101").upperEndpoint().getKeys().get(0).getStringValue());
    }

    private static PartitionMapping toLazyMappingRange(Range<PartitionKey> baseRange,
                                                       String granularity) {
        return MVLazyRangePartitionMapper.INSTANCE.toMappingRanges(baseRange, granularity);
    }

    private static List<PartitionMapping> toEagerMappingRanges(Range<PartitionKey> baseRange,
                                                               String granularity) {
        return MVEagerRangePartitionMapper.INSTANCE.toMappingRanges(baseRange, granularity);
    }

    public static Map<String, Range<PartitionKey>> toEagerMappingRanges(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                        String granularity, PrimitiveType partitionType) {
        return MVEagerRangePartitionMapper.INSTANCE.toMappingRanges(baseRangeMap, granularity, partitionType);
    }

    private static List<PartitionMapping> toPartitionMappings(Range<PartitionKey> baseRange, String granularity) {
        return toEagerMappingRanges(baseRange, granularity);
    }

    private static PartitionMapping toPartitionMapping(Range<PartitionKey> baseRange, String granularity) {
        List<PartitionMapping> partitionMappings = toEagerMappingRanges(baseRange, granularity);
        Preconditions.checkState(partitionMappings.size() == 1);
        return partitionMappings.get(0);
    }

    @Test
    public void testMappingRangeRollup() throws AnalysisException {
        // minute
        Range<PartitionKey> baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        PartitionMapping mappedRange = toLazyMappingRange(baseRange, "minute");

        Assertions.assertEquals("2020-05-03T12:34:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-06-04T12:35:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // hour
        baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        mappedRange = toLazyMappingRange(baseRange, "hour");

        Assertions.assertEquals("2020-05-03T12:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-06-04T13:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // day
        baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        mappedRange = toLazyMappingRange(baseRange, "day");

        Assertions.assertEquals("2020-05-03T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-06-05T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // month
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = toLazyMappingRange(baseRange, "month");

        Assertions.assertEquals("2020-05-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-07-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // quarter
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = toLazyMappingRange(baseRange, "quarter");

        Assertions.assertEquals("2020-04-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-07-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // year
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = toLazyMappingRange(baseRange, "year");

        Assertions.assertEquals("2020-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2021-01-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testMappingRangeRollupWithMaxValue() throws AnalysisException {
        String maxValueDate =
                new DateLiteral(Type.DATE, true).toLocalDateTime().format(DateTimeFormatter.ISO_DATE_TIME);
        // minute
        Range<PartitionKey> baseRange = createMaxValueRange("2020-05-03 12:34:56");
        PartitionMapping mappedRange = toPartitionMapping(baseRange, "minute");

        Assertions.assertEquals("2020-05-03T12:34:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // hour
        baseRange = createMaxValueRange("2020-05-03 12:34:56");
        mappedRange = toPartitionMapping(baseRange, "hour");

        Assertions.assertEquals("2020-05-03T12:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // day
        baseRange = createMaxValueRange("2020-05-03 12:34:56");
        mappedRange = toPartitionMapping(baseRange, "day");

        Assertions.assertEquals("2020-05-03T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // month
        baseRange = createMaxValueRange("2020-05-03");
        mappedRange = toPartitionMapping(baseRange, "month");

        Assertions.assertEquals("2020-05-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // quarter
        baseRange = createMaxValueRange("2020-05-03");
        mappedRange = toPartitionMapping(baseRange, "quarter");

        Assertions.assertEquals("2020-04-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // year
        baseRange = createMaxValueRange("2020-05-03");
        mappedRange = toPartitionMapping(baseRange, "year");

        Assertions.assertEquals("2020-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testMappingRangeList() throws AnalysisException {
        Map<String, Range<PartitionKey>> baseRangeMap = Maps.newHashMap();
        Map<String, Range<PartitionKey>> result;
        baseRangeMap.put("p202001", createRange("2020-01-01", "2020-02-01"));
        baseRangeMap.put("p202002", createRange("2020-02-01", "2020-03-01"));
        baseRangeMap.put("p202003", createRange("2020-03-01", "2020-04-01"));
        baseRangeMap.put("p202004", createMaxValueRange("2020-04-01"));

        result = toEagerMappingRanges(baseRangeMap, "month", PrimitiveType.DATE);

        Assertions.assertTrue(result.containsKey("p202004_999912"));
        Assertions.assertEquals(1, result.get("p202004_999912").upperEndpoint().getKeys().size());
        Assertions.assertEquals("9999-12-31", result.get("p202004_999912").upperEndpoint().getKeys().get(0).
                getStringValue());

        baseRangeMap.clear();
        baseRangeMap.put("p202001", createRange("2020-01-01 12:00:25", "2020-02-01 20:01:59"));
        baseRangeMap.put("p202002", createRange("2020-02-01 20:01:59", "2020-03-01 02:50:49"));
        baseRangeMap.put("p202003", createRange("2020-03-01 02:50:49", "2020-04-01 01:05:06"));
        baseRangeMap.put("p202004", createMaxValueRange("2020-04-01 01:05:06"));
        result = toEagerMappingRanges(baseRangeMap, "hour", PrimitiveType.DATETIME);
        Assertions.assertTrue(result.size() == 2175);
    }

    @Test
    public void testMappingRangeWithOldVersion() throws AnalysisException {
        // less than
        Range<PartitionKey> baseRange = createLessThanRange("2020-05-03");
        PartitionMapping mappedRange = toPartitionMapping(baseRange, "day");

        Assertions.assertEquals("0000-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-05-03T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // big partition
        baseRange = createRange("2020-01-01", "2020-02-01");
        mappedRange = toLazyMappingRange(baseRange, "day");
        Assertions.assertEquals("2020-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-02-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testMappingRange() throws AnalysisException {
        // less than
        Range<PartitionKey> baseRange = createLessThanRange("2020-05-03");
        PartitionMapping mappedRange = toPartitionMapping(baseRange, "day");

        Assertions.assertEquals("0000-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assertions.assertEquals("2020-05-03T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // big partition
        baseRange = createRange("2020-01-01", "2020-02-01");
        {
            List<PartitionMapping> mappedRanges = toPartitionMappings(baseRange, "day");
            Assertions.assertTrue(mappedRanges.size() == 31);
        }
        {
            List<PartitionMapping> mappedRanges = toPartitionMappings(baseRange, "month");
            Assertions.assertTrue(mappedRanges.size() == 1);
        }
        {
            List<PartitionMapping> mappedRanges = toPartitionMappings(baseRange, "year");
            Assertions.assertTrue(mappedRanges.size() == 1);
        }
    }

    @Test
    public void testCalcSyncRollupSpecial() throws AnalysisException {

        // less than
        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        Range<PartitionKey> basePartition = createLessThanRange("2020-05-03");
        baseRange.put("p1", basePartition);
        baseRange.put("p2", createRange("2020-05-04", "2020-11-12"));

        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();
        PartitionDiff diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                createDateTruncFunc("month", PrimitiveType.DATETIME), null);
        Map<String, Range<PartitionKey>> adds = toRangeMap(diff.getAdds());
        Map<String, Range<PartitionKey>> deletes = toRangeMap(diff.getDeletes());
        Assertions.assertEquals(8, adds.size());
        Assertions.assertEquals(0, deletes.size());
        Set<String> expectPartNames = ImmutableSet.of(
                "p000101_202005",
                "p202005_202006",
                "p202006_202007",
                "p202007_202008",
                "p202008_202009",
                "p202009_202010",
                "p202010_202011",
                "p202011_202012"
        );
        Assertions.assertTrue(expectPartNames.containsAll(adds.keySet()));

        // big partition
        baseRange = Maps.newHashMap();
        baseRange.put("p202001", createRange("2020-01-01", "2020-02-01"));

        mvRange = Maps.newHashMap();
        mvRange.put("p20200101_20200102", createRange("2020-01-01", "2020-01-02"));
        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                createDateTruncFunc("day", PrimitiveType.DATETIME), null);
        adds = toRangeMap(diff.getAdds());
        deletes = toRangeMap(diff.getDeletes());

        System.out.println(adds);
        System.out.println(deletes);
    }

    static class EPartitionMapping {
        private final String name;
        private final String lowerEndpoint;
        private final String upperEndpoint;

        public EPartitionMapping(String name, String lowerEndpoint, String upperEndpoint) {
            this.name = name;
            this.lowerEndpoint = lowerEndpoint;
            this.upperEndpoint = upperEndpoint;
        }

        public String getName() {
            return this.name;
        }

        public String getLowerEndpoint() {
            return lowerEndpoint;
        }

        public String getUpperEndpoint() {
            return upperEndpoint;
        }
    }

    private static boolean checkPartitionMapping(Map<String, Range<PartitionKey>> actuals,
                                                 EPartitionMapping expect) {
        return checkPartitionMapping(actuals, ImmutableList.of(expect));
    }

    private static boolean checkPartitionMapping(Map<String, Range<PartitionKey>> actuals,
                                                 List<EPartitionMapping> expects) {
        if (actuals.size() != expects.size()) {
            return false;
        }
        for (EPartitionMapping expect : expects) {
            Range<PartitionKey> actual = actuals.get(expect.getName());
            if (actual == null) {
                return false;
            }
            if (!expect.getLowerEndpoint().equals(actual.lowerEndpoint().getKeys().get(0).getStringValue())) {
                return false;
            }
            if (!expect.getUpperEndpoint().equals(actual.upperEndpoint().getKeys().get(0).getStringValue())) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testCalcSyncRollupPartition() throws AnalysisException {
        // overlap scenario
        String granularity = "month";

        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();

        baseRange.put("p1", createRange("2020-09-12", "2020-10-12"));
        baseRange.put("p2", createRange("2020-10-12", "2020-11-12"));

        PartitionDiff diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                createDateTruncFunc(granularity, PrimitiveType.DATETIME), null);

        Map<String, Range<PartitionKey>> adds = toRangeMap(diff.getAdds());
        Map<String, Range<PartitionKey>> deletes = toRangeMap(diff.getDeletes());

        Assertions.assertEquals(3, adds.size());
        Assertions.assertEquals(0, deletes.size());
        List<EPartitionMapping> expects = ImmutableList.of(
                new EPartitionMapping("p202009_202010", "2020-09-01 00:00:00", "2020-10-01 00:00:00"),
                new EPartitionMapping("p202010_202011", "2020-10-01 00:00:00", "2020-11-01 00:00:00"),
                new EPartitionMapping("p202011_202012", "2020-11-01 00:00:00", "2020-12-01 00:00:00")
        );
        Assertions.assertTrue(checkPartitionMapping(adds, expects));

        // bigger than granularity scenario
        baseRange = Maps.newHashMap();
        baseRange.put("p20200101", createRange("2020-01-01", "2021-01-01"));
        mvRange = Maps.newHashMap();
        mvRange.put("p202001_202002", createRange("2020-01-01", "2020-02-01"));

        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                createDateTruncFunc(granularity, PrimitiveType.DATETIME), null);
        adds = toRangeMap(diff.getAdds());
        deletes = toRangeMap(diff.getDeletes());
        Assertions.assertEquals(11, adds.size());
        Assertions.assertEquals(0, deletes.size());

        baseRange = Maps.newHashMap();
        baseRange.put("p20200503", createRange("2020-05-03", "2020-06-05"));
        mvRange = Maps.newHashMap();
        mvRange.put("p202005_202006", createRange("2020-05-01", "2020-06-01"));
        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                createDateTruncFunc("month", PrimitiveType.DATETIME), null);
        adds = toRangeMap(diff.getAdds());
        deletes = toRangeMap(diff.getDeletes());
        Assertions.assertEquals(1, adds.size());
        Assertions.assertEquals(0, deletes.size());
        expects = ImmutableList.of(
                new EPartitionMapping("p202006_202007", "2020-06-01 00:00:00", "2020-07-01 00:00:00")
        );
        Assertions.assertTrue(checkPartitionMapping(adds, expects));

        baseRange = Maps.newHashMap();
        baseRange.put("p20200403", createRange("2020-04-03", "2020-05-02"));
        baseRange.put("p20200503", createRange("2020-05-03", "2020-06-05"));
        mvRange = Maps.newHashMap();
        mvRange.put("p202005_202006", createRange("2020-05-01", "2020-06-01"));
        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                createDateTruncFunc("month", PrimitiveType.DATETIME), null);
        adds = toRangeMap(diff.getAdds());
        deletes = toRangeMap(diff.getDeletes());
        Assertions.assertEquals(2, adds.size());
        Assertions.assertEquals("2020-04-01 00:00:00",
                adds.get("p202004_202005").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-05-01 00:00:00",
                adds.get("p202004_202005").upperEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-06-01 00:00:00",
                adds.get("p202006_202007").lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2020-07-01 00:00:00",
                adds.get("p202006_202007").upperEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals(0, deletes.size());
    }

    private PRangeCellPlus buildPartitionRange(String name, String start, String end) throws AnalysisException {
        return new PRangeCellPlus(name, Range.closedOpen(
                PartitionKey.ofDateTime(DateUtils.parseStrictDateTime(start)),
                PartitionKey.ofDateTime(DateUtils.parseStrictDateTime(end))));
    }

    @Test
    public void test_getIntersectedPartitions() throws AnalysisException {
        List<PRangeCellPlus> srcs = Arrays.asList(
                buildPartitionRange("p20230801", "00000101", "20230801"),
                buildPartitionRange("p20230802", "20230801", "20230802"),
                buildPartitionRange("p20230803", "20230802", "20230803")
        );
        List<PRangeCellPlus> dsts = Arrays.asList(
                buildPartitionRange("p000101_202308", "0001-01-01", "2023-08-01"),
                buildPartitionRange("p202308_202309", "2023-08-01", "2023-09-01")
        );
        Map<String, Set<String>> res = SyncPartitionUtils.getIntersectedPartitions(srcs, dsts);
        Assertions.assertEquals(
                ImmutableMap.of(
                        "p20230801", ImmutableSet.of("p000101_202308"),
                        "p20230802", ImmutableSet.of("p202308_202309"),
                        "p20230803", ImmutableSet.of("p202308_202309")
                ),
                res);
    }

    @Test
    public void transferRangeHandlesNonFunctionExpression() throws Exception {
        Range<PartitionKey> baseRange = createRange("2020-01-01", "2020-02-01");
        Expr nonFunctionExpr = new SlotRef(TABLE_NAME, "column");

        Range<PartitionKey> result = SyncPartitionUtils.transferRange(baseRange, nonFunctionExpr);

        Assertions.assertEquals(baseRange, result);
    }

    @Test
    public void transferRangeHandlesUnsupportedFunction() throws Exception {
        Range<PartitionKey> baseRange = createRange("2020-01-01", "2020-02-01");
        FunctionCallExpr unsupportedFunction = new FunctionCallExpr("unsupported_function", Lists.newArrayList());

        try {
            SyncPartitionUtils.transferRange(baseRange, unsupportedFunction);
            Assertions.fail("Expected SemanticException to be thrown");
        } catch (SemanticException e) {
            Assertions.assertTrue(e.getMessage().contains("Do not support function"));
        }
    }

    private Pair<String, String> getDateTruncFuncTransform(Range<PartitionKey> range, String granularity) {
        FunctionCallExpr dateTruncFunction = createDateTruncFunc(granularity, PrimitiveType.DATE);
        Range<PartitionKey> result = SyncPartitionUtils.transferRange(range, dateTruncFunction);
        String lower = result.lowerEndpoint().getKeys().get(0).getStringValue();
        String upper = result.upperEndpoint().getKeys().get(0).getStringValue();
        return Pair.create(lower, upper);
    }
    @Test
    public void transferRangeHandlesDateTruncFunction() throws AnalysisException {
        Range<PartitionKey> baseRange = createRange("2020-01-01 12:34:56", "2020-01-02 12:34:56");

        {
            // MINUTE
            Pair<String, String> result = getDateTruncFuncTransform(baseRange, "MINUTE");
            Assertions.assertEquals("2020-01-01 12:34:00", result.first);
            Assertions.assertEquals("2020-01-02 12:35:00", result.second);
        }

        {
            // HOUR
            Pair<String, String> result = getDateTruncFuncTransform(baseRange, "HOUR");
            Assertions.assertEquals("2020-01-01 12:00:00", result.first);
            Assertions.assertEquals("2020-01-02 13:00:00", result.second);
        }

        {
            // day
            Pair<String, String> result = getDateTruncFuncTransform(baseRange, "day");
            Assertions.assertEquals("2020-01-01 00:00:00", result.first);
            Assertions.assertEquals("2020-01-03 00:00:00", result.second);
        }

        {
            // WEEK
            Pair<String, String> result = getDateTruncFuncTransform(baseRange, "WEEK");
            Assertions.assertEquals("2019-12-30 00:00:00", result.first);
            Assertions.assertEquals("2020-01-09 00:00:00", result.second);
        }

        {
            // MONTH
            Pair<String, String> result = getDateTruncFuncTransform(baseRange, "MONTH");
            Assertions.assertEquals("2020-01-01 12:34:56", result.first);
            Assertions.assertEquals("2020-02-01 12:34:56", result.second);
        }
        {
            // QUARTER
            Pair<String, String> result = getDateTruncFuncTransform(baseRange, "QUARTER");
            Assertions.assertEquals("2020-01-01 12:34:56", result.first);
            Assertions.assertEquals("2020-04-01 12:34:56", result.second);
        }
        {
            // YEAR
            Pair<String, String> result = getDateTruncFuncTransform(baseRange, "YEAR");
            Assertions.assertEquals("2020-01-01 12:34:56", result.first);
            Assertions.assertEquals("2021-01-01 12:34:56", result.second);
        }
    }

    @Test
    public void transferRangeHandlesStr2DateFunction() throws AnalysisException {
        Range<PartitionKey> baseRange = createRange("2020-01-01", "2020-02-01");
        FunctionCallExpr str2DateFunction = new FunctionCallExpr(FunctionSet.STR2DATE, Lists.newArrayList());

        Range<PartitionKey> result = SyncPartitionUtils.transferRange(baseRange, str2DateFunction);

        Assertions.assertEquals(baseRange, result);
    }

    @Test
    public void transferRangeHandlesInvalidGranularity() throws  Exception {
        Range<PartitionKey> baseRange = createRange("2020-01-01", "2020-02-01");
        FunctionCallExpr invalidGranularityFunction = createDateTruncFunc("invalid_granularity", PrimitiveType.DATE);

        try {
            SyncPartitionUtils.transferRange(baseRange, invalidGranularityFunction);
            Assertions.fail("Expected SemanticException to be thrown");
        } catch (SemanticException e) {
            Assertions.assertTrue(e.getMessage().contains("Do not support in date_trunc format string"));
        }
    }

    private DateLiteral plusDay(DateLiteral dateLiteral, int diff) {
        try {
            LocalDateTime date = dateLiteral.toLocalDateTime().plusDays(diff);
            return new DateLiteral(date, Type.DATE);
        } catch (Exception e) {
            Assertions.fail();
            return null;
        }
    }

    @Test
    public void transferRangeHandlesMinValue() throws AnalysisException {
        final DateLiteral minValue = DateLiteral.createMinValue(Type.DATE);
        Range<PartitionKey> range = createRange(minValue, plusDay(minValue, 1));
        {
            Expr partitionExpr = new SlotRef(TABLE_NAME, "column");
            Range<PartitionKey> result = SyncPartitionUtils.transferRange(range, partitionExpr);
            Assertions.assertEquals(result, range);
        }
        {
            FunctionCallExpr partitionExpr = createDateTruncFunc("day", PrimitiveType.DATE);
            Range<PartitionKey> result = SyncPartitionUtils.transferRange(range, partitionExpr);
            Assertions.assertEquals(result, range);
        }
        {
            FunctionCallExpr partitionExpr = createDateTruncFunc("month", PrimitiveType.DATE);
            Range<PartitionKey> result = SyncPartitionUtils.transferRange(range, partitionExpr);
            Assertions.assertEquals("0000-01-01 00:00:00", result.lowerEndpoint().getKeys().get(0).getStringValue());
            Assertions.assertEquals("0000-02-01 00:00:00", result.upperEndpoint().getKeys().get(0).getStringValue());
        }
    }

    @Test
    public void transferRangeHandlesMaxValue() throws AnalysisException {
        final DateLiteral maxValue = DateLiteral.createMaxValue(Type.DATE);
        Range<PartitionKey> range = createRange(plusDay(maxValue, -1), maxValue);
        {
            Expr partitionExpr = new SlotRef(TABLE_NAME, "column");
            Range<PartitionKey> result = SyncPartitionUtils.transferRange(range, partitionExpr);
            Assertions.assertEquals(result, range);
        }
        {
            FunctionCallExpr partitionExpr = createDateTruncFunc("day", PrimitiveType.DATE);
            Range<PartitionKey> result = SyncPartitionUtils.transferRange(range, partitionExpr);
            Assertions.assertEquals(result, range);
        }
        {
            FunctionCallExpr partitionExpr = createDateTruncFunc("month", PrimitiveType.DATE);
            Range<PartitionKey> result = SyncPartitionUtils.transferRange(range, partitionExpr);
            Assertions.assertEquals("9999-12-01 00:00:00", result.lowerEndpoint().getKeys().get(0).getStringValue());
            Assertions.assertEquals("9999-12-31 00:00:00", result.upperEndpoint().getKeys().get(0).getStringValue());
        }
    }

    @Test
    public void transferRangeHandlesNullExpression() throws AnalysisException {
        Range<PartitionKey> baseRange = createRange("2020-01-01", "2020-02-01");
        Range<PartitionKey> result = SyncPartitionUtils.transferRange(baseRange, null);
        Assertions.assertEquals(baseRange, result);
    }

    @Test
    public void getIntersectedPartitionsHandlesNonOverlappingRanges() throws AnalysisException {
        Map<String, Range<PartitionKey>> srcRangeMap = Maps.newHashMap();
        srcRangeMap.put("p202001", createRange("2020-01-01", "2020-02-01"));
        Map<String, Range<PartitionKey>> dstRangeMap = Maps.newHashMap();
        dstRangeMap.put("p202002", createRange("2020-02-01", "2020-03-01"));
        Map<String, Set<String>> partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(srcRangeMap, dstRangeMap);
        Assertions.assertTrue(partitionRefMap.get("p202001").isEmpty());
    }

    @Test
    public void getIntersectedPartitionsHandlesIdenticalRanges() throws AnalysisException {
        Map<String, Range<PartitionKey>> srcRangeMap = Maps.newHashMap();
        srcRangeMap.put("p202001", createRange("2020-01-01", "2020-02-01"));

        Map<String, Range<PartitionKey>> dstRangeMap = Maps.newHashMap();
        dstRangeMap.put("p202001", createRange("2020-01-01", "2020-02-01"));

        Map<String, Set<String>> partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(srcRangeMap, dstRangeMap);

        Assertions.assertEquals(1, partitionRefMap.size());
        Assertions.assertTrue(partitionRefMap.get("p202001").contains("p202001"));
    }

    @Test
    public void transferRangeHandlesMaxValueRange() throws AnalysisException {
        final DateLiteral maxValue = DateLiteral.createMaxValue(Type.DATE);
        final Range<PartitionKey> maxValueRange = createRange(plusDay(maxValue, -1), maxValue);
        FunctionCallExpr dateTruncFunction = createDateTruncFunc("year", PrimitiveType.DATE);
        Range<PartitionKey> result = SyncPartitionUtils.transferRange(maxValueRange, dateTruncFunction);

        Assertions.assertEquals("9999-01-01 00:00:00",
                result.lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("9999-12-31 00:00:00",
                result.upperEndpoint().getKeys().get(0).getStringValue());
    }

    // Helper method to create PRangeCell from date strings
    private PRangeCell createPRangeCell(String lowerBound, String upperBound) throws AnalysisException {
        Range<PartitionKey> range = createRange(lowerBound, upperBound);
        return new PRangeCell(range);
    }

    @Test
    public void testIsManyToManyPartitionRangeMapping() throws AnalysisException {
        // Test case 1: Empty destination ranges
        PRangeCell srcRange = createPRangeCell("2023-07-27", "2023-07-30");
        List<PRangeCell> emptyDstRanges = Lists.newArrayList();
        boolean result = SyncPartitionUtils.isManyToManyPartitionRangeMapping(srcRange, emptyDstRanges);
        Assertions.assertFalse(result, "Empty destination ranges should return false");

        // Test case 2: Single destination range with intersection
        List<PRangeCell> singleDstRanges = Lists.newArrayList(
                createPRangeCell("2023-07-01", "2023-08-01")
        );
        result = SyncPartitionUtils.isManyToManyPartitionRangeMapping(srcRange, singleDstRanges);
        Assertions.assertTrue(result, "Single intersecting range should return true");

        // Test case 3: Single destination range without intersection
        List<PRangeCell> nonIntersectingDstRanges = Lists.newArrayList(
                createPRangeCell("2023-08-01", "2023-09-01")
        );
        result = SyncPartitionUtils.isManyToManyPartitionRangeMapping(srcRange, nonIntersectingDstRanges);
        Assertions.assertFalse(result, "Non-intersecting single range should return false");

        // Test case 4: Multiple destination ranges with intersections (many-to-many scenario)
        List<PRangeCell> multiDstRanges = Lists.newArrayList(
                createPRangeCell("2023-07-01", "2023-07-28"),  // intersects
                createPRangeCell("2023-07-29", "2023-08-01"),  // intersects
                createPRangeCell("2023-08-01", "2023-09-01")   // doesn't intersect
        );
        result = SyncPartitionUtils.isManyToManyPartitionRangeMapping(srcRange, multiDstRanges);
        Assertions.assertTrue(result, "Multiple intersecting ranges should return true");

        // Test case 5: Adjacent ranges (edge case)
        PRangeCell adjacentSrcRange = createPRangeCell("2023-07-30", "2023-08-02");
        List<PRangeCell> adjacentDstRanges = Lists.newArrayList(
                createPRangeCell("2023-07-01", "2023-07-30"),  // adjacent but not intersecting
                createPRangeCell("2023-08-02", "2023-09-01")   // adjacent but not intersecting
        );
        result = SyncPartitionUtils.isManyToManyPartitionRangeMapping(adjacentSrcRange, adjacentDstRanges);
        Assertions.assertFalse(result, "Adjacent non-intersecting ranges should return false");

        // Test case 6: Exact overlap with one range
        PRangeCell exactSrcRange = createPRangeCell("2023-07-01", "2023-08-01");
        List<PRangeCell> exactDstRanges = Lists.newArrayList(
                createPRangeCell("2023-07-01", "2023-08-01")
        );
        result = SyncPartitionUtils.isManyToManyPartitionRangeMapping(exactSrcRange, exactDstRanges);
        Assertions.assertFalse(result, "Exact overlap should return true");
    }

    private static Map<Table, PCellSortedSet> buildChangedPartitionMap(Table mvTable, Map<String, PCell> mvPartitionMap,
                                                                       Set<String> changedPartitions) {
        Map<Table, PCellSortedSet> mvPartitionToCells = Maps.newHashMap();
        Map<String, PCell> changedPCells = Maps.newHashMap();
        for (String partName : changedPartitions) {
            PCell pCell = mvPartitionMap.get(partName);
            if (pCell != null) {
                changedPCells.put(partName, pCell);
            }
        }
        mvPartitionToCells.put(mvTable, PCellSortedSet.of(changedPCells));
        return mvPartitionToCells;
    }

    @Test
    public void testIsCalcPotentialRefreshPartition() throws AnalysisException {
        // Test case 1: One-to-One mapping (should return false)
        Table mockTable1 = new OlapTable();
        Map<String, PCell> basePartitionMap = Maps.newHashMap();
        basePartitionMap.put("p1", createPRangeCell("2023-07-27", "2023-07-28"));
        basePartitionMap.put("p2", createPRangeCell("2023-07-28", "2023-07-29"));

        Map<String, PCell> mvPartitionMap = Maps.newHashMap();
        mvPartitionMap.put("mv_p1", createPRangeCell("2023-07-27", "2023-07-28"));
        mvPartitionMap.put("mv_p2", createPRangeCell("2023-07-28", "2023-07-29"));

        Map<Table, PCellSortedSet> baseChangedPartitions =
                buildChangedPartitionMap(mockTable1, basePartitionMap, Set.of("p1"));

        boolean result = SyncPartitionUtils.isCalcPotentialRefreshPartition(
                baseChangedPartitions, PCellSortedSet.of(mvPartitionMap));
        Assertions.assertFalse(result, "One-to-one mapping should return false");

        // Test case 2: Many-to-One mapping (should return false)
        Map<String, PCell> manyToOneBasePartitionMap = Maps.newHashMap();
        manyToOneBasePartitionMap.put("p1", createPRangeCell("2023-07-01", "2023-07-15"));
        manyToOneBasePartitionMap.put("p2", createPRangeCell("2023-07-15", "2023-07-31"));

        Map<String, PCell> manyToOneMvPartitionMap = Maps.newHashMap();
        manyToOneMvPartitionMap.put("mv_p1", createPRangeCell("2023-07-01", "2023-08-01"));

        Map<Table, Map<String, PCell>> manyToOneRefBaseTablePartitionToCells = Maps.newHashMap();
        manyToOneRefBaseTablePartitionToCells.put(mockTable1, manyToOneBasePartitionMap);
        Map<Table, PCellSortedSet> manyToOneBaseChangedPartitions =
                buildChangedPartitionMap(mockTable1, manyToOneBasePartitionMap, Set.of("p1"));

        result = SyncPartitionUtils.isCalcPotentialRefreshPartition(
                manyToOneBaseChangedPartitions, PCellSortedSet.of(manyToOneMvPartitionMap));
        Assertions.assertTrue(result, "Many-to-one mapping should return true");

        // Test case 3: Many-to-Many mapping (should return true)
        // Base table partitions: 3-day intervals
        // MV partitions: monthly intervals
        Map<String, PCell> manyToManyBasePartitionMap = Maps.newHashMap();
        manyToManyBasePartitionMap.put("p1", createPRangeCell("2023-07-27", "2023-07-30"));
        manyToManyBasePartitionMap.put("p2", createPRangeCell("2023-07-30", "2023-08-02"));
        manyToManyBasePartitionMap.put("p3", createPRangeCell("2023-08-02", "2023-08-05"));

        Map<String, PCell> manyToManyMvPartitionMap = Maps.newHashMap();
        manyToManyMvPartitionMap.put("mv_p1", createPRangeCell("2023-07-01", "2023-08-01"));
        manyToManyMvPartitionMap.put("mv_p2", createPRangeCell("2023-08-01", "2023-09-01"));

        Map<Table, PCellSortedSet> manyToManyBaseChangedPartitions =
                buildChangedPartitionMap(mockTable1, manyToManyBasePartitionMap, Set.of("p2"));

        result = SyncPartitionUtils.isCalcPotentialRefreshPartition(
                manyToManyBaseChangedPartitions, PCellSortedSet.of(manyToManyMvPartitionMap));
        Assertions.assertTrue(result, "Many-to-many mapping should return true");

        // Test case 4: Empty base changed partitions (should return false)
        Map<Table, PCellSortedSet> emptyBaseChangedPartitions = buildChangedPartitionMap(
                mockTable1, mvPartitionMap, Set.of());
        result = SyncPartitionUtils.isCalcPotentialRefreshPartition(
                emptyBaseChangedPartitions, PCellSortedSet.of(mvPartitionMap));
        Assertions.assertFalse(result, "Empty base changed partitions should return false");
    }
}
