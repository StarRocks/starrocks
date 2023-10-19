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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.ast.PartitionValue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SyncPartitionUtilsTest {

    private static final TableName TABLE_NAME = new TableName("db1", "table1");
    private static SlotRef slotRef;
    private static Column partitionColumn;

    @BeforeClass
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

        Assert.assertTrue(partitionRefMap.get("p20201015_20201115").contains("p202010_202011"));
        Assert.assertTrue(partitionRefMap.get("p20201015_20201115").contains("p202011_202012"));
        Assert.assertTrue(partitionRefMap.get("p20201115_20201215").contains("p202011_202012"));
        Assert.assertTrue(partitionRefMap.get("p20201115_20201215").contains("p202012_202101"));

        partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(dstRangeMap, srcRangeMap);

        Assert.assertTrue(partitionRefMap.get("p202010_202011").contains("p20201015_20201115"));
        Assert.assertTrue(partitionRefMap.get("p202011_202012").contains("p20201015_20201115"));
        Assert.assertTrue(partitionRefMap.get("p202011_202012").contains("p20201115_20201215"));
        Assert.assertTrue(partitionRefMap.get("p202012_202101").contains("p20201115_20201215"));

        // test border
        srcRangeMap = Maps.newHashMap();
        srcRangeMap.put("p20201015", createRange("2020-10-15", "2020-11-01"));

        dstRangeMap = Maps.newHashMap();
        dstRangeMap.put("p202011_202012", createRange("2020-11-01", "2020-12-01"));

        partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(srcRangeMap, dstRangeMap);
        Assert.assertEquals(0, partitionRefMap.get("p20201015").size());

        partitionRefMap = SyncPartitionUtils.getIntersectedPartitions(dstRangeMap, srcRangeMap);
        Assert.assertEquals(0, partitionRefMap.get("p202011_202012").size());

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

        Assert.assertEquals(1, partitionRefMap.get("p202010_202011").size());
        Assert.assertEquals(1, partitionRefMap.get("p202011_202012").size());
        Assert.assertEquals(1, partitionRefMap.get("p202012_202101").size());

        Assert.assertTrue(partitionRefMap.get("p202010_202011").contains("p202010_202011"));
        Assert.assertTrue(partitionRefMap.get("p202011_202012").contains("p202011_202012"));
        Assert.assertTrue(partitionRefMap.get("p202012_202101").contains("p202012_202101"));
    }

    @Test
    public void testDiffRange() throws AnalysisException {

        // normal condition
        Map<String, Range<PartitionKey>> srcRange = Maps.newHashMap();
        srcRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        srcRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));

        Map<String, Range<PartitionKey>> dstRange = Maps.newHashMap();
        dstRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));

        Map<String, Range<PartitionKey>> diff = PartitionDiffer.diffRange(srcRange, dstRange);
        Assert.assertEquals(1, diff.size());
        Assert.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-03 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());

        diff = PartitionDiffer.diffRange(dstRange, srcRange);
        Assert.assertEquals(0, diff.size());

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

        diff = PartitionDiffer.diffRange(srcRange, dstRange);
        Assert.assertEquals(2, diff.size());
        Assert.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-03 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-05 00:00:00",
                diff.get("p20200105").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-06 00:00:00",
                diff.get("p20200105").upperEndpoint().getKeys().get(0).getStringValue());

        diff = PartitionDiffer.diffRange(dstRange, srcRange);
        Assert.assertEquals(1, diff.size());
        Assert.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-06 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
    }

    @Test
    public void testDiffList() {
        // same
        Map<String, List<List<String>>> baseListMap = Maps.newHashMap();
        baseListMap.put("p20230619", Collections.singletonList(Lists.newArrayList("2023-06-19")));
        baseListMap.put("p20230620", Collections.singletonList(Lists.newArrayList("2023-06-20")));
        baseListMap.put("p20230621", Collections.singletonList(Lists.newArrayList("2023-06-21")));

        Map<String, List<List<String>>> mvListMap = Maps.newHashMap();
        mvListMap.put("p20230619", Collections.singletonList(Lists.newArrayList("2023-06-19")));
        mvListMap.put("p20230621", Collections.singletonList(Lists.newArrayList("2023-06-21")));
        mvListMap.put("p20230620", Collections.singletonList(Lists.newArrayList("2023-06-20")));

        Map<String, List<List<String>>> diff = SyncPartitionUtils.diffList(baseListMap, mvListMap);
        Assert.assertEquals(0, diff.size());

        baseListMap = Maps.newHashMap();
        baseListMap.put("p20230619", Collections.singletonList(Lists.newArrayList("2023-06-19")));
        baseListMap.put("p20230620", Collections.singletonList(Lists.newArrayList("2023-06-20")));

        mvListMap = Maps.newHashMap();
        mvListMap.put("p20230619", Collections.singletonList(Lists.newArrayList("2023-06-19")));

        diff = SyncPartitionUtils.diffList(baseListMap, mvListMap);
        Assert.assertEquals(1, diff.size());
        Assert.assertEquals("2023-06-20", diff.get("p20230620").get(0).get(0));

        baseListMap = Maps.newHashMap();
        baseListMap.put("p20230619", Collections.singletonList(Lists.newArrayList("2023-06-19")));

        mvListMap = Maps.newHashMap();
        mvListMap.put("p20230619", Collections.singletonList(Lists.newArrayList("2023-06-19")));
        mvListMap.put("p20230620", Collections.singletonList(Lists.newArrayList("2023-06-20")));

        diff = SyncPartitionUtils.diffList(baseListMap, mvListMap);
        Assert.assertEquals(0, diff.size());
    }

    @Test
    public void testCalcSyncSameRangePartition() throws AnalysisException {

        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        baseRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        baseRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));
        baseRange.put("p20200131", createRange("2020-01-31", "2020-02-01"));

        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();
        mvRange.put("p202001", createRange("2020-01-01", "2020-02-01"));

        RangePartitionDiff diff = SyncPartitionUtils.getRangePartitionDiffOfSlotRef(baseRange, mvRange);

        Map<String, Range<PartitionKey>> adds = diff.getAdds();
        Assert.assertEquals(3, adds.size());
        Assert.assertEquals("2020-01-01 00:00:00",
                adds.get("p20200101").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-02 00:00:00",
                adds.get("p20200101").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-02 00:00:00",
                adds.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-03 00:00:00",
                adds.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-31 00:00:00",
                adds.get("p20200131").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-02-01 00:00:00",
                adds.get("p20200131").upperEndpoint().getKeys().get(0).getStringValue());

        Map<String, Range<PartitionKey>> deletes = diff.getDeletes();
        Assert.assertEquals(1, deletes.size());
        Assert.assertEquals("2020-01-01 00:00:00",
                deletes.get("p202001").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-02-01 00:00:00",
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

        diff = SyncPartitionUtils.getRangePartitionDiffOfSlotRef(baseRange, mvRange);

        adds = diff.getAdds();
        deletes = diff.getDeletes();

        Assert.assertEquals(1, adds.size());
        Assert.assertEquals("2020-01-04 00:00:00",
                adds.get("p20200104").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-05 00:00:00",
                adds.get("p20200104").upperEndpoint().getKeys().get(0).getStringValue());

        Assert.assertEquals(1, deletes.size());
        Assert.assertEquals("2020-01-01 00:00:00",
                deletes.get("p20200101").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-02 00:00:00",
                deletes.get("p20200101").upperEndpoint().getKeys().get(0).getStringValue());
    }

    @Test
    public void testMappingRangeRollup() throws AnalysisException {
        // minute
        Range<PartitionKey> baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        PartitionMapping mappedRange = SyncPartitionUtils.mappingRange(baseRange, "minute");

        Assert.assertEquals("2020-05-03T12:34:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-06-04T12:35:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // hour
        baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "hour");

        Assert.assertEquals("2020-05-03T12:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-06-04T13:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // day
        baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "day");

        Assert.assertEquals("2020-05-03T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-06-05T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // month
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "month");

        Assert.assertEquals("2020-05-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-07-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // quarter
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "quarter");

        Assert.assertEquals("2020-04-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-07-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // year
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "year");

        Assert.assertEquals("2020-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2021-01-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testMappingRangeRollupWithMaxValue() throws AnalysisException {
        String maxValueDate =
                new DateLiteral(Type.DATE, true).toLocalDateTime().format(DateTimeFormatter.ISO_DATE_TIME);
        // minute
        Range<PartitionKey> baseRange = createMaxValueRange("2020-05-03 12:34:56");
        PartitionMapping mappedRange = SyncPartitionUtils.mappingRange(baseRange, "minute");

        Assert.assertEquals("2020-05-03T12:34:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // hour
        baseRange = createMaxValueRange("2020-05-03 12:34:56");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "hour");

        Assert.assertEquals("2020-05-03T12:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // day
        baseRange = createMaxValueRange("2020-05-03 12:34:56");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "day");

        Assert.assertEquals("2020-05-03T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // month
        baseRange = createMaxValueRange("2020-05-03");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "month");

        Assert.assertEquals("2020-05-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // quarter
        baseRange = createMaxValueRange("2020-05-03");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "quarter");

        Assert.assertEquals("2020-04-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // year
        baseRange = createMaxValueRange("2020-05-03");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "year");

        Assert.assertEquals("2020-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(maxValueDate, mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testMappingRangeList() throws AnalysisException {
        Map<String, Range<PartitionKey>> baseRangeMap = Maps.newHashMap();
        Map<String, Range<PartitionKey>> result;
        baseRangeMap.put("p202001", createRange("2020-01-01", "2020-02-01"));
        baseRangeMap.put("p202002", createRange("2020-02-01", "2020-03-01"));
        baseRangeMap.put("p202003", createRange("2020-03-01", "2020-04-01"));
        baseRangeMap.put("p202004", createMaxValueRange("2020-04-01"));

        result = SyncPartitionUtils.mappingRangeList(baseRangeMap, "month", PrimitiveType.DATE);

        Assert.assertTrue(result.containsKey("p202004_999912"));
        Assert.assertEquals(1, result.get("p202004_999912").upperEndpoint().getKeys().size());
        Assert.assertEquals("9999-12-31", result.get("p202004_999912").upperEndpoint().getKeys().get(0).
                getStringValue());

        baseRangeMap.clear();
        baseRangeMap.put("p202001", createRange("2020-01-01 12:00:25", "2020-02-01 20:01:59"));
        baseRangeMap.put("p202002", createRange("2020-02-01 20:01:59", "2020-03-01 02:50:49"));
        baseRangeMap.put("p202003", createRange("2020-03-01 02:50:49", "2020-04-01 01:05:06"));
        baseRangeMap.put("p202004", createMaxValueRange("2020-04-01 01:05:06"));
        result = SyncPartitionUtils.mappingRangeList(baseRangeMap, "hour", PrimitiveType.DATETIME);
        Assert.assertTrue(result.containsKey("p2020040102_9999123100"));
        Assert.assertEquals(1, result.get("p2020040102_9999123100").upperEndpoint().getKeys().size());
        Assert.assertEquals("9999-12-31 00:00:00", result.get("p2020040102_9999123100").upperEndpoint().getKeys().get(0).
                getStringValue());
    }

    @Test
    public void testMappingRange() throws AnalysisException {

        // less than
        Range<PartitionKey> baseRange = createLessThanRange("2020-05-03");
        PartitionMapping mappedRange = SyncPartitionUtils.mappingRange(baseRange, "day");

        Assert.assertEquals("0000-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-05-03T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // big partition
        baseRange = createRange("2020-01-01", "2020-02-01");
        mappedRange = SyncPartitionUtils.mappingRange(baseRange, "day");

        Assert.assertEquals("2020-01-01T00:00:00",
                mappedRange.getLowerDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-02-01T00:00:00",
                mappedRange.getUpperDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testCalcSyncRollupSpecial() throws AnalysisException {

        // less than
        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        Range<PartitionKey> basePartition = createLessThanRange("2020-05-03");
        baseRange.put("p1", basePartition);
        baseRange.put("p2", createRange("2020-05-04", "2020-11-12"));

        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();
        RangePartitionDiff diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                "month", PrimitiveType.DATETIME);
        Map<String, Range<PartitionKey>> adds = diff.getAdds();
        Map<String, Range<PartitionKey>> deletes = diff.getDeletes();

        Assert.assertEquals(3, adds.size());
        Assert.assertEquals("0000-01-01 00:00:00",
                adds.get("p000101_202005").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-05-01 00:00:00",
                adds.get("p000101_202005").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-06-01 00:00:00",
                adds.get("p202006_202012").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-12-01 00:00:00",
                adds.get("p202006_202012").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-05-01 00:00:00",
                adds.get("p202005_202006").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-06-01 00:00:00",
                adds.get("p202005_202006").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals(0, deletes.size());

        // big partition
        baseRange = Maps.newHashMap();
        baseRange.put("p202001", createRange("2020-01-01", "2020-02-01"));

        mvRange = Maps.newHashMap();
        mvRange.put("p20200101_20200102", createRange("2020-01-01", "2020-01-02"));
        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                "day", PrimitiveType.DATETIME);
        adds = diff.getAdds();
        deletes = diff.getDeletes();

        System.out.println(adds);
        System.out.println(deletes);

    }

    @Test
    public void testCalcSyncRollupPartition() throws AnalysisException {
        // overlap scenario
        String granularity = "month";

        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        baseRange.put("p1", createRange("2020-09-12", "2020-10-12"));
        baseRange.put("p2", createRange("2020-10-12", "2020-11-12"));

        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();
        RangePartitionDiff diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                granularity, PrimitiveType.DATETIME);

        Map<String, Range<PartitionKey>> adds = diff.getAdds();
        Map<String, Range<PartitionKey>> deletes = diff.getDeletes();

        Assert.assertEquals(3, adds.size());
        Assert.assertEquals("2020-09-01 00:00:00",
                adds.get("p202009_202010").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-10-01 00:00:00",
                adds.get("p202009_202010").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-10-01 00:00:00",
                adds.get("p202010_202011").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-11-01 00:00:00",
                adds.get("p202010_202011").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-11-01 00:00:00",
                adds.get("p202011_202012").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-12-01 00:00:00",
                adds.get("p202011_202012").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals(0, deletes.size());

        // bigger than granularity scenario
        baseRange = Maps.newHashMap();
        baseRange.put("p20200101", createRange("2020-01-01", "2021-01-01"));
        mvRange = Maps.newHashMap();
        mvRange.put("p202001_202002", createRange("2020-01-01", "2020-02-01"));

        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                granularity, PrimitiveType.DATETIME);
        adds = diff.getAdds();
        deletes = diff.getDeletes();
        Assert.assertEquals(1, adds.size());
        Assert.assertEquals("2020-01-01 00:00:00",
                adds.get("p202001_202101").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2021-01-01 00:00:00",
                adds.get("p202001_202101").upperEndpoint().getKeys().get(0).getStringValue());

        Assert.assertEquals(1, deletes.size());
        Assert.assertEquals("2020-01-01 00:00:00",
                deletes.get("p202001_202002").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-02-01 00:00:00",
                deletes.get("p202001_202002").upperEndpoint().getKeys().get(0).getStringValue());

        baseRange = Maps.newHashMap();
        baseRange.put("p20200503", createRange("2020-05-03", "2020-06-05"));
        mvRange = Maps.newHashMap();
        mvRange.put("p202005_202006", createRange("2020-05-01", "2020-06-01"));
        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                "month", PrimitiveType.DATETIME);
        adds = diff.getAdds();
        deletes = diff.getDeletes();
        Assert.assertEquals(1, adds.size());
        Assert.assertEquals("2020-05-01 00:00:00",
                adds.get("p202005_202007").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-07-01 00:00:00",
                adds.get("p202005_202007").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals(1, deletes.size());
        Assert.assertEquals("2020-05-01 00:00:00",
                deletes.get("p202005_202006").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-06-01 00:00:00",
                deletes.get("p202005_202006").upperEndpoint().getKeys().get(0).getStringValue());

        baseRange = Maps.newHashMap();
        baseRange.put("p20200403", createRange("2020-04-03", "2020-05-02"));
        baseRange.put("p20200503", createRange("2020-05-03", "2020-06-05"));
        mvRange = Maps.newHashMap();
        mvRange.put("p202005_202006", createRange("2020-05-01", "2020-06-01"));
        diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(baseRange, mvRange,
                "month", PrimitiveType.DATETIME);
        adds = diff.getAdds();
        deletes = diff.getDeletes();
        Assert.assertEquals(2, adds.size());
        Assert.assertEquals("2020-04-01 00:00:00",
                adds.get("p202004_202005").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-05-01 00:00:00",
                adds.get("p202004_202005").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-06-01 00:00:00",
                adds.get("p202006_202007").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-07-01 00:00:00",
                adds.get("p202006_202007").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals(0, deletes.size());
    }

    private PartitionRange buildPartitionRange(String name, String start, String end) throws AnalysisException {
        return new PartitionRange(name,
                Range.closedOpen(
                        PartitionKey.ofDateTime(DateUtils.parseStrictDateTime(start)),
                        PartitionKey.ofDateTime(DateUtils.parseStrictDateTime(end))));
    }

    @Test
    public void test_getIntersectedPartitions() throws AnalysisException {
        List<PartitionRange> srcs = Arrays.asList(
                buildPartitionRange("p20230801", "00000101", "20230801"),
                buildPartitionRange("p20230802", "20230801", "20230802"),
                buildPartitionRange("p20230803", "20230802", "20230803")
        );
        List<PartitionRange> dsts = Arrays.asList(
                buildPartitionRange("p000101_202308", "0001-01-01", "2023-08-01"),
                buildPartitionRange("p202308_202309", "2023-08-01", "2023-09-01")
        );
        Map<String, Set<String>> res = SyncPartitionUtils.getIntersectedPartitions(srcs, dsts);
        Assert.assertEquals(
                ImmutableMap.of(
                        "p20230801", ImmutableSet.of("p000101_202308"),
                        "p20230802", ImmutableSet.of("p202308_202309"),
                        "p20230803", ImmutableSet.of("p202308_202309")
                ),
                res);
    }

}
