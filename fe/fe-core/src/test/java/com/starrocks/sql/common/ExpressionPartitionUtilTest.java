// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExpressionPartitionUtilTest {

    private static final TableName TABLE_NAME = new TableName("db1", "table1");
    private static SlotRef slotRef;
    private static Column partitionColumn;

    @BeforeClass
    public static void beforeClass() throws Exception {
        slotRef = new SlotRef(TABLE_NAME, "k1");
        partitionColumn = new Column("k1", ScalarType.DATETIME);
    }

    private static Range<PartitionKey> createRange(String lowerBound, String upperBound) throws AnalysisException {
        PartitionValue lowerValue = new PartitionValue(lowerBound);
        PartitionValue upperValue = new PartitionValue(upperBound);
        PartitionKey lowerBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue),
                Collections.singletonList(partitionColumn));
        PartitionKey upperBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                Collections.singletonList(partitionColumn));
        return Range.closedOpen(lowerBoundPartitionKey, upperBoundPartitionKey);
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
    public void testDiffRange() throws AnalysisException {

        // normal condition
        Map<String, Range<PartitionKey>> srcRange = Maps.newHashMap();
        srcRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        srcRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));

        Map<String, Range<PartitionKey>> dstRange = Maps.newHashMap();
        dstRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));

        Map<String, Range<PartitionKey>> diff = ExpressionPartitionUtil.diffRange(srcRange, dstRange);
        Assert.assertEquals(1, diff.size());
        Assert.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-03 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());

        diff = ExpressionPartitionUtil.diffRange(dstRange, srcRange);
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

        diff = ExpressionPartitionUtil.diffRange(srcRange, dstRange);
        Assert.assertEquals(2, diff.size());
        Assert.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-03 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-05 00:00:00",
                diff.get("p20200105").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-06 00:00:00",
                diff.get("p20200105").upperEndpoint().getKeys().get(0).getStringValue());

        diff = ExpressionPartitionUtil.diffRange(dstRange, srcRange);
        Assert.assertEquals(1, diff.size());
        Assert.assertEquals("2020-01-02 00:00:00",
                diff.get("p20200102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-01-06 00:00:00",
                diff.get("p20200102").upperEndpoint().getKeys().get(0).getStringValue());
    }

    @Test
    public void testCalcSyncSamePartition() throws AnalysisException {

        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        baseRange.put("p20200101", createRange("2020-01-01", "2020-01-02"));
        baseRange.put("p20200102", createRange("2020-01-02", "2020-01-03"));
        baseRange.put("p20200131", createRange("2020-01-31", "2020-02-01"));

        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();
        mvRange.put("p202001", createRange("2020-01-01", "2020-02-01"));

        PartitionDiff diff = ExpressionPartitionUtil.calcSyncSamePartition(baseRange, mvRange);

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


        diff = ExpressionPartitionUtil.calcSyncSamePartition(baseRange, mvRange);

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
        PartitionMapping mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "minute");

        Assert.assertEquals("2020-05-03T12:34:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-06-04T12:35:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // hour
        baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "hour");

        Assert.assertEquals("2020-05-03T12:00:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-06-04T13:00:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // day
        baseRange = createRange("2020-05-03 12:34:56", "2020-06-04 12:34:56");
        mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "day");

        Assert.assertEquals("2020-05-03T00:00:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-06-05T00:00:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // month
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "month");

        Assert.assertEquals("2020-05-01T00:00:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-07-01T00:00:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // quarter
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "quarter");

        Assert.assertEquals("2020-04-01T00:00:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-07-01T00:00:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // year
        baseRange = createRange("2020-05-03", "2020-06-04");
        mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "year");

        Assert.assertEquals("2020-01-01T00:00:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2021-01-01T00:00:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testMappingRange() throws AnalysisException {

        // less than
        Range<PartitionKey> baseRange = createLessThanRange("2020-05-03");
        PartitionMapping mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "day");


        Assert.assertEquals("0000-01-01T00:00:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-05-04T00:00:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));

        // big partition
        baseRange = createRange("2020-01-01", "2020-02-01");
        mappedRange = ExpressionPartitionUtil.mappingRange(baseRange, "day");

        Assert.assertEquals("2020-01-01T00:00:00",
                mappedRange.getFirstTime().format(DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals("2020-02-02T00:00:00",
                mappedRange.getLastTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testCalcSyncRollupPartition() throws AnalysisException {
        // overlap scenario
        String granularity = "month";

        Map<String, Range<PartitionKey>> baseRange = Maps.newHashMap();
        baseRange.put("p1", createRange("2020-09-12", "2020-10-12"));
        baseRange.put("p2", createRange("2020-10-12", "2020-11-12"));

        Map<String, Range<PartitionKey>> mvRange = Maps.newHashMap();
        PartitionDiff diff = ExpressionPartitionUtil.calcSyncRollupPartition(baseRange, mvRange, granularity);

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

        diff = ExpressionPartitionUtil.calcSyncRollupPartition(baseRange, mvRange, granularity);
        adds = diff.getAdds();
        deletes = diff.getDeletes();
        Assert.assertEquals(1, adds.size());
        Assert.assertEquals("2020-01-01 00:00:00",
                adds.get("p202001_202102").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2021-02-01 00:00:00",
                adds.get("p202001_202102").upperEndpoint().getKeys().get(0).getStringValue());

        Assert.assertEquals(1, deletes.size());
        Assert.assertEquals("2020-01-01 00:00:00",
                deletes.get("p202001_202002").lowerEndpoint().getKeys().get(0).getStringValue());
        Assert.assertEquals("2020-02-01 00:00:00",
                deletes.get("p202001_202002").upperEndpoint().getKeys().get(0).getStringValue());

        baseRange = Maps.newHashMap();
        baseRange.put("p20200503", createRange("2020-05-03", "2020-06-05"));
        mvRange = Maps.newHashMap();
        mvRange.put("p202005_202006", createRange("2020-05-01", "2020-06-01"));
        diff = ExpressionPartitionUtil.calcSyncRollupPartition(baseRange, mvRange, "month");
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
        diff = ExpressionPartitionUtil.calcSyncRollupPartition(baseRange, mvRange, "month");
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

}
