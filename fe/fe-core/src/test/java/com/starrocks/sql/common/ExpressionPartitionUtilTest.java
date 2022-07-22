// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;

import com.clearspring.analytics.util.Lists;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExpressionPartitionUtilTest {

    private static final TableName TABLE_NAME = new TableName("db1", "table1");

    private static SlotRef slotRef;

    private static Column partitionColumn;

    private static String lowerBound;

    private static String upperBound;

    private static FunctionCallExpr quarterFunctionCallExpr;

    private static FunctionCallExpr yearFunctionCallExpr;

    private static Range<PartitionKey> basePartitionKeyRange;

    @BeforeClass
    public static void beforeClass() throws Exception {
        slotRef = new SlotRef(TABLE_NAME, "k1");
        StringLiteral quarterStringLiteral = new StringLiteral("quarter");
        quarterFunctionCallExpr =
                new FunctionCallExpr("date_trunc", Arrays.asList(quarterStringLiteral, slotRef));
        StringLiteral yearStringLiteral = new StringLiteral("year");
        yearFunctionCallExpr =
                new FunctionCallExpr("date_trunc", Arrays.asList(yearStringLiteral, slotRef));

        partitionColumn = new Column("k1", ScalarType.DATETIME);
        lowerBound = "2020-04-21 20:43:00";
        upperBound = "2021-04-21 20:43:00";
        basePartitionKeyRange = getRange(lowerBound, upperBound);
    }

    @Test
    public void testGetPartitionKeyRange() throws AnalysisException {
        List<Range<PartitionKey>> existPartitionKeyRanges = Lists.newArrayList();
        // slot ref test
        Range<PartitionKey> partitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(slotRef, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(partitionKeyRange != null);
        Assert.assertEquals(partitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue(), lowerBound);
        Assert.assertEquals(partitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue(), upperBound);
        // date_trunc quarter test
        Range<PartitionKey> quarterPartitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(quarterFunctionCallExpr, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(quarterPartitionKeyRange != null);
        Assert.assertEquals(quarterPartitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue(),
                "2020-04-01 00:00:00");
        Assert.assertEquals(quarterPartitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue(),
                "2021-07-01 00:00:00");
        //date_trunc year test
        Range<PartitionKey> yearPartitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(yearFunctionCallExpr, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(yearPartitionKeyRange != null);
        Assert.assertEquals(yearPartitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue(),
                "2020-01-01 00:00:00");
        Assert.assertEquals(yearPartitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue(),
                "2022-01-01 00:00:00");

        // date_trunc quarter test with exist ranges
        existPartitionKeyRanges = Lists.newArrayList();
        existPartitionKeyRanges.add(getRange("2020-01-01 00:00:00", "2020-07-01 00:00:00"));
        existPartitionKeyRanges.add(getRange("2021-04-01 00:00:00", "2021-07-01 00:00:00"));
        quarterPartitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(quarterFunctionCallExpr, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(quarterPartitionKeyRange != null);
        Assert.assertEquals(quarterPartitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue(),
                "2020-07-01 00:00:00");
        Assert.assertEquals(quarterPartitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue(),
                "2021-04-01 00:00:00");
        // date_trunc year test with exist ranges
        existPartitionKeyRanges = Lists.newArrayList();
        existPartitionKeyRanges.add(getRange("2020-01-01 00:00:00", "2021-01-01 00:00:00"));
        existPartitionKeyRanges.add(getRange("2021-01-01 00:00:00", "2022-01-01 00:00:00"));
        yearPartitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(yearFunctionCallExpr, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(yearPartitionKeyRange == null);
    }

    @Test
    public void testGetPartitionKeyRangeWithExist() throws AnalysisException {
        List<Range<PartitionKey>> existPartitionKeyRanges = Lists.newArrayList();
        existPartitionKeyRanges.add(getRange("2021-04-01","2022-04-01"));
        existPartitionKeyRanges.add(getRange("2021-01-01","2021-04-01"));
        existPartitionKeyRanges.add(getRange("2022-04-01","2022-07-01"));

        Range<PartitionKey> basePartitionKeyRange = getRange("2021-01-15", "2022-01-15");

        Range<PartitionKey> quarterPartitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(quarterFunctionCallExpr, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(quarterPartitionKeyRange == null);
    }

    private static Range<PartitionKey> getRange(String lowerBound, String  upperBound) throws AnalysisException {
        PartitionKey lowerBoundPartitionKey =
                PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue(lowerBound)),
                        Collections.singletonList(partitionColumn));
        PartitionKey upperBoundPartitionKey =
                PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue(upperBound)),
                Collections.singletonList(partitionColumn));
        return Range.closedOpen(lowerBoundPartitionKey, upperBoundPartitionKey);
    }

    @Test
    public void testGetPartitionKeyRangeHasNoSupportExpr() {
        List<Range<PartitionKey>> existPartitionKeyRanges = Lists.newArrayList();
        StringLiteral stringLiteral = new StringLiteral("year");
        try {
            ExpressionPartitionUtil.getPartitionKeyRange(stringLiteral, partitionColumn,
                    existPartitionKeyRanges, basePartitionKeyRange, 0);
        } catch (Exception e) {
            Assert.assertEquals("Do not support expr:'year'", e.getMessage());
        }

    }

    @Test
    public void testGetPartitionKeyRangesHasNoSupportFunction() {
        List<Range<PartitionKey>> existPartitionKeyRanges = Lists.newArrayList();
        StringLiteral stringLiteral = new StringLiteral("yyyy-MM-dd");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_format", Arrays.asList(slotRef, stringLiteral));
        try {
            ExpressionPartitionUtil.getPartitionKeyRange(functionCallExpr, partitionColumn,
                    existPartitionKeyRanges, basePartitionKeyRange, 0);
        } catch (Exception e) {
            Assert.assertEquals("Do not support function:date_format(`db1`.`table1`.`k1`, 'yyyy-MM-dd')",
                    e.getMessage());
        }
    }

    @Test
    public void testGetBound() throws AnalysisException {
        LiteralExpr dateLiteralExpr = DateLiteral.create("2020-04-21", Type.DATE);
        LiteralExpr dateTimeLiteralExpr = DateLiteral.create("2020-04-21 20:43:00", Type.DATETIME);

        LiteralExpr minDateLiteralExpr= DateLiteral.createMinValue(Type.DATE);
        LiteralExpr minDatetimeLiteralExpr = DateLiteral.createMinValue(Type.DATETIME);

        // date_trunc quarter test
        Assert.assertEquals("2020-04-01", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, dateLiteralExpr, PrimitiveType.DATE, 0).getStringValue());
        Assert.assertEquals("2020-07-01", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, dateLiteralExpr, PrimitiveType.DATE, 1).getStringValue());
        Assert.assertEquals("2021-07-01", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, dateLiteralExpr, PrimitiveType.DATE, 5).getStringValue());

        Assert.assertEquals("2020-04-01 00:00:00", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, dateTimeLiteralExpr, PrimitiveType.DATETIME, 0).getStringValue());
        Assert.assertEquals("2020-07-01 00:00:00", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, dateTimeLiteralExpr, PrimitiveType.DATETIME, 1).getStringValue());
        Assert.assertEquals("2021-07-01 00:00:00", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, dateTimeLiteralExpr, PrimitiveType.DATETIME, 5).getStringValue());


        Assert.assertEquals("0000-01-01", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, minDateLiteralExpr, PrimitiveType.DATE, -1).getStringValue());
        Assert.assertEquals("0000-01-01", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, minDateLiteralExpr, PrimitiveType.DATE, 0).getStringValue());
        Assert.assertEquals("0000-04-01", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, minDateLiteralExpr, PrimitiveType.DATE, 1).getStringValue());

        Assert.assertEquals("0000-01-01 00:00:00", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, minDatetimeLiteralExpr, PrimitiveType.DATETIME, -1).getStringValue());
        Assert.assertEquals("0000-01-01 00:00:00", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, minDatetimeLiteralExpr, PrimitiveType.DATETIME, 0).getStringValue());
        Assert.assertEquals("0000-04-01 00:00:00", ExpressionPartitionUtil.getBound(
                quarterFunctionCallExpr, minDatetimeLiteralExpr, PrimitiveType.DATETIME, 1).getStringValue());

        // date_trunc year test
        Assert.assertEquals("2020-01-01", ExpressionPartitionUtil.getBound(
                yearFunctionCallExpr, dateLiteralExpr, PrimitiveType.DATE, 0).getStringValue());
        Assert.assertEquals("2021-01-01", ExpressionPartitionUtil.getBound(
                yearFunctionCallExpr, dateLiteralExpr, PrimitiveType.DATE, 1).getStringValue());
        Assert.assertEquals("2025-01-01", ExpressionPartitionUtil.getBound(
                yearFunctionCallExpr, dateLiteralExpr, PrimitiveType.DATE, 5).getStringValue());

        Assert.assertEquals("2020-01-01 00:00:00", ExpressionPartitionUtil.getBound(
                yearFunctionCallExpr, dateTimeLiteralExpr, PrimitiveType.DATETIME, 0).getStringValue());
        Assert.assertEquals("2021-01-01 00:00:00", ExpressionPartitionUtil.getBound(
                yearFunctionCallExpr, dateTimeLiteralExpr, PrimitiveType.DATETIME, 1).getStringValue());
        Assert.assertEquals("2025-01-01 00:00:00", ExpressionPartitionUtil.getBound(
                yearFunctionCallExpr, dateTimeLiteralExpr, PrimitiveType.DATETIME, 5).getStringValue());

    }

    @Test
    public void testGetBoundHasNoSupportFunction() throws AnalysisException {
        LiteralExpr dateTimeLiteralExpr = DateLiteral.create("2020-04-21 20:43:00", Type.DATETIME);
        StringLiteral stringLiteral = new StringLiteral("yyyy-MM-dd");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_format", Arrays.asList(slotRef, stringLiteral));
        try {
            ExpressionPartitionUtil.getBound(functionCallExpr, dateTimeLiteralExpr, PrimitiveType.DATETIME, 0);
        } catch (Exception e) {
            Assert.assertEquals("Do not support function:date_format(`db1`.`table1`.`k1`, 'yyyy-MM-dd')",
                    e.getMessage());
        }
    }

    @Test
    public void testCompareBound() throws AnalysisException {

        LiteralExpr dateBound1 = DateLiteral.create("2020-04-21", Type.DATE);
        LiteralExpr dateBound2 = DateLiteral.create("2021-04-21", Type.DATE);
        LiteralExpr dateTimeBound1 = DateLiteral.create("2020-04-21 20:43:00", Type.DATETIME);
        LiteralExpr dateTimeBound2 = DateLiteral.create("2021-04-21 20:43:00", Type.DATETIME);;

        Assert.assertEquals(ExpressionPartitionUtil.compareBound(dateBound1, dateBound2), -1);
        Assert.assertEquals(ExpressionPartitionUtil.compareBound(dateBound2, dateBound1), 1);
        Assert.assertEquals(ExpressionPartitionUtil.compareBound(dateBound1, dateBound1), 0);

        Assert.assertEquals(
                ExpressionPartitionUtil.compareBound(dateTimeBound1, dateTimeBound2), -1);
        Assert.assertEquals(
                ExpressionPartitionUtil.compareBound(dateTimeBound2, dateTimeBound1), 1);
        Assert.assertEquals(
                ExpressionPartitionUtil.compareBound(dateTimeBound1, dateTimeBound1), 0);
    }

    @Test
    public void testGetFormattedPartitionName(){
        List<String> times = Arrays.asList(
                "0000-01-01",
                "0001-01-01",
                "2020-04-21 20:43:00",
                "2021-04-21 20:43:00",
                "1",
                "10"
        );
        Assert.assertEquals(ExpressionPartitionUtil.getFormattedPartitionName(times.get(0), times.get(1),PrimitiveType.DATE),"00000101_00010101");
        Assert.assertEquals(ExpressionPartitionUtil.getFormattedPartitionName(times.get(2), times.get(3),PrimitiveType.DATETIME),"20200421204300_20210421204300");
        Assert.assertEquals(ExpressionPartitionUtil.getFormattedPartitionName(times.get(4), times.get(5),PrimitiveType.INT),"1_10");
    }

}
