// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.common.util;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
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

    private static String lowerBorder;

    private static String upperBorder;

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
        lowerBorder = "2020-04-21 20:45:00";
        upperBorder = "2021-04-21 20:45:00";
        PartitionValue lowerValue = new PartitionValue(lowerBorder);
        PartitionValue upperValue = new PartitionValue(upperBorder);
        PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue),
                Collections.singletonList(partitionColumn));
        PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                Collections.singletonList(partitionColumn));
        basePartitionKeyRange = Range.closedOpen(lowerBound, upperBound);
    }

    @Test
    public void testGetPartitionKeyRange() throws AnalysisException {
        List<Range<PartitionKey>> existPartitionKeyRanges = Lists.newArrayList();
        // slot ref test
        Range<PartitionKey> partitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(slotRef, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(partitionKeyRange != null);
        Assert.assertEquals(partitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue(), lowerBorder);
        Assert.assertEquals(partitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue(), upperBorder);
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
        PartitionKey lowerBound1 =
                PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2020-01-01 00:00:00")),
                        Collections.singletonList(partitionColumn));
        PartitionKey upperBound1 =
                PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2020-07-01 00:00:00")),
                        Collections.singletonList(partitionColumn));
        existPartitionKeyRanges.add(Range.closedOpen(lowerBound1, upperBound1));
        PartitionKey lowerBound2 =
                PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2021-04-01 00:00:00")),
                        Collections.singletonList(partitionColumn));
        PartitionKey upperBound2 =
                PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2021-07-01 00:00:00")),
                        Collections.singletonList(partitionColumn));
        existPartitionKeyRanges.add(Range.closedOpen(lowerBound2, upperBound2));
        quarterPartitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(quarterFunctionCallExpr, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(quarterPartitionKeyRange != null);
        Assert.assertEquals(quarterPartitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue(),
                "2020-07-01 00:00:00");
        Assert.assertEquals(quarterPartitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue(),
                "2021-04-01 00:00:00");
        //date_trunc year test with exist ranges
        existPartitionKeyRanges = Lists.newArrayList();
        lowerBound1 = PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2020-01-01 00:00:00")),
                Collections.singletonList(partitionColumn));
        upperBound1 = PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2021-01-01 00:00:00")),
                Collections.singletonList(partitionColumn));
        existPartitionKeyRanges.add(Range.closedOpen(lowerBound1, upperBound1));
        lowerBound2 = PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2021-01-01 00:00:00")),
                Collections.singletonList(partitionColumn));
        upperBound2 = PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue("2022-01-01 00:00:00")),
                Collections.singletonList(partitionColumn));
        existPartitionKeyRanges.add(Range.closedOpen(lowerBound2, upperBound2));
        yearPartitionKeyRange =
                ExpressionPartitionUtil.getPartitionKeyRange(yearFunctionCallExpr, partitionColumn,
                        existPartitionKeyRanges, basePartitionKeyRange, 0);
        Assert.assertTrue(yearPartitionKeyRange == null);
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
    public void testGetPartitionKeyRangsHasNoSupportFunction() {
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
    public void testGetBorder() {
        String dateBorder = "2020-04-21";
        String dateTimeBorder = "2020-04-21 20:45:00";

        // date_trunc quarter test
        Assert.assertEquals("2020-04-01", ExpressionPartitionUtil.getBorder(
                quarterFunctionCallExpr, dateBorder, PrimitiveType.DATE, 0));
        Assert.assertEquals("2020-07-01", ExpressionPartitionUtil.getBorder(
                quarterFunctionCallExpr, dateBorder, PrimitiveType.DATE, 1));
        Assert.assertEquals("2021-07-01", ExpressionPartitionUtil.getBorder(
                quarterFunctionCallExpr, dateBorder, PrimitiveType.DATE, 5));

        Assert.assertEquals("2020-04-01 00:00:00", ExpressionPartitionUtil.getBorder(
                quarterFunctionCallExpr, dateTimeBorder, PrimitiveType.DATETIME, 0));
        Assert.assertEquals("2020-07-01 00:00:00", ExpressionPartitionUtil.getBorder(
                quarterFunctionCallExpr, dateTimeBorder, PrimitiveType.DATETIME, 1));
        Assert.assertEquals("2021-07-01 00:00:00", ExpressionPartitionUtil.getBorder(
                quarterFunctionCallExpr, dateTimeBorder, PrimitiveType.DATETIME, 5));

        // date_trunc year test
        Assert.assertEquals("2020-01-01", ExpressionPartitionUtil.getBorder(
                yearFunctionCallExpr, dateBorder, PrimitiveType.DATE, 0));
        Assert.assertEquals("2021-01-01", ExpressionPartitionUtil.getBorder(
                yearFunctionCallExpr, dateBorder, PrimitiveType.DATE, 1));
        Assert.assertEquals("2025-01-01", ExpressionPartitionUtil.getBorder(
                yearFunctionCallExpr, dateBorder, PrimitiveType.DATE, 5));

        Assert.assertEquals("2020-01-01 00:00:00", ExpressionPartitionUtil.getBorder(
                yearFunctionCallExpr, dateTimeBorder, PrimitiveType.DATETIME, 0));
        Assert.assertEquals("2021-01-01 00:00:00", ExpressionPartitionUtil.getBorder(
                yearFunctionCallExpr, dateTimeBorder, PrimitiveType.DATETIME, 1));
        Assert.assertEquals("2025-01-01 00:00:00", ExpressionPartitionUtil.getBorder(
                yearFunctionCallExpr, dateTimeBorder, PrimitiveType.DATETIME, 5));
    }

    @Test
    public void testGetBorderHasNoSupportFunction() {
        String dateTimeBorder = "2020-04-21 20:45:00";
        StringLiteral stringLiteral = new StringLiteral("yyyy-MM-dd");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_format", Arrays.asList(slotRef, stringLiteral));
        try {
            ExpressionPartitionUtil.getBorder(functionCallExpr, dateTimeBorder, PrimitiveType.DATETIME, 0);
        } catch (Exception e) {
            Assert.assertEquals("Do not support function:date_format(`db1`.`table1`.`k1`, 'yyyy-MM-dd')",
                    e.getMessage());
        }
    }

    @Test
    public void testCompareBorder() {
        String dateBorder1 = "2020-04-21";
        String dateBorder2 = "2021-04-21";
        String dateTimeBorder1 = "2020-04-21 20:45:00";
        String dateTimeBorder2 = "2021-04-21 20:45:00";

        Assert.assertEquals(ExpressionPartitionUtil.compareBorder(quarterFunctionCallExpr, dateBorder1, dateBorder2,
                PrimitiveType.DATE), -1);
        Assert.assertEquals(ExpressionPartitionUtil.compareBorder(quarterFunctionCallExpr, dateBorder2, dateBorder1,
                PrimitiveType.DATE), 1);
        Assert.assertEquals(ExpressionPartitionUtil.compareBorder(quarterFunctionCallExpr, dateBorder1, dateBorder1,
                PrimitiveType.DATE), 0);

        Assert.assertEquals(
                ExpressionPartitionUtil.compareBorder(quarterFunctionCallExpr, dateTimeBorder1, dateTimeBorder2,
                        PrimitiveType.DATETIME), -1);
        Assert.assertEquals(
                ExpressionPartitionUtil.compareBorder(quarterFunctionCallExpr, dateTimeBorder2, dateTimeBorder1,
                        PrimitiveType.DATETIME), 1);
        Assert.assertEquals(
                ExpressionPartitionUtil.compareBorder(quarterFunctionCallExpr, dateTimeBorder1, dateTimeBorder1,
                        PrimitiveType.DATETIME), 0);
    }

    @Test
    public void testCompareBorderHasNoSupportFunction() {
        String dateTimeBorder1 = "2020-04-21 20:45:00";
        String dateTimeBorder2 = "2021-04-21 20:45:00";
        StringLiteral stringLiteral = new StringLiteral("yyyy-MM-dd");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_format", Arrays.asList(slotRef, stringLiteral));
        try {
            ExpressionPartitionUtil.compareBorder(functionCallExpr, dateTimeBorder1, dateTimeBorder2,
                    PrimitiveType.DATETIME);
        } catch (Exception e) {
            Assert.assertEquals("Do not support function:date_format(`db1`.`table1`.`k1`, 'yyyy-MM-dd')",
                    e.getMessage());
        }
    }
}
