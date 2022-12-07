// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScalarOperatorFunctionsTest {
    private ConstantOperator O_DT_20101102_183010;

    private ConstantOperator O_DT_20101202_023010;

    private ConstantOperator O_DT_20150323_092355;

    private ConstantOperator O_INT_10;
    private ConstantOperator O_FLOAT_100;
    private ConstantOperator O_DOUBLE_100;
    private ConstantOperator O_BI_100;
    private ConstantOperator O_LI_100;
    private ConstantOperator O_DECIMAL_100;
    private ConstantOperator O_DECIMAL32P7S2_100;
    private ConstantOperator O_DECIMAL32P9S0_100;
    private ConstantOperator O_DECIMAL64P18S15_100;
    private ConstantOperator O_DECIMAL64P15S10_100;
    private ConstantOperator O_DECIMAL128P38S20_100;
    private ConstantOperator O_DECIMAL128P30S2_100;

    @Before
    public void setUp() throws AnalysisException {

        O_DT_20101102_183010 = ConstantOperator.createDatetime(LocalDateTime.of(2010, 11, 2, 18, 30, 10));
        O_DT_20101202_023010 = ConstantOperator.createDatetime(LocalDateTime.of(2010, 12, 2, 2, 30, 10));
        O_DT_20150323_092355 = ConstantOperator.createDatetime(LocalDateTime.of(2015, 3, 23, 9, 23, 55));
        O_INT_10 = ConstantOperator.createInt(10);
        O_FLOAT_100 = ConstantOperator.createFloat(100);
        O_DOUBLE_100 = ConstantOperator.createFloat(100);
        O_BI_100 = ConstantOperator.createBigint(100);
        O_LI_100 = ConstantOperator.createLargeInt(new BigInteger("100"));
        O_DECIMAL_100 = ConstantOperator.createDecimal(new BigDecimal(100), Type.DECIMALV2);
        O_DECIMAL32P7S2_100 = ConstantOperator.createDecimal(new BigDecimal(100),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        O_DECIMAL32P9S0_100 = ConstantOperator.createDecimal(new BigDecimal(100),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 0));
        O_DECIMAL64P15S10_100 = ConstantOperator.createDecimal(new BigDecimal(100),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10));
        O_DECIMAL64P18S15_100 = ConstantOperator.createDecimal(new BigDecimal(100),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 15));
        O_DECIMAL128P38S20_100 = ConstantOperator.createDecimal(new BigDecimal(100),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 20));
        O_DECIMAL128P30S2_100 = ConstantOperator.createDecimal(new BigDecimal(100),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 30, 2));
    }

    @Test
    public void timeDiff() {
        assertEquals(-2534400.0, ScalarOperatorFunctions.timeDiff(O_DT_20101102_183010, O_DT_20101202_023010).getTime(),
                1);
    }

    @Test
    public void dateDiff() {
        assertEquals(-1602,
                ScalarOperatorFunctions.dateDiff(O_DT_20101102_183010, O_DT_20150323_092355).getInt());

        assertEquals(-1572, ScalarOperatorFunctions.dateDiff(O_DT_20101202_023010, O_DT_20150323_092355).getInt());
    }

    @Test
    public void yearsAdd() {
        assertEquals("2025-03-23T09:23:55",
                ScalarOperatorFunctions.yearsAdd(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void monthsAdd() {
        assertEquals("2016-01-23T09:23:55",
                ScalarOperatorFunctions.monthsAdd(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void daysAdd() {
        assertEquals("2015-04-02T09:23:55",
                ScalarOperatorFunctions.daysAdd(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void hoursAdd() {
        assertEquals("2015-03-23T19:23:55",
                ScalarOperatorFunctions.hoursAdd(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void minutesAdd() {
        assertEquals("2015-03-23T09:33:55",
                ScalarOperatorFunctions.minutesAdd(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void secondsAdd() {
        assertEquals("2015-03-23T09:24:05",
                ScalarOperatorFunctions.secondsAdd(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void dateFormat() {
        Locale.setDefault(Locale.ENGLISH);
        ConstantOperator testDate = ConstantOperator.createDatetime(LocalDateTime.of(2001, 1, 9, 13, 4, 5));
        assertEquals("1",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%c")).getVarchar());
        assertEquals("09",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%d")).getVarchar());
        assertEquals("9",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%e")).getVarchar());
        assertEquals("13",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%H")).getVarchar());
        assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%h")).getVarchar());
        assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%I")).getVarchar());
        assertEquals("04",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%i")).getVarchar());
        assertEquals("009",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%j")).getVarchar());
        assertEquals("13",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%k")).getVarchar());
        assertEquals("1",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%l")).getVarchar());
        assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%m")).getVarchar());
        assertEquals("05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%S")).getVarchar());
        assertEquals("05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%s")).getVarchar());
        assertEquals("13:04:05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%T")).getVarchar());
        assertEquals("02",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%v")).getVarchar());
        assertEquals("2001",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%Y")).getVarchar());
        assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%y")).getVarchar());
        assertEquals("%",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%%")).getVarchar());
        assertEquals("foo",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("foo")).getVarchar());
        assertEquals("g",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%g")).getVarchar());
        assertEquals("4",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%4")).getVarchar());
        assertEquals("02",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%v")).getVarchar());
        assertEquals("yyyy",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyy")).getVarchar());
        assertEquals("20010109",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyyMMdd")).getVarchar());
        assertEquals("yyyyMMdd HH:mm:ss",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyyMMdd HH:mm:ss"))
                        .getVarchar());
        assertEquals("HH:mm:ss",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("HH:mm:ss")).getVarchar());
        assertEquals("2001-01-09",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyy-MM-dd"))
                        .getVarchar());
        assertEquals("2001-01-09 13:04:05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyy-MM-dd HH:mm:ss"))
                        .getVarchar());

        assertEquals("2001-01-09",
                ScalarOperatorFunctions.dateFormat(ConstantOperator.createDate(LocalDateTime.of(2001, 1, 9, 13, 4, 5)),
                        ConstantOperator.createVarchar("%Y-%m-%d"))
                        .getVarchar());
        assertEquals("000123", ScalarOperatorFunctions
                .dateFormat(ConstantOperator.createDate(LocalDateTime.of(2022, 3, 13, 0, 0, 0, 123000)),
                        ConstantOperator.createVarchar("%f")).getVarchar());

        assertEquals("asdfafdfsçv",
                ScalarOperatorFunctions.dateFormat(ConstantOperator.createDate(LocalDateTime.of(2020, 2, 21, 13, 4, 5)),
                        ConstantOperator.createVarchar("asdfafdfsçv")).getVarchar());

        Assert.assertThrows("%a not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%a")).getVarchar());
        Assert.assertThrows("%b not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%b")).getVarchar());
        Assert.assertThrows("%M not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%M")).getVarchar());
        Assert.assertThrows("%W not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%W")).getVarchar());
        Assert.assertThrows("%x not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%x")).getVarchar());
        Assert.assertThrows("%w not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%w")).getVarchar());
        Assert.assertThrows("%p not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%p")).getVarchar());
        Assert.assertThrows("%r not supported in date format string", IllegalArgumentException.class, () ->
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%r")).getVarchar());

        Assert.assertThrows(IllegalArgumentException.class, () -> ScalarOperatorFunctions
                .dateFormat(ConstantOperator.createDate(LocalDateTime.of(2020, 2, 21, 13, 4, 5)),
                        ConstantOperator.createVarchar("%U")).getVarchar());
        Assert.assertThrows(IllegalArgumentException.class, () -> ScalarOperatorFunctions
                .dateFormat(ConstantOperator.createDate(LocalDateTime.of(2020, 2, 21, 13, 4, 5)),
                        ConstantOperator.createVarchar("%X")).getVarchar());
        assertTrue(ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar(""))
                .isNull());
        assertEquals("  ",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("  "))
                        .getVarchar());
    }

    @Test
    public void dateParse() {
        assertEquals("2013-05-10T00:00", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2013,05,10"), ConstantOperator.createVarchar("%Y,%m,%d"))
                .getDatetime().toString());
        assertEquals("2013-05-10T00:00", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("   2013,05,10   "),
                        ConstantOperator.createVarchar("%Y,%m,%d"))
                .getDatetime().toString());
        assertEquals("2013-05-17T12:35:10", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2013-05-17 12:35:10"),
                        ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")).getDatetime().toString());
        assertEquals("2013-05-17T00:35:10", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2013-05-17 00:35:10"),
                        ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")).getDatetime().toString());
        assertEquals("2013-05-17T23:35:10", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("abc 2013-05-17 fff 23:35:10 xyz"),
                        ConstantOperator.createVarchar("abc %Y-%m-%d fff %H:%i:%s xyz")).getDatetime().toString());
        assertEquals("2019-05-09T00:00", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2019,129"), ConstantOperator.createVarchar("%Y,%j"))
                .getDatetime().toString());
        assertEquals("2019-05-09T12:10:45", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("12:10:45-20190509"),
                        ConstantOperator.createVarchar("%T-%Y%m%d")).getDatetime().toString());
        assertEquals("2019-05-09T09:10:45", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("20190509-9:10:45"),
                        ConstantOperator.createVarchar("%Y%m%d-%k:%i:%S")).getDatetime().toString());
        assertEquals("2020-02-21 00:00:00",
                ScalarOperatorFunctions.dateParse(ConstantOperator.createVarchar("2020-02-21"),
                        ConstantOperator.createVarchar("%Y-%m-%d")).toString());
        assertEquals("2020-02-21 00:00:00",
                ScalarOperatorFunctions.dateParse(ConstantOperator.createVarchar("20-02-21"),
                        ConstantOperator.createVarchar("%y-%m-%d")).toString());
        assertEquals("1998-02-21 00:00:00",
                ScalarOperatorFunctions.dateParse(ConstantOperator.createVarchar("98-02-21"),
                        ConstantOperator.createVarchar("%y-%m-%d")).toString());

        Assert.assertThrows(DateTimeException.class, () -> ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("201905"),
                        ConstantOperator.createVarchar("%Y%m")).getDatetime());

        Assert.assertThrows(DateTimeException.class, () -> ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("20190507"),
                        ConstantOperator.createVarchar("%Y%m")).getDatetime());

        Assert.assertThrows(DateTimeParseException.class, () -> ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2019-02-29"),
                        ConstantOperator.createVarchar("%Y-%m-%d")).getDatetime());

        Assert.assertThrows(DateTimeParseException.class, () -> ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2019-02-29 11:12:13"),
                        ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")).getDatetime());

        Assert.assertThrows(IllegalArgumentException.class,
                () -> ScalarOperatorFunctions.dateParse(ConstantOperator.createVarchar("2020-2-21"),
                        ConstantOperator.createVarchar("%w")).getVarchar());

        Assert.assertThrows(IllegalArgumentException.class,
                () -> ScalarOperatorFunctions.dateParse(ConstantOperator.createVarchar("2020-02-21"),
                        ConstantOperator.createVarchar("%w")).getVarchar());

        Assert.assertThrows(DateTimeParseException.class,
                () -> ScalarOperatorFunctions.dateParse(ConstantOperator.createVarchar("\f 2020-02-21"),
                        ConstantOperator.createVarchar("%Y-%m-%d")).getVarchar());

        Assert.assertThrows("Unable to obtain LocalDateTime", DateTimeException.class, () -> ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2013-05-17 12:35:10"),
                        ConstantOperator.createVarchar("%Y-%m-%d %h:%i:%s")).getDatetime());
    }

    @Test
    public void str2Date() {
        assertEquals("2013-05-10T00:00", ScalarOperatorFunctions
                .str2Date(ConstantOperator.createVarchar("2013,05,10"), ConstantOperator.createVarchar("%Y,%m,%d"))
                .getDate().toString());
        assertEquals("2013-05-10T00:00", ScalarOperatorFunctions
                .str2Date(ConstantOperator.createVarchar("   2013,05,10  "), ConstantOperator.createVarchar("%Y,%m,%d"))
                .getDate().toString());
        assertEquals("2013-05-17T00:00", ScalarOperatorFunctions
                .str2Date(ConstantOperator.createVarchar("2013-05-17 12:35:10"),
                        ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")).getDate().toString());
        assertEquals("2013-05-17T00:00", ScalarOperatorFunctions
                .str2Date(ConstantOperator.createVarchar("13-05-17 12:35:10"),
                        ConstantOperator.createVarchar("%y-%m-%d %H:%i:%s")).getDate().toString());
        assertEquals("1998-05-17T00:00", ScalarOperatorFunctions
                .str2Date(ConstantOperator.createVarchar("98-05-17 12:35:10"),
                        ConstantOperator.createVarchar("%y-%m-%d %H:%i:%s")).getDate().toString());

        Assert.assertThrows(DateTimeParseException.class, () -> ScalarOperatorFunctions
                .str2Date(ConstantOperator.createVarchar("2019-02-29"),
                        ConstantOperator.createVarchar("%Y-%m-%d")).getDatetime());
    }

    @Test
    public void yearsSub() {
        assertEquals("2005-03-23T09:23:55",
                ScalarOperatorFunctions.yearsSub(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void monthsSub() {
        assertEquals("2014-05-23T09:23:55",
                ScalarOperatorFunctions.monthsSub(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void daysSub() {
        assertEquals("2015-03-13T09:23:55",
                ScalarOperatorFunctions.daysSub(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void hoursSub() {
        assertEquals("2015-03-22T23:23:55",
                ScalarOperatorFunctions.hoursSub(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void minutesSub() {
        assertEquals("2015-03-23T09:13:55",
                ScalarOperatorFunctions.minutesSub(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void secondsSub() {
        assertEquals("2015-03-23T09:23:45",
                ScalarOperatorFunctions.secondsSub(O_DT_20150323_092355, O_INT_10).getDatetime().toString());
    }

    @Test
    public void year() {
        ConstantOperator date = ConstantOperator.createDatetime(LocalDateTime.of(2000, 10, 21, 12, 0));
        ConstantOperator result = ScalarOperatorFunctions.year(date);

        assertEquals(Type.SMALLINT, result.getType());
        assertEquals(2000, result.getSmallint());
    }

    @Test
    public void month() {
        assertEquals(3, ScalarOperatorFunctions.month(O_DT_20150323_092355).getTinyInt());
    }

    @Test
    public void day() {
        assertEquals(23, ScalarOperatorFunctions.day(O_DT_20150323_092355).getTinyInt());
    }

    @Test
    public void unixTimestamp() {
        ConstantOperator codt = ConstantOperator.createDatetime(LocalDateTime.of(2050, 3, 23, 9, 23, 55));

        assertEquals(0,
                ScalarOperatorFunctions.unixTimestamp(codt).getInt());
        assertEquals(1427073835,
                ScalarOperatorFunctions.unixTimestamp(O_DT_20150323_092355).getInt());
    }

    @Test
    public void fromUnixTime() throws AnalysisException {
        assertEquals("1970-01-01 08:00:10",
                ScalarOperatorFunctions.fromUnixTime(O_INT_10).getVarchar());
    }

    @Test
    public void curDate() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setThreadLocalInfo();
        ctx.setStartTime();
        LocalDateTime now = LocalDateTime.of(LocalDate.now(), LocalTime.of(0, 0, 0));
        assertEquals(now, ScalarOperatorFunctions.curDate().getDate());
    }

    @Test
    public void floor() {
        assertEquals(100, ScalarOperatorFunctions.floor(O_FLOAT_100).getBigint());
    }

    @Test
    public void addInt() {
        assertEquals(200, ScalarOperatorFunctions.addBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void addDouble() {
        assertEquals(200.0,
                ScalarOperatorFunctions.addDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void addDecimal() {
        assertEquals("200",
                ScalarOperatorFunctions.addDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toPlainString());
        assertEquals("200",
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toPlainString());
        assertTrue(
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getType().isDecimalV3());

        assertEquals("200",
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toPlainString());
        assertTrue(
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getType().isDecimalV3());

        assertEquals("200",
                ScalarOperatorFunctions.addDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getType()
                .isDecimalV3());

        assertEquals("200",
                ScalarOperatorFunctions.addDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getType()
                .isDecimalV3());

        assertEquals("200",
                ScalarOperatorFunctions.addDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getType()
                .isDecimalV3());

        assertEquals("200",
                ScalarOperatorFunctions.addDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void addBigInt() {
        assertEquals("200",
                ScalarOperatorFunctions.addLargeInt(O_LI_100, O_LI_100).getLargeInt().toString());
    }

    @Test
    public void subtractInt() {
        assertEquals(0, ScalarOperatorFunctions.subtractBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void subtractDouble() {
        assertEquals(0.0,
                ScalarOperatorFunctions.subtractDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void subtractDecimal() {
        assertEquals("0",
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toString());
        assertEquals("0",
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toString());

        assertTrue(ScalarOperatorFunctions.subtractDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void subtractBigInt() {
        assertEquals("0",
                ScalarOperatorFunctions.subtractLargeInt(O_LI_100, O_LI_100).getLargeInt().toString());
    }

    @Test
    public void multiplyInt() {
        assertEquals(10000,
                ScalarOperatorFunctions.multiplyBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void multiplyDouble() {
        assertEquals(10000.0,
                ScalarOperatorFunctions.multiplyDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void multiplyDecimal() {
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toPlainString());
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toPlainString());
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toPlainString());
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toPlainString());
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toPlainString());
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toPlainString());
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toPlainString());

        assertTrue(ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void multiplyBigInt() {
        assertEquals("10000",
                ScalarOperatorFunctions.multiplyLargeInt(O_LI_100, O_LI_100).getLargeInt().toString());
    }

    @Test
    public void divideDouble() {
        assertEquals(1.0,
                ScalarOperatorFunctions.divideDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void divideDecimal() {
        assertEquals("1",
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toString());
        assertEquals("1",
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toString());
        assertEquals("1",
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toString());
        assertEquals("1",
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toString());
        assertEquals("1",
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toString());
        assertEquals("1",
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toString());
        assertEquals("1",
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toString());

        assertTrue(ScalarOperatorFunctions.divideDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());

    }

    @Test
    public void modInt() {
        assertEquals(0, ScalarOperatorFunctions.modBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void modDecimal() {
        assertEquals("0", ScalarOperatorFunctions.modDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toString());
        assertEquals("0",
                ScalarOperatorFunctions.modDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal().toString());
        assertEquals("0",
                ScalarOperatorFunctions.modDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal().toString());
        assertEquals("0",
                ScalarOperatorFunctions.modDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.modDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.modDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toString());
        assertEquals("0",
                ScalarOperatorFunctions.modDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toString());
        assertTrue(ScalarOperatorFunctions.modDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void concat() {
        ConstantOperator[] arg = {ConstantOperator.createVarchar("1"),
                ConstantOperator.createVarchar("2"),
                ConstantOperator.createVarchar("3")};
        ConstantOperator result = ScalarOperatorFunctions.concat(arg);

        assertEquals(Type.VARCHAR, result.getType());
        assertEquals("123", result.getVarchar());
    }

    @Test
    public void concat_ws() {
        ConstantOperator[] arg = {ConstantOperator.createVarchar("1"),
                ConstantOperator.createVarchar("2"),
                ConstantOperator.createVarchar("3")};
        ConstantOperator result = ScalarOperatorFunctions.concat_ws(ConstantOperator.createVarchar(","), arg);

        assertEquals(Type.VARCHAR, result.getType());
        assertEquals("1,2,3", result.getVarchar());
    }

    @Test
    public void concat_ws_with_null() {
        ConstantOperator[] arg_with_null = {ConstantOperator.createVarchar("star"),
                ConstantOperator.createNull(Type.VARCHAR),
                ConstantOperator.createVarchar("cks")};
        ConstantOperator result =
                ScalarOperatorFunctions.concat_ws(ConstantOperator.createVarchar("ro"), arg_with_null);
        assertEquals(Type.VARCHAR, result.getType());
        assertEquals("starrocks", result.getVarchar());

        result = ScalarOperatorFunctions.concat_ws(ConstantOperator.createVarchar(","),
                ConstantOperator.createNull(Type.VARCHAR));
        assertEquals("", result.getVarchar());

        ConstantOperator[] arg_without_null = {ConstantOperator.createVarchar("star"),
                ConstantOperator.createVarchar("cks")};
        result = ScalarOperatorFunctions.concat_ws(ConstantOperator.createNull(Type.VARCHAR), arg_without_null);
        assertTrue(result.isNull());
    }

    @Test
    public void fromUnixTime2() throws AnalysisException {
        ConstantOperator date =
                ScalarOperatorFunctions.fromUnixTime(O_INT_10, ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s"));
        assertTrue(date.toString().matches("1970-01-01 0.*:00:10"));
    }
}
