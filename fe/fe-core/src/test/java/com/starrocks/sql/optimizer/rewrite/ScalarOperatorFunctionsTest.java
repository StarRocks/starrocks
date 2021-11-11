// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.rewrite.FEFunctions;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Objects;

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

    private DateLiteral L_DT_20101102_183010;

    private DateLiteral L_DT_20101202_023010;

    private DateLiteral L_DT_20150323_092355;

    private IntLiteral L_INT_10;
    private FloatLiteral L_FLOAT_100;
    private FloatLiteral L_DOUBLE_100;
    private IntLiteral L_BI_100;
    private LargeIntLiteral L_LI_100;
    private DecimalLiteral L_DECIMAL_100;
    private DecimalLiteral L_DECIMAL32P7S2_100;
    private DecimalLiteral L_DECIMAL32P9S0_100;
    private DecimalLiteral L_DECIMAL64P18S15_100;
    private DecimalLiteral L_DECIMAL64P15S10_100;
    private DecimalLiteral L_DECIMAL128P38S20_100;
    private DecimalLiteral L_DECIMAL128P30S2_100;

    @Before
    public void setUp() throws AnalysisException {
        Config.enable_decimal_v3 = true;

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

        L_DT_20101102_183010 = new DateLiteral(2010, 11, 2, 18, 30, 10);
        L_DT_20101202_023010 = new DateLiteral(2010, 12, 2, 2, 30, 10);
        L_DT_20150323_092355 = new DateLiteral(2015, 3, 23, 9, 23, 55);
        L_INT_10 = new IntLiteral(10);
        L_FLOAT_100 = new FloatLiteral(100.0, Type.FLOAT);

        L_DOUBLE_100 = new FloatLiteral(100.0, Type.DOUBLE);
        L_BI_100 = new IntLiteral(100);
        L_LI_100 = new LargeIntLiteral("100");
        L_DECIMAL_100 = new DecimalLiteral("100", ScalarType.createDecimalV2Type());
        L_DECIMAL32P7S2_100 = new DecimalLiteral("100", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        L_DECIMAL32P9S0_100 = new DecimalLiteral("100", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 0));
        L_DECIMAL64P15S10_100 =
                new DecimalLiteral("100", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10));
        L_DECIMAL64P18S15_100 =
                new DecimalLiteral("100", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 15));
        L_DECIMAL128P38S20_100 =
                new DecimalLiteral("100", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 20));
        L_DECIMAL128P30S2_100 =
                new DecimalLiteral("100", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 30, 2));
    }

    @Test
    public void timeDiff() throws AnalysisException {
        assertEquals(FEFunctions.timeDiff(L_DT_20101102_183010, L_DT_20101202_023010).getDoubleValue(),
                ScalarOperatorFunctions.timeDiff(O_DT_20101102_183010, O_DT_20101202_023010).getTime(), 1);
    }

    @Test
    public void dateDiff() throws AnalysisException {
        assertEquals(FEFunctions.dateDiff(L_DT_20101102_183010, L_DT_20150323_092355).getLongValue(),
                ScalarOperatorFunctions.dateDiff(O_DT_20101102_183010, O_DT_20150323_092355).getInt());

        assertEquals(FEFunctions.dateDiff(L_DT_20101202_023010, L_DT_20150323_092355).getLongValue(),
                ScalarOperatorFunctions.dateDiff(O_DT_20101202_023010, O_DT_20150323_092355).getInt());
    }

    @Test
    public void yearsAdd() throws AnalysisException {
        assertEquals(FEFunctions.yearsAdd(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.yearsAdd(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void monthsAdd() throws AnalysisException {
        assertEquals(FEFunctions.monthsAdd(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.monthsAdd(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void daysAdd() throws AnalysisException {
        assertEquals(FEFunctions.daysAdd(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.daysAdd(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void hoursAdd() throws AnalysisException {
        assertEquals(FEFunctions.hoursAdd(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.hoursAdd(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void minutesAdd() throws AnalysisException {
        assertEquals(FEFunctions.minutesAdd(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.minutesAdd(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void secondsAdd() throws AnalysisException {
        assertEquals(FEFunctions.secondsAdd(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.secondsAdd(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void dateFormat() throws AnalysisException {
        Locale.setDefault(Locale.ENGLISH);
        ConstantOperator testDate = ConstantOperator.createDatetime(LocalDateTime.of(2001, 1, 9, 13, 4, 5));
        Assert.assertEquals("Tue",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%a")).getVarchar());
        Assert.assertEquals("Jan",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%b")).getVarchar());
        Assert.assertEquals("1",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%c")).getVarchar());
        Assert.assertEquals("09",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%d")).getVarchar());
        Assert.assertEquals("9",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%e")).getVarchar());
        Assert.assertEquals("13",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%H")).getVarchar());
        Assert.assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%h")).getVarchar());
        Assert.assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%I")).getVarchar());
        Assert.assertEquals("04",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%i")).getVarchar());
        Assert.assertEquals("009",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%j")).getVarchar());
        Assert.assertEquals("13",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%k")).getVarchar());
        Assert.assertEquals("1",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%l")).getVarchar());
        Assert.assertEquals("January",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%M")).getVarchar());
        Assert.assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%m")).getVarchar());
        Assert.assertEquals("PM",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%p")).getVarchar());
        Assert.assertEquals("01:04:05 PM",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%r")).getVarchar());
        Assert.assertEquals("05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%S")).getVarchar());
        Assert.assertEquals("05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%s")).getVarchar());
        Assert.assertEquals("13:04:05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%T")).getVarchar());
        Assert.assertEquals("02",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%v")).getVarchar());
        Assert.assertEquals("Tuesday",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%W")).getVarchar());
        Assert.assertEquals("2001",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%Y")).getVarchar());
        Assert.assertEquals("01",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%y")).getVarchar());
        Assert.assertEquals("%",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%%")).getVarchar());
        Assert.assertEquals("foo",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("foo")).getVarchar());
        Assert.assertEquals("g",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%g")).getVarchar());
        Assert.assertEquals("4",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%4")).getVarchar());
        Assert.assertEquals("2001 02",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("%x %v")).getVarchar());
        Assert.assertEquals("yyyy",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyy")).getVarchar());
        Assert.assertEquals("20010109",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyyMMdd")).getVarchar());
        Assert.assertEquals("yyyyMMdd HH:mm:ss",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyyMMdd HH:mm:ss"))
                        .getVarchar());
        Assert.assertEquals("HH:mm:ss",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("HH:mm:ss")).getVarchar());
        Assert.assertEquals("2001-01-09",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyy-MM-dd"))
                        .getVarchar());
        Assert.assertEquals("2001-01-09 13:04:05",
                ScalarOperatorFunctions.dateFormat(testDate, ConstantOperator.createVarchar("yyyy-MM-dd HH:mm:ss"))
                        .getVarchar());

        Assert.assertEquals("2001-01-09",
                ScalarOperatorFunctions.dateFormat(ConstantOperator.createDate(LocalDateTime.of(2001, 1, 9, 13, 4, 5)),
                        ConstantOperator.createVarchar("%Y-%m-%d"))
                        .getVarchar());
    }

    @Test
    public void dateParse() throws AnalysisException {
        Assert.assertEquals("2013-05-10T00:00", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2013,05,10"), ConstantOperator.createVarchar("%Y,%m,%d"))
                .getDatetime().toString());
        Assert.assertEquals("2013-05-17T00:35:10", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2013-05-17 12:35:10"),
                        ConstantOperator.createVarchar("%Y-%m-%d %h:%i:%s")).getDatetime().toString());
        Assert.assertEquals("2013-05-17T00:35:10", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2013-05-17 00:35:10"),
                        ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")).getDatetime().toString());
        Assert.assertEquals("2013-05-17T23:35:10", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("abc 2013-05-17 fff 23:35:10 xyz"),
                        ConstantOperator.createVarchar("abc %Y-%m-%d fff %H:%i:%s xyz")).getDatetime().toString());
        Assert.assertEquals("2019-05-09T00:00", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("2019,129"), ConstantOperator.createVarchar("%Y,%j"))
                .getDatetime().toString());
        Assert.assertEquals("2019-05-09T12:10:45", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("12:10:45-20190509"),
                        ConstantOperator.createVarchar("%T-%Y%m%d")).getDatetime().toString());
        Assert.assertEquals("2019-05-09T09:10:45", ScalarOperatorFunctions
                .dateParse(ConstantOperator.createVarchar("20190509-9:10:45"),
                        ConstantOperator.createVarchar("%Y%m%d-%k:%i:%S")).getDatetime().toString());
    }

    @Test
    public void yearsSub() throws AnalysisException {
        assertEquals(FEFunctions.yearsSub(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.yearsSub(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void monthsSub() throws AnalysisException {
        assertEquals(FEFunctions.monthsSub(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.monthsSub(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void daysSub() throws AnalysisException {
        assertEquals(FEFunctions.daysSub(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.daysSub(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void hoursSub() throws AnalysisException {
        assertEquals(FEFunctions.hoursSub(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.hoursSub(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void minutesSub() throws AnalysisException {
        assertEquals(FEFunctions.minutesSub(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.minutesSub(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void secondsSub() throws AnalysisException {
        assertEquals(FEFunctions.secondsSub(L_DT_20150323_092355, L_INT_10).toLocalDateTime(),
                ScalarOperatorFunctions.secondsSub(O_DT_20150323_092355, O_INT_10).getDatetime());
    }

    @Test
    public void year() {
        ConstantOperator date = ConstantOperator.createDatetime(LocalDateTime.of(2000, 10, 21, 12, 0));
        ConstantOperator result = ScalarOperatorFunctions.year(date);

        assertEquals(Type.INT, result.getType());
        assertEquals(2000, result.getInt());
    }

    @Test
    public void month() throws AnalysisException {
        assertEquals(FEFunctions.month(L_DT_20150323_092355).getLongValue(),
                ScalarOperatorFunctions.month(O_DT_20150323_092355).getInt());
    }

    @Test
    public void day() throws AnalysisException {
        assertEquals(FEFunctions.day(L_DT_20150323_092355).getLongValue(),
                ScalarOperatorFunctions.day(O_DT_20150323_092355).getInt());
    }

    @Test
    public void unixTimestamp() throws AnalysisException {
        DateLiteral dl = new DateLiteral(2050, 3, 23, 9, 23, 55);
        ConstantOperator codt = ConstantOperator.createDatetime(LocalDateTime.of(2050, 3, 23, 9, 23, 55));

        assertEquals(FEFunctions.unixTimestamp(dl).getLongValue(),
                ScalarOperatorFunctions.unixTimestamp(codt).getInt());
        assertEquals(FEFunctions.unixTimestamp(L_DT_20150323_092355).getLongValue(),
                ScalarOperatorFunctions.unixTimestamp(O_DT_20150323_092355).getInt());
    }

    @Test
    public void fromUnixTime() throws AnalysisException {
        assertEquals(FEFunctions.fromUnixTime(L_INT_10).getStringValue(),
                ScalarOperatorFunctions.fromUnixTime(O_INT_10).getVarchar());
    }

    @Test
    public void curDate() throws AnalysisException {
        assertEquals(FEFunctions.curDate().toLocalDateTime().truncatedTo(ChronoUnit.DAYS),
                ScalarOperatorFunctions.curDate().getDate());
    }

    @Test
    public void floor() throws AnalysisException {
        assertEquals(FEFunctions.floor(L_FLOAT_100).getLongValue(),
                ScalarOperatorFunctions.floor(O_FLOAT_100).getBigint());
    }

    @Test
    public void addInt() throws AnalysisException {
        assertEquals(FEFunctions.addInt(L_BI_100, L_BI_100).getLongValue(),
                ScalarOperatorFunctions.addBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void addDouble() throws AnalysisException {
        assertEquals(FEFunctions.addDouble(L_DOUBLE_100, L_DOUBLE_100).getDoubleValue(),
                ScalarOperatorFunctions.addDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void addDecimal() throws AnalysisException {
        assertEquals(FEFunctions.addDecimal(L_DECIMAL_100, L_DECIMAL_100).getStringValue(),
                ScalarOperatorFunctions.addDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toPlainString());
        assertEquals(FEFunctions.addDecimal(L_DECIMAL32P7S2_100, L_DECIMAL32P7S2_100).getStringValue(),
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toPlainString());
        assertTrue(
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getType().isDecimalV3());

        assertEquals(FEFunctions.addDecimal(L_DECIMAL32P9S0_100, L_DECIMAL32P9S0_100).getStringValue(),
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toPlainString());
        assertTrue(
                ScalarOperatorFunctions.addDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getType().isDecimalV3());

        assertEquals(FEFunctions.addDecimal(L_DECIMAL64P15S10_100, L_DECIMAL64P15S10_100).getStringValue(),
                ScalarOperatorFunctions.addDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getType()
                .isDecimalV3());

        assertEquals(FEFunctions.addDecimal(L_DECIMAL64P18S15_100, L_DECIMAL64P18S15_100).getStringValue(),
                ScalarOperatorFunctions.addDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getType()
                .isDecimalV3());

        assertEquals(FEFunctions.addDecimal(L_DECIMAL128P30S2_100, L_DECIMAL128P30S2_100).getStringValue(),
                ScalarOperatorFunctions.addDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getType()
                .isDecimalV3());

        assertEquals(FEFunctions.addDecimal(L_DECIMAL128P38S20_100, L_DECIMAL128P38S20_100).getStringValue(),
                ScalarOperatorFunctions.addDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toPlainString());
        assertTrue(ScalarOperatorFunctions.addDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void addBigInt() throws AnalysisException {
        assertEquals(FEFunctions.addBigInt(L_LI_100, L_LI_100).getStringValue(),
                ScalarOperatorFunctions.addLargeInt(O_LI_100, O_LI_100).getLargeInt().toString());
    }

    @Test
    public void subtractInt() throws AnalysisException {
        assertEquals(FEFunctions.subtractInt(L_BI_100, L_BI_100).getLongValue(),
                ScalarOperatorFunctions.subtractBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void subtractDouble() throws AnalysisException {
        assertEquals(FEFunctions.subtractDouble(L_DOUBLE_100, L_DOUBLE_100).getDoubleValue(),
                ScalarOperatorFunctions.subtractDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void subtractDecimal() throws AnalysisException {
        assertEquals(FEFunctions.subtractDecimal(L_DECIMAL_100, L_DECIMAL_100).getStringValue(),
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toString());
        assertEquals(FEFunctions.subtractDecimal(L_DECIMAL32P7S2_100, L_DECIMAL32P7S2_100).getStringValue(),
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toString());
        assertEquals(FEFunctions.subtractDecimal(L_DECIMAL32P9S0_100, L_DECIMAL32P9S0_100).getStringValue(),
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toString());
        assertEquals(FEFunctions.subtractDecimal(L_DECIMAL64P15S10_100, L_DECIMAL64P15S10_100).getStringValue(),
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toString());
        assertEquals(FEFunctions.subtractDecimal(L_DECIMAL64P18S15_100, L_DECIMAL64P18S15_100).getStringValue(),
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toString());
        assertEquals(FEFunctions.subtractDecimal(L_DECIMAL128P30S2_100, L_DECIMAL128P30S2_100).getStringValue(),
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toString());
        assertEquals(FEFunctions.subtractDecimal(L_DECIMAL128P38S20_100, L_DECIMAL128P38S20_100).getStringValue(),
                ScalarOperatorFunctions.subtractDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toString());

        assertTrue(ScalarOperatorFunctions.subtractDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void subtractBigInt() throws AnalysisException {
        assertEquals(FEFunctions.subtractBigInt(L_LI_100, L_LI_100).getStringValue(),
                ScalarOperatorFunctions.subtractLargeInt(O_LI_100, O_LI_100).getLargeInt().toString());
    }

    @Test
    public void multiplyInt() throws AnalysisException {
        assertEquals(FEFunctions.multiplyInt(L_BI_100, L_BI_100).getLongValue(),
                ScalarOperatorFunctions.multiplyBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void multiplyDouble() throws AnalysisException {
        assertEquals(FEFunctions.multiplyDouble(L_DOUBLE_100, L_DOUBLE_100).getDoubleValue(),
                ScalarOperatorFunctions.multiplyDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void multiplyDecimal() throws AnalysisException {
        assertEquals(FEFunctions.multiplyDecimal(L_DECIMAL_100, L_DECIMAL_100).getStringValue(),
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toPlainString());
        assertEquals(FEFunctions.multiplyDecimal(L_DECIMAL32P7S2_100, L_DECIMAL32P7S2_100).getStringValue(),
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toPlainString());
        assertEquals(FEFunctions.multiplyDecimal(L_DECIMAL32P9S0_100, L_DECIMAL32P9S0_100).getStringValue(),
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toPlainString());
        assertEquals(FEFunctions.multiplyDecimal(L_DECIMAL64P15S10_100, L_DECIMAL64P15S10_100).getStringValue(),
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toPlainString());
        assertEquals(FEFunctions.multiplyDecimal(L_DECIMAL64P18S15_100, L_DECIMAL64P18S15_100).getStringValue(),
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toPlainString());
        assertEquals(FEFunctions.multiplyDecimal(L_DECIMAL128P30S2_100, L_DECIMAL128P30S2_100).getStringValue(),
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toPlainString());
        assertEquals(FEFunctions.multiplyDecimal(L_DECIMAL128P38S20_100, L_DECIMAL128P38S20_100).getStringValue(),
                ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toPlainString());

        assertTrue(ScalarOperatorFunctions.multiplyDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void multiplyBigInt() throws AnalysisException {
        assertEquals(FEFunctions.multiplyBigInt(L_LI_100, L_LI_100).getStringValue(),
                ScalarOperatorFunctions.multiplyLargeInt(O_LI_100, O_LI_100).getLargeInt().toString());
    }

    @Test
    public void divideDouble() throws AnalysisException {
        assertEquals(Objects.requireNonNull(FEFunctions.divideDouble(L_DOUBLE_100, L_DOUBLE_100)).getDoubleValue(),
                ScalarOperatorFunctions.divideDouble(O_DOUBLE_100, O_DOUBLE_100).getDouble(), 1);
    }

    @Test
    public void divideDecimal() throws AnalysisException {
        assertEquals(Objects.requireNonNull(FEFunctions.divideDecimal(L_DECIMAL_100, L_DECIMAL_100)).getStringValue(),
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toString());
        assertEquals(Objects.requireNonNull(FEFunctions.divideDecimal(L_DECIMAL32P7S2_100, L_DECIMAL32P7S2_100))
                        .getStringValue(),
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.divideDecimal(L_DECIMAL32P9S0_100, L_DECIMAL32P9S0_100))
                        .getStringValue(),
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.divideDecimal(L_DECIMAL64P15S10_100, L_DECIMAL64P15S10_100))
                        .getStringValue(),
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.divideDecimal(L_DECIMAL64P18S15_100, L_DECIMAL64P18S15_100))
                        .getStringValue(),
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.divideDecimal(L_DECIMAL128P30S2_100, L_DECIMAL128P30S2_100))
                        .getStringValue(),
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.divideDecimal(L_DECIMAL128P38S20_100, L_DECIMAL128P38S20_100))
                        .getStringValue(),
                ScalarOperatorFunctions.divideDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toString());

        assertTrue(ScalarOperatorFunctions.divideDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());

    }

    @Test
    public void modInt() throws AnalysisException {
        assertEquals(Objects.requireNonNull(FEFunctions.modInt(L_BI_100, L_BI_100)).getLongValue(),
                ScalarOperatorFunctions.modBigInt(O_BI_100, O_BI_100).getBigint());
    }

    @Test
    public void modDecimal() throws AnalysisException {
        assertEquals(Objects.requireNonNull(FEFunctions.modDecimal(L_DECIMAL_100, L_DECIMAL_100)).getStringValue(),
                ScalarOperatorFunctions.modDecimal(O_DECIMAL_100, O_DECIMAL_100).getDecimal().toString());
        assertEquals(Objects.requireNonNull(FEFunctions.modDecimal(L_DECIMAL32P7S2_100, L_DECIMAL32P7S2_100))
                        .getStringValue(),
                ScalarOperatorFunctions.modDecimal(O_DECIMAL32P7S2_100, O_DECIMAL32P7S2_100).getDecimal().toString());
        assertEquals(Objects.requireNonNull(FEFunctions.modDecimal(L_DECIMAL32P9S0_100, L_DECIMAL32P9S0_100))
                        .getStringValue(),
                ScalarOperatorFunctions.modDecimal(O_DECIMAL32P9S0_100, O_DECIMAL32P9S0_100).getDecimal().toString());
        assertEquals(Objects.requireNonNull(FEFunctions.modDecimal(L_DECIMAL64P15S10_100, L_DECIMAL64P15S10_100))
                        .getStringValue(),
                ScalarOperatorFunctions.modDecimal(O_DECIMAL64P15S10_100, O_DECIMAL64P15S10_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.modDecimal(L_DECIMAL64P18S15_100, L_DECIMAL64P18S15_100))
                        .getStringValue(),
                ScalarOperatorFunctions.modDecimal(O_DECIMAL64P18S15_100, O_DECIMAL64P18S15_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.modDecimal(L_DECIMAL128P30S2_100, L_DECIMAL128P30S2_100))
                        .getStringValue(),
                ScalarOperatorFunctions.modDecimal(O_DECIMAL128P30S2_100, O_DECIMAL128P30S2_100).getDecimal()
                        .toString());
        assertEquals(Objects.requireNonNull(FEFunctions.modDecimal(L_DECIMAL128P38S20_100, L_DECIMAL128P38S20_100))
                        .getStringValue(),
                ScalarOperatorFunctions.modDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getDecimal()
                        .toString());

        assertTrue(ScalarOperatorFunctions.modDecimal(O_DECIMAL128P38S20_100, O_DECIMAL128P38S20_100).getType()
                .isDecimalV3());
    }

    @Test
    public void modLargeInt() throws AnalysisException {
        assertEquals(Objects.requireNonNull(FEFunctions.modLargeInt(L_LI_100, L_LI_100)).getStringValue(),
                ScalarOperatorFunctions.modLargeInt(O_LI_100, O_LI_100).getLargeInt().toString());
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
    public void fromUnixTime2() throws AnalysisException {
        ConstantOperator date =
                ScalarOperatorFunctions.fromUnixTime(O_INT_10, ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s"));
        assertTrue(date.toString().matches("1970-01-01 0.*:00:10"));
    }
}
