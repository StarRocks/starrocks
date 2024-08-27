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

package com.starrocks.connector.delta;

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScalarOperationToDeltaLakeExprTest {
    private StructType schema;
    private Set<String> partitionColumns;
    ColumnRefOperator cBoolCol = new ColumnRefOperator(1, Type.BOOLEAN, "c_bool", true, false);
    ColumnRefOperator cShortCol = new ColumnRefOperator(1, Type.SMALLINT, "c_short", true, false);
    ColumnRefOperator cIntCol = new ColumnRefOperator(1, Type.INT, "c_int", true, false);
    ColumnRefOperator cLongCol = new ColumnRefOperator(1, Type.BIGINT, "c_long", true, false);
    ColumnRefOperator cFloatCol = new ColumnRefOperator(1, Type.FLOAT, "c_float", true, false);
    ColumnRefOperator cDoubleCol = new ColumnRefOperator(1, Type.DOUBLE, "c_double", true, false);
    ColumnRefOperator cDateCol = new ColumnRefOperator(1, Type.DATE, "c_date", true, false);
    ColumnRefOperator cTimestampCol = new ColumnRefOperator(1, Type.DATETIME, "c_timestamp", true, false);
    ColumnRefOperator cTimestampNTZCol = new ColumnRefOperator(1, Type.DATETIME, "c_timestamp_ntz", true, false);
    ColumnRefOperator cCharCol = new ColumnRefOperator(1, Type.CHAR, "c_string", true, false);
    ColumnRefOperator cVarcharCol = new ColumnRefOperator(1, Type.VARCHAR, "c_string", true, false);
    ColumnRefOperator cHLLCol = new ColumnRefOperator(1, Type.VARCHAR, "c_string", true, false);

    Column cDeltaBoolCol = new Column("c_bool");
    Column cDeltaShortCol = new Column("c_short");
    Column cDeltaIntCol = new Column("c_int");
    Column cDeltaLongCol = new Column("c_long");
    Column cDeltaFloatCol = new Column("c_float");
    Column cDeltaDoubleCol = new Column("c_double");
    Column cDeltaDateCol = new Column("c_date");
    Column cDeltaTimestampCol = new Column("c_timestamp");
    Column cDeltaTimestampNTZCol = new Column("c_timestamp_ntz");
    Column cDeltaCharCol = new Column("c_string");
    Column cDeltaVarcharCol = new Column("c_string");
    Column cDeltaHLLCol = new Column("c_string");

    @Before
    public void setUp() throws Exception {
        schema = new StructType()
                .add("c_int", IntegerType.INTEGER)
                .add("c_array", new ArrayType(IntegerType.INTEGER, true))
                .add("c_binary", io.delta.kernel.types.BinaryType.BINARY)
                .add("c_bool", BooleanType.BOOLEAN)
                .add("c_string", StringType.STRING)
                .add("c_byte", ByteType.BYTE)
                .add("c_date", DateType.DATE)
                .add("c_decimal", new DecimalType(10, 2))
                .add("c_double", DoubleType.DOUBLE)
                .add("c_float", FloatType.FLOAT)
                .add("c_long", LongType.LONG)
                .add("c_map", new MapType(IntegerType.INTEGER, IntegerType.INTEGER, true))
                .add("c_short", ShortType.SHORT)
                .add("c_string", StringType.STRING)
                .add("c_struct", new StructType().add(new StructField("c_sub_1", IntegerType.INTEGER, true)))
                .add("c_timestamp_ntz", TimestampNTZType.TIMESTAMP_NTZ)
                .add("c_timestamp", TimestampType.TIMESTAMP);
        partitionColumns = new HashSet<>();
        partitionColumns.add("c_timestamp");
    }

    @Test
    public void testConvertCompoundPredicate() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        ConstantOperator value1 = ConstantOperator.createInt(5);
        ConstantOperator value2 = ConstantOperator.createInt(10);
        Literal literal1 = Literal.ofInt(5);
        Literal literal2 = Literal.ofInt(10);
        List<ScalarOperator> operators;

        // and
        ScalarOperator gtOperator = new BinaryPredicateOperator(BinaryType.GT, cIntCol, value1);
        ScalarOperator ltOperator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value2);
        CompoundPredicateOperator operator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                gtOperator, ltOperator);
        operators = new ArrayList<>(List.of(operator));
        Predicate convertExpr = converter.convert(operators, context);
        Predicate expectedExpr = new And(new Predicate(">", cDeltaIntCol, literal1),
                new Predicate("<", cDeltaIntCol, literal2));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // or
        ltOperator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value1);
        gtOperator = new BinaryPredicateOperator(BinaryType.GT, cIntCol, value2);
        operator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, ltOperator, gtOperator);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Or(new Predicate("<", cDeltaIntCol, literal1), new Predicate(">", cDeltaIntCol, literal2));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // NOT
        ltOperator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value1);
        operator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, ltOperator);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("NOT", new Predicate("<", cDeltaIntCol, literal1));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }

    @Test
    public void testConvertIsNullPredicate() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        List<ScalarOperator> operators;

        // is null
        IsNullPredicateOperator isNullPredicateOperator = new IsNullPredicateOperator(false, cIntCol);
        operators = new ArrayList<>(List.of(isNullPredicateOperator));
        Predicate convertExpr = converter.convert(operators, context);
        Predicate expectedExpr = new Predicate("IS_NULL", cDeltaIntCol);
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // is not null
        isNullPredicateOperator = new IsNullPredicateOperator(true, cIntCol);
        operators = new ArrayList<>(List.of(isNullPredicateOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("IS_NOT_NULL", cDeltaIntCol);
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }

    @Test
    public void testConvertCastLitervalValue() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        List<ScalarOperator> operators;
        ConstantOperator value;
        ScalarOperator operator;

        // int -> boolean
        value = ConstantOperator.createInt(0);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cBoolCol, value);
        operators = new ArrayList<>(List.of(operator));
        Predicate convertExpr = converter.convert(operators, context);
        Predicate expectedExpr = new Predicate("=", cDeltaBoolCol, Literal.ofBoolean(false));
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // int -> smallint
        value = ConstantOperator.createInt(5);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cShortCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaShortCol, Literal.ofShort((short) 5));
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // bigint -> int
        value = ConstantOperator.createBigint(5);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cIntCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaIntCol, Literal.ofLong(5));
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // int -> bigint
        value = ConstantOperator.createInt(5);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cLongCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaLongCol, Literal.ofInt(5));
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // string -> date
        value = ConstantOperator.createVarchar("2023-01-05");
        operator = new BinaryPredicateOperator(BinaryType.EQ, cDateCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaDateCol, Literal.ofDate(19362));
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // string -> datetime
        value = ConstantOperator.createVarchar("2023-01-05 01:01:01");
        operator = new BinaryPredicateOperator(BinaryType.EQ, cTimestampCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaTimestampCol, Literal.ofTimestamp(1672851661000000L));
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // date -> string
        value = ConstantOperator.createDate(LocalDateTime.of(2023, 1, 5, 0, 0));
        operator = new BinaryPredicateOperator(BinaryType.EQ, cVarcharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaVarcharCol, Literal.ofString("2023-01-05"));
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // int -> string (not supported)
        value = ConstantOperator.createInt(12345);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cVarcharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = AlwaysTrue.ALWAYS_TRUE;
        Assert.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // not supported
        value = ConstantOperator.createInt(12345);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cDoubleCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        Assert.assertEquals(AlwaysTrue.ALWAYS_TRUE.toString(), convertExpr.toString());
    }

    @Test
    public void testConvertTimestampToNtz() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, partitionColumns);

        LocalDateTime localDateTime = LocalDateTime.now();
        ConstantOperator value = ConstantOperator.createDatetime(localDateTime);
        ScalarOperator operator = new BinaryPredicateOperator(BinaryType.LT, cTimestampCol, value);
        List<ScalarOperator> operators = new ArrayList<>(List.of(operator));
        Predicate convertExpr = converter.convert(operators, context);
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toEpochSecond() * 1000 * 1000
                + localDateTime.getNano() / 1000;
        Predicate expectedExpr = new Predicate("<", cDeltaTimestampCol, Literal.ofTimestamp(timestamp));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }

    @Test
    public void testConvertConstType() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        ScalarOperator operator;
        List<ScalarOperator> operators;
        Predicate convertExpr;
        Predicate expectedExpr;
        ConstantOperator value;

        // Boolean
        value = ConstantOperator.createBoolean(true);
        operator = new BinaryPredicateOperator(BinaryType.LT, cBoolCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaBoolCol, Literal.ofBoolean(true));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // Tinyint
        value = ConstantOperator.createTinyInt((byte) 5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cShortCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaShortCol, Literal.ofShort((short) 5));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // Smallint
        value = ConstantOperator.createSmallInt((short) 5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cShortCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaShortCol, Literal.ofShort((short) 5));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // int
        value = ConstantOperator.createInt(5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaIntCol, Literal.ofInt(5));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // bigint
        value = ConstantOperator.createBigint(5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cLongCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaLongCol, Literal.ofLong(5));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // float
        value = ConstantOperator.createFloat(5.5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cFloatCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaFloatCol, Literal.ofFloat((float) 5.5));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // double
        value = ConstantOperator.createDouble(5.5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cDoubleCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaDoubleCol, Literal.ofFloat((float) 5.5));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // date
        LocalDateTime localDateTime = LocalDateTime.now();
        value = ConstantOperator.createDate(localDateTime);
        operator = new BinaryPredicateOperator(BinaryType.LT, cDateCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaDateCol, Literal.ofDate((int) localDateTime.toLocalDate().toEpochDay()));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // datetime (timestamp)
        localDateTime = LocalDateTime.now();
        value = ConstantOperator.createDatetime(localDateTime);
        operator = new BinaryPredicateOperator(BinaryType.LT, cTimestampCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        ZoneId zoneId = TimeUtils.getTimeZone().toZoneId();
        long timestamp = localDateTime.atZone(zoneId).toEpochSecond() * 1000 * 1000 + localDateTime.getNano() / 1000;
        expectedExpr = new Predicate("<", cDeltaTimestampCol, Literal.ofTimestamp(timestamp));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // datetime (timestamp_ntz)
        localDateTime = LocalDateTime.now();
        value = ConstantOperator.createDatetime(localDateTime);
        operator = new BinaryPredicateOperator(BinaryType.LT, cTimestampNTZCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        timestamp = localDateTime.atZone(ZoneOffset.UTC).toEpochSecond() * 1000 * 1000 + localDateTime.getNano() / 1000;
        expectedExpr = new Predicate("<", cDeltaTimestampNTZCol, Literal.ofTimestamp(timestamp));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // varchar
        value = ConstantOperator.createVarchar("12345");
        operator = new BinaryPredicateOperator(BinaryType.LT, cVarcharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaVarcharCol, Literal.ofString("12345"));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // char
        value = ConstantOperator.createChar("12345");
        operator = new BinaryPredicateOperator(BinaryType.LT, cCharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaCharCol, Literal.ofString("12345"));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // hll
        value = ConstantOperator.createObject("12345", Type.HLL);
        operator = new BinaryPredicateOperator(BinaryType.LT, cHLLCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaHLLCol, Literal.ofString("12345"));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }

    @Test
    public void testConvertBinaryPredicate() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        Literal literal = Literal.ofInt(5);
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        List<ScalarOperator> operators;

        // <
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator ltOperator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value);
        operators = new ArrayList<>(List.of(ltOperator));
        Predicate convertExpr = converter.convert(operators, context);
        Predicate expectedExpr = new Predicate("<", cDeltaIntCol, literal);
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // <=
        ScalarOperator leOperator = new BinaryPredicateOperator(BinaryType.LE, cIntCol, value);
        operators = new ArrayList<>(List.of(leOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<=", cDeltaIntCol, literal);
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // >
        ScalarOperator gtOperator = new BinaryPredicateOperator(BinaryType.GT, cIntCol, value);
        operators = new ArrayList<>(List.of(gtOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate(">", cDeltaIntCol, literal);
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // >=
        ScalarOperator geOperator = new BinaryPredicateOperator(BinaryType.GE, cIntCol, value);
        operators = new ArrayList<>(List.of(geOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate(">=", cDeltaIntCol, literal);
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // ==
        ScalarOperator eqOperator = new BinaryPredicateOperator(BinaryType.EQ, cIntCol, value);
        operators = new ArrayList<>(List.of(eqOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaIntCol, literal);
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // !=
        ScalarOperator neOperator = new BinaryPredicateOperator(BinaryType.NE, cIntCol, value);
        operators = new ArrayList<>(List.of(neOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("NOT", new Predicate("=", cDeltaIntCol, literal));
        Assert.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }
}