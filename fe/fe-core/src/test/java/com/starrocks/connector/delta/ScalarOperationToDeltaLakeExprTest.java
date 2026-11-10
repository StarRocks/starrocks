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

import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.type.CharType;
import com.starrocks.type.HLLType;
import com.starrocks.type.VarcharType;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScalarOperationToDeltaLakeExprTest {
    private StructType schema;
    private Set<String> partitionColumns;
    ColumnRefOperator cBoolCol = new ColumnRefOperator(1, com.starrocks.type.BooleanType.BOOLEAN, "c_bool", true, false);
    ColumnRefOperator cShortCol = new ColumnRefOperator(1, com.starrocks.type.IntegerType.SMALLINT, "c_short", true, false);
    ColumnRefOperator cIntCol = new ColumnRefOperator(1, com.starrocks.type.IntegerType.INT, "c_int", true, false);
    ColumnRefOperator cLongCol = new ColumnRefOperator(1, com.starrocks.type.IntegerType.BIGINT, "c_long", true, false);
    ColumnRefOperator cFloatCol = new ColumnRefOperator(1, com.starrocks.type.FloatType.FLOAT, "c_float", true, false);
    ColumnRefOperator cDoubleCol = new ColumnRefOperator(1, com.starrocks.type.FloatType.DOUBLE, "c_double", true, false);
    ColumnRefOperator cDateCol = new ColumnRefOperator(1, com.starrocks.type.DateType.DATE, "c_date", true, false);
    ColumnRefOperator cTimestampCol = new ColumnRefOperator(1, com.starrocks.type.DateType.DATETIME, "c_timestamp", true, false);
    ColumnRefOperator cTimestampNTZCol = new ColumnRefOperator(1, com.starrocks.type.DateType.DATETIME,
            "c_timestamp_ntz", true, false);
    ColumnRefOperator cCharCol = new ColumnRefOperator(1, CharType.CHAR, "c_string", true, false);
    ColumnRefOperator cVarcharCol = new ColumnRefOperator(1, VarcharType.VARCHAR, "c_string", true, false);
    ColumnRefOperator cHLLCol = new ColumnRefOperator(1, VarcharType.VARCHAR, "c_string", true, false);

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

    @BeforeEach
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
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // or
        ltOperator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value1);
        gtOperator = new BinaryPredicateOperator(BinaryType.GT, cIntCol, value2);
        operator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, ltOperator, gtOperator);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Or(new Predicate("<", cDeltaIntCol, literal1), new Predicate(">", cDeltaIntCol, literal2));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // NOT
        ltOperator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value1);
        operator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, ltOperator);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("NOT", new Predicate("<", cDeltaIntCol, literal1));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }

    @Test
    public void testConvertInPredicate() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());

        // c_int IN (1, 2, 3) becomes an OR of equalities, which the kernel's data skipping understands.
        ScalarOperator in = new InPredicateOperator(false, cIntCol, ConstantOperator.createInt(1),
                ConstantOperator.createInt(2), ConstantOperator.createInt(3));
        Predicate expected = new Or(new Predicate("=", cDeltaIntCol, Literal.ofInt(1)),
                new Or(new Predicate("=", cDeltaIntCol, Literal.ofInt(2)),
                        new Predicate("=", cDeltaIntCol, Literal.ofInt(3))));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(in), context).toString());

        // A single value is a plain equality.
        in = new InPredicateOperator(false, cDateCol, ConstantOperator.createVarchar("2020-01-01"));
        expected = new Predicate("=", cDeltaDateCol,
                Literal.ofDate((int) java.time.LocalDate.of(2020, 1, 1).toEpochDay()));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(in), context).toString());

        // One value that cannot become a literal stops the whole IN from being pushed down.
        in = new InPredicateOperator(false, cIntCol, ConstantOperator.createInt(1),
                ConstantOperator.createVarchar("abc"));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(in), context));

        // NOT IN is not pushed down.
        in = new InPredicateOperator(true, cIntCol, ConstantOperator.createInt(1), ConstantOperator.createInt(2));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(in), context));

        // Nor is an over-long list.
        List<ScalarOperator> args = new ArrayList<>();
        args.add(cIntCol);
        for (int i = 0; i <= ScalarOperationToDeltaLakeExpr.MAX_IN_LIST_SIZE; i++) {
            args.add(ConstantOperator.createInt(i));
        }
        in = new InPredicateOperator(false, args);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(in), context));
        args.remove(args.size() - 1);
        in = new InPredicateOperator(false, args);
        Assertions.assertNotEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(in), context));
    }

    @Test
    public void testPartialAndPushdown() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        ScalarOperator convertible = new BinaryPredicateOperator(BinaryType.GT, cIntCol, ConstantOperator.createInt(5));
        // c_int = 'abc' cannot be converted.
        ScalarOperator unconvertible = new BinaryPredicateOperator(BinaryType.EQ, cIntCol,
                ConstantOperator.createVarchar("abc"));
        Predicate gt = new Predicate(">", cDeltaIntCol, Literal.ofInt(5));

        // AND with one convertible side pushes that side, on either side.
        ScalarOperator and = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                convertible, unconvertible);
        Assertions.assertEquals(gt.toString(), converter.convert(List.of(and), context).toString());
        and = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, unconvertible, convertible);
        Assertions.assertEquals(gt.toString(), converter.convert(List.of(and), context).toString());

        // Nested under OR it still widens the OR safely.
        ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ, cLongCol, ConstantOperator.createBigint(7));
        ScalarOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, and, eq);
        Predicate expected = new Or(gt, new Predicate("=", cDeltaLongCol, Literal.ofLong(7)));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(or), context).toString());

        // OR is never pushed down partially.
        ScalarOperator orUnconvertible = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                convertible, unconvertible);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(orUnconvertible), context));

        // Under NOT, NOT(AND(a, b)) must not become NOT(a).
        ScalarOperator not = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, and);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(not), context));
        ScalarOperator notOr = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, or);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(notOr), context));

        // NOT state does not leak to the next conjunct.
        Assertions.assertEquals(gt.toString(), converter.convert(List.of(not, and), context).toString());

        // Neither side convertible.
        and = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, unconvertible, unconvertible);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(and), context));
    }

    @Test
    public void testSubfieldNotBoundToDottedTopLevelColumn() {
        // A struct s with field a, and a separate top-level partition column literally named "s.a".
        StructType dottedSchema = new StructType()
                .add("s", new StructType().add("a", IntegerType.INTEGER).add("b", StringType.STRING))
                .add("s.a", IntegerType.INTEGER)
                .add("s.b", StringType.STRING);
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(dottedSchema, Set.of("s.a", "s.b"));
        ColumnRefOperator structCol = new ColumnRefOperator(20, com.starrocks.type.AnyStructType.ANY_STRUCT, "s",
                true, false);
        ScalarOperator subA = new SubfieldOperator(structCol, com.starrocks.type.IntegerType.INT, List.of("a"));
        ScalarOperator subB = new SubfieldOperator(structCol, VarcharType.VARCHAR, List.of("b"));

        List<ScalarOperator> predicates = List.of(
                new BinaryPredicateOperator(BinaryType.EQ, subA, ConstantOperator.createInt(1)),
                new InPredicateOperator(false, subA, ConstantOperator.createInt(1), ConstantOperator.createInt(2)),
                new LikePredicateOperator(subB, ConstantOperator.createVarchar("abc%")));
        for (ScalarOperator predicate : predicates) {
            Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(predicate), context),
                    predicate.toString());
        }

        // The top-level column named "s.a" itself is still pushed down.
        ColumnRefOperator dottedCol = new ColumnRefOperator(21, com.starrocks.type.IntegerType.INT, "s.a", true,
                false);
        ScalarOperator in = new InPredicateOperator(false, dottedCol, ConstantOperator.createInt(1),
                ConstantOperator.createInt(2));
        Column column = new Column("s.a");
        Predicate expected = new Or(new Predicate("=", column, Literal.ofInt(1)),
                new Predicate("=", column, Literal.ofInt(2)));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(in), context).toString());
    }

    @Test
    public void testConvertLikePrefix() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext partitionContext =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, Set.of("c_string", "c_int"));

        ScalarOperator like = new LikePredicateOperator(cVarcharCol, ConstantOperator.createVarchar("abc%"));
        Predicate expected = new Predicate("STARTS_WITH", cDeltaVarcharCol, Literal.ofString("abc"));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(like), partitionContext).toString());

        // Only a non-empty prefix followed by a single trailing % is pushed down.
        for (String pattern : List.of("%", "abc", "%abc", "a%c%", "ab_%", "a\\%%", "abc%%")) {
            like = new LikePredicateOperator(cVarcharCol, ConstantOperator.createVarchar(pattern));
            Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(like), partitionContext),
                    pattern);
        }

        // REGEXP, NOT LIKE and non-string partition columns are not pushed down.
        like = new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, cVarcharCol,
                ConstantOperator.createVarchar("abc%"));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(like), partitionContext));
        like = new LikePredicateOperator(cVarcharCol, ConstantOperator.createVarchar("abc%"));
        ScalarOperator notLike = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, like);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(notLike), partitionContext));
        like = new LikePredicateOperator(cIntCol, ConstantOperator.createVarchar("1%"));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(like), partitionContext));

        // The kernel's data skipping ignores STARTS_WITH, so a non-partition column is not pushed down.
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        like = new LikePredicateOperator(cVarcharCol, ConstantOperator.createVarchar("abc%"));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(like), context));
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
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // is not null
        isNullPredicateOperator = new IsNullPredicateOperator(true, cIntCol);
        operators = new ArrayList<>(List.of(isNullPredicateOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("IS_NOT_NULL", cDeltaIntCol);
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }

    @Test
    public void testTruncatingCharCastIsNotPushedDown() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        ConstantOperator value = ConstantOperator.createVarchar("hello");
        ScalarOperator equality = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(new CharType(5), cVarcharCol), value);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(equality), context));

        ScalarOperator safeEquality = new BinaryPredicateOperator(BinaryType.EQ, cIntCol,
                ConstantOperator.createInt(5));
        Predicate expected = new Predicate("=", cDeltaIntCol, Literal.ofInt(5));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(equality, safeEquality), context).toString());

        // An implicit CHAR cast still changes the type, so it is not stripped either.
        ScalarOperator implicitEquality = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(new CharType(5), cVarcharCol, true), value);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(implicitEquality), context));
    }

    @Test
    public void testTypeChangingCastIsNotPushedDown() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        ConstantOperator ts = ConstantOperator.createDatetime(LocalDateTime.of(2020, 1, 1, 10, 0, 0));

        // cast(c_date AS datetime) < '2020-01-01 10:00:00' must not become c_date < 2020-01-01,
        // which would prune c_date = 2020-01-01 although it satisfies the original predicate.
        for (BinaryType op : List.of(BinaryType.LT, BinaryType.LE, BinaryType.EQ, BinaryType.GE, BinaryType.GT)) {
            ScalarOperator predicate = new BinaryPredicateOperator(op,
                    new CastOperator(com.starrocks.type.DateType.DATETIME, cDateCol), ts);
            Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(predicate), context), op.toString());
        }

        // cast(c_date AS varchar) < '2020-01-01 00:00:01' compares strings, so '2020-01-01' matches;
        // it must not become c_date < 2020-01-01.
        ScalarOperator toVarchar = new BinaryPredicateOperator(BinaryType.LT,
                new CastOperator(VarcharType.VARCHAR, cDateCol), ConstantOperator.createVarchar("2020-01-01 00:00:01"));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(toVarchar), context));

        // cast(c_long AS int) = 5 must not become c_long = 5: values overflowing int also satisfy it.
        ScalarOperator narrowing = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(com.starrocks.type.IntegerType.INT, cLongCol), ConstantOperator.createInt(5));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(narrowing), context));

        // The other conjuncts are still pushed down.
        ScalarOperator safeEquality = new BinaryPredicateOperator(BinaryType.EQ, cIntCol,
                ConstantOperator.createInt(5));
        Predicate expected = new Predicate("=", cDeltaIntCol, Literal.ofInt(5));
        Assertions.assertEquals(expected.toString(),
                converter.convert(List.of(narrowing, safeEquality), context).toString());

        // An identity cast is still stripped and pushed down.
        ScalarOperator identity = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(com.starrocks.type.IntegerType.INT, cIntCol), ConstantOperator.createInt(5));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(identity), context).toString());
    }

    @Test
    public void testTypeChangingCastInNestedAndCompoundPredicates() {
        ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
        ScalarOperator castToVarcharLt = new BinaryPredicateOperator(BinaryType.LT,
                new CastOperator(VarcharType.VARCHAR, cDateCol), ConstantOperator.createVarchar("2020-01-01 00:00:01"));
        ScalarOperator safeEquality = new BinaryPredicateOperator(BinaryType.EQ, cIntCol, ConstantOperator.createInt(5));
        Predicate expected = new Predicate("=", cDeltaIntCol, Literal.ofInt(5));

        // Nested casts: a type change anywhere in the chain blocks pushdown; a chain of identity casts does not.
        ScalarOperator nestedNarrowing = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(com.starrocks.type.IntegerType.INT,
                        new CastOperator(com.starrocks.type.IntegerType.BIGINT, cIntCol)),
                ConstantOperator.createInt(5));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(nestedNarrowing), context));
        ScalarOperator nestedIdentity = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(com.starrocks.type.IntegerType.INT,
                        new CastOperator(com.starrocks.type.IntegerType.INT, cIntCol)),
                ConstantOperator.createInt(5));
        Assertions.assertEquals(expected.toString(), converter.convert(List.of(nestedIdentity), context).toString());

        // OR: one side through a type-changing cast drops the whole disjunction.
        ScalarOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                castToVarcharLt, safeEquality);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(or), context));

        // NOT: negating the stripped predicate would prune the wrong side, so it is not pushed down either.
        ScalarOperator not = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, castToVarcharLt);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(not), context));

        // IS NULL / IS NOT NULL through a type-changing cast stay residual; through an identity cast they are pushed.
        for (boolean isNotNull : List.of(false, true)) {
            ScalarOperator castNull = new IsNullPredicateOperator(isNotNull,
                    new CastOperator(VarcharType.VARCHAR, cDateCol));
            Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, converter.convert(List.of(castNull), context));
            ScalarOperator identityNull = new IsNullPredicateOperator(isNotNull,
                    new CastOperator(com.starrocks.type.IntegerType.INT, cIntCol));
            Predicate expectedNull = new Predicate(isNotNull ? "IS_NOT_NULL" : "IS_NULL", cDeltaIntCol);
            Assertions.assertEquals(expectedNull.toString(),
                    converter.convert(List.of(identityNull), context).toString());
        }
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
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // int -> smallint
        value = ConstantOperator.createInt(5);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cShortCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaShortCol, Literal.ofShort((short) 5));
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // bigint -> int
        value = ConstantOperator.createBigint(5);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cIntCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaIntCol, Literal.ofLong(5));
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // int -> bigint
        value = ConstantOperator.createInt(5);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cLongCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaLongCol, Literal.ofInt(5));
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // string -> date
        value = ConstantOperator.createVarchar("2023-01-05");
        operator = new BinaryPredicateOperator(BinaryType.EQ, cDateCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaDateCol, Literal.ofDate(19362));
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // string -> datetime
        value = ConstantOperator.createVarchar("2023-01-05 01:01:01");
        operator = new BinaryPredicateOperator(BinaryType.EQ, cTimestampCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaTimestampCol, Literal.ofTimestamp(1672851661000000L));
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // date -> string
        value = ConstantOperator.createDate(LocalDateTime.of(2023, 1, 5, 0, 0));
        operator = new BinaryPredicateOperator(BinaryType.EQ, cVarcharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaVarcharCol, Literal.ofString("2023-01-05"));
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // int -> string (not supported)
        value = ConstantOperator.createInt(12345);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cVarcharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = AlwaysTrue.ALWAYS_TRUE;
        Assertions.assertEquals(expectedExpr.toString(), convertExpr.toString());

        // not supported
        value = ConstantOperator.createInt(12345);
        operator = new BinaryPredicateOperator(BinaryType.EQ, cDoubleCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE.toString(), convertExpr.toString());
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
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());
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
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // Tinyint
        value = ConstantOperator.createTinyInt((byte) 5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cShortCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaShortCol, Literal.ofShort((short) 5));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // Smallint
        value = ConstantOperator.createSmallInt((short) 5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cShortCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaShortCol, Literal.ofShort((short) 5));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // int
        value = ConstantOperator.createInt(5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cIntCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaIntCol, Literal.ofInt(5));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // bigint
        value = ConstantOperator.createBigint(5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cLongCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaLongCol, Literal.ofLong(5));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // float
        value = ConstantOperator.createFloat(5.5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cFloatCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaFloatCol, Literal.ofFloat((float) 5.5));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // double
        value = ConstantOperator.createDouble(5.5);
        operator = new BinaryPredicateOperator(BinaryType.LT, cDoubleCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaDoubleCol, Literal.ofFloat((float) 5.5));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // date
        LocalDateTime localDateTime = LocalDateTime.now();
        value = ConstantOperator.createDate(localDateTime);
        operator = new BinaryPredicateOperator(BinaryType.LT, cDateCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaDateCol, Literal.ofDate((int) localDateTime.toLocalDate().toEpochDay()));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // datetime (timestamp)
        localDateTime = LocalDateTime.now();
        value = ConstantOperator.createDatetime(localDateTime);
        operator = new BinaryPredicateOperator(BinaryType.LT, cTimestampCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        ZoneId zoneId = TimeUtils.getTimeZone().toZoneId();
        long timestamp = localDateTime.atZone(zoneId).toEpochSecond() * 1000 * 1000 + localDateTime.getNano() / 1000;
        expectedExpr = new Predicate("<", cDeltaTimestampCol, Literal.ofTimestamp(timestamp));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // datetime (timestamp_ntz)
        localDateTime = LocalDateTime.now();
        value = ConstantOperator.createDatetime(localDateTime);
        operator = new BinaryPredicateOperator(BinaryType.LT, cTimestampNTZCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        timestamp = localDateTime.atZone(ZoneOffset.UTC).toEpochSecond() * 1000 * 1000 + localDateTime.getNano() / 1000;
        expectedExpr = new Predicate("<", cDeltaTimestampNTZCol, Literal.ofTimestamp(timestamp));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // varchar
        value = ConstantOperator.createVarchar("12345");
        operator = new BinaryPredicateOperator(BinaryType.LT, cVarcharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaVarcharCol, Literal.ofString("12345"));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // char
        value = ConstantOperator.createChar("12345");
        operator = new BinaryPredicateOperator(BinaryType.LT, cCharCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaCharCol, Literal.ofString("12345"));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // hll
        value = ConstantOperator.createObject("12345", HLLType.HLL);
        operator = new BinaryPredicateOperator(BinaryType.LT, cHLLCol, value);
        operators = new ArrayList<>(List.of(operator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<", cDeltaHLLCol, Literal.ofString("12345"));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());
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
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // <=
        ScalarOperator leOperator = new BinaryPredicateOperator(BinaryType.LE, cIntCol, value);
        operators = new ArrayList<>(List.of(leOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("<=", cDeltaIntCol, literal);
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // >
        ScalarOperator gtOperator = new BinaryPredicateOperator(BinaryType.GT, cIntCol, value);
        operators = new ArrayList<>(List.of(gtOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate(">", cDeltaIntCol, literal);
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // >=
        ScalarOperator geOperator = new BinaryPredicateOperator(BinaryType.GE, cIntCol, value);
        operators = new ArrayList<>(List.of(geOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate(">=", cDeltaIntCol, literal);
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // ==
        ScalarOperator eqOperator = new BinaryPredicateOperator(BinaryType.EQ, cIntCol, value);
        operators = new ArrayList<>(List.of(eqOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("=", cDeltaIntCol, literal);
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());

        // !=
        ScalarOperator neOperator = new BinaryPredicateOperator(BinaryType.NE, cIntCol, value);
        operators = new ArrayList<>(List.of(neOperator));
        convertExpr = converter.convert(operators, context);
        expectedExpr = new Predicate("NOT", new Predicate("=", cDeltaIntCol, literal));
        Assertions.assertEquals(convertExpr.toString(), expectedExpr.toString());
    }

    /**
     * Evaluates a converted predicate on one value of c_timestamp (epoch micros); ALWAYS_TRUE keeps every file.
     */
    private static boolean eval(Expression expr, long micros) {
        Predicate p = (Predicate) expr;
        List<Expression> children = p.getChildren();
        switch (p.getName()) {
            case "ALWAYS_TRUE":
                return true;
            case "AND":
                return eval(children.get(0), micros) && eval(children.get(1), micros);
            case "OR":
                return eval(children.get(0), micros) || eval(children.get(1), micros);
            case "NOT":
                return !eval(children.get(0), micros);
            default:
                int cmp = Long.compare(micros, (Long) ((Literal) children.get(1)).getValue());
                return compare(p.getName(), cmp);
        }
    }

    private static boolean compare(String op, int cmp) {
        switch (op) {
            case "=":
                return cmp == 0;
            case "<":
                return cmp < 0;
            case "<=":
                return cmp <= 0;
            case ">":
                return cmp > 0;
            case ">=":
                return cmp >= 0;
            case "!=":
                return cmp != 0;
            default:
                throw new IllegalArgumentException(op);
        }
    }

    private static String sqlOp(BinaryType op) {
        switch (op) {
            case EQ:
                return "=";
            case NE:
                return "!=";
            case LT:
                return "<";
            case LE:
                return "<=";
            case GT:
                return ">";
            default:
                return ">=";
        }
    }

    /**
     * A non-partition TIMESTAMP constant is a local time in the session zone. Across a DST switch it names two
     * instants or none, and data skipping must still keep every file holding a matching value.
     */
    @Test
    public void testTimestampConstantAcrossDst() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("America/New_York");
        ctx.setThreadLocalInfo();
        try {
            ZoneId zone = ZoneId.of("America/New_York");
            ScalarOperationToDeltaLakeExpr converter = new ScalarOperationToDeltaLakeExpr();
            ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                    new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, new HashSet<>());
            List<Long> values = new ArrayList<>();
            List<LocalDateTime> constants = new ArrayList<>();
            for (List<String> range : List.of(List.of("2024-11-03T03:00:00Z", "2024-11-03T09:00:00Z"),
                    List.of("2024-03-10T04:00:00Z", "2024-03-10T10:00:00Z"))) {
                for (Instant t = Instant.parse(range.get(0)); t.isBefore(Instant.parse(range.get(1)));
                        t = t.plusSeconds(300)) {
                    values.add(ChronoUnit.MICROS.between(Instant.EPOCH, t));
                }
            }
            for (LocalDateTime c = LocalDateTime.of(2024, 11, 3, 0, 30);
                    c.isBefore(LocalDateTime.of(2024, 11, 3, 2, 45)); c = c.plusMinutes(15)) {
                constants.add(c);
            }
            for (LocalDateTime c = LocalDateTime.of(2024, 3, 10, 1, 30);
                    c.isBefore(LocalDateTime.of(2024, 3, 10, 3, 45)); c = c.plusMinutes(15)) {
                constants.add(c);
            }
            BinaryType[] ops = {BinaryType.EQ, BinaryType.NE, BinaryType.LT, BinaryType.LE, BinaryType.GT,
                    BinaryType.GE};
            for (BinaryType op : ops) {
                for (LocalDateTime c : constants) {
                    ScalarOperator predicate = new BinaryPredicateOperator(op, cTimestampCol,
                            ConstantOperator.createDatetime(c));
                    for (boolean negate : new boolean[] {false, true}) {
                        ScalarOperator original = negate ? new CompoundPredicateOperator(
                                CompoundPredicateOperator.CompoundType.NOT, predicate) : predicate;
                        Predicate converted = converter.convert(List.of(original), context);
                        for (long v : values) {
                            LocalDateTime local = LocalDateTime.ofInstant(
                                    Instant.EPOCH.plus(v, ChronoUnit.MICROS), zone);
                            boolean matches = compare(sqlOp(op), local.compareTo(c)) != negate;
                            if (matches) {
                                Assertions.assertTrue(eval(converted, v),
                                        original + " matches " + local + " but " + converted + " skips it");
                            }
                        }
                    }
                }
            }

            // 01:30 happens twice on 2024-11-03: both instants stay
            Predicate eq = converter.convert(List.of(new BinaryPredicateOperator(BinaryType.EQ, cTimestampCol,
                    ConstantOperator.createDatetime(LocalDateTime.of(2024, 11, 3, 1, 30)))), context);
            long first = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse("2024-11-03T05:30:00Z"));
            long second = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse("2024-11-03T06:30:00Z"));
            Assertions.assertEquals(new And(new Predicate(">=", cDeltaTimestampCol, Literal.ofTimestamp(first)),
                    new Predicate("<=", cDeltaTimestampCol, Literal.ofTimestamp(second))).toString(), eq.toString());
            // an unambiguous constant converts as before
            Predicate lt = converter.convert(List.of(new BinaryPredicateOperator(BinaryType.LT, cTimestampCol,
                    ConstantOperator.createDatetime(LocalDateTime.of(2024, 11, 3, 3, 0)))), context);
            Assertions.assertEquals(new Predicate("<", cDeltaTimestampCol, Literal.ofTimestamp(
                    ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse("2024-11-03T08:00:00Z")))).toString(),
                    lt.toString());

            // IN expands each ambiguous value to the same range as EQ, and keeps unambiguous values as equalities
            ScalarOperator in = new InPredicateOperator(false, cTimestampCol,
                    ConstantOperator.createDatetime(LocalDateTime.of(2024, 11, 3, 1, 30)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2024, 11, 3, 3, 0)));
            Assertions.assertEquals(new Or(new And(
                    new Predicate(">=", cDeltaTimestampCol, Literal.ofTimestamp(first)),
                    new Predicate("<=", cDeltaTimestampCol, Literal.ofTimestamp(second))),
                    new Predicate("=", cDeltaTimestampCol, Literal.ofTimestamp(ChronoUnit.MICROS.between(
                            Instant.EPOCH, Instant.parse("2024-11-03T08:00:00Z"))))).toString(),
                    converter.convert(List.of(in), context).toString());
            // under a NOT an ambiguous value has no subset, so the whole IN is dropped
            Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE.toString(), converter.convert(List.of(
                    new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, in)), context)
                    .toString());
        } finally {
            ConnectContext.remove();
        }
    }
}
