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

package com.starrocks.connector.paimon;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class PaimonPredicateConverterTest {

    private static final List<DataField> DATA_FIELDS =
            Arrays.asList(
                    new DataField(0, "f0", new IntType()),
                    new DataField(1, "f1", new VarCharType()),
                    new DataField(2, "f2", new FloatType()),
                    new DataField(3, "f3", new DateType()),
                    new DataField(4, "f4", new BooleanType()),
                    new DataField(5, "f5", new TimestampType()),
                    new DataField(6, "f6", new BigIntType()),
                    new DataField(7, "f7", new DecimalType()),
                    new DataField(8, "f8", new SmallIntType()),
                    new DataField(9, "f9", new TinyIntType()));
    private static final ColumnRefOperator F0 = new ColumnRefOperator(0, Type.INT, "f0", true, false);
    private static final ColumnRefOperator F1 = new ColumnRefOperator(1, Type.VARCHAR, "f1", true, false);
    private static final ColumnRefOperator F2 = new ColumnRefOperator(2, Type.FLOAT, "f2", true, false);
    private static final ColumnRefOperator F3 = new ColumnRefOperator(3, Type.DATE, "f3", true, false);
    private static final ColumnRefOperator F4 = new ColumnRefOperator(4, Type.BOOLEAN, "f4", true, false);
    private static final ColumnRefOperator F5 = new ColumnRefOperator(5, Type.DATETIME, "f5", true, false);
    private static final ColumnRefOperator F6 = new ColumnRefOperator(6, Type.BIGINT, "f6", true, false);
    private static final ColumnRefOperator F7 = new ColumnRefOperator(7, Type.DEFAULT_DECIMAL128, "f7", true, false);
    private static final ColumnRefOperator F8 = new ColumnRefOperator(8, Type.SMALLINT, "f8", true, false);
    private static final ColumnRefOperator F9 = new ColumnRefOperator(9, Type.TINYINT, "f9", true, false);
    private static final PaimonPredicateConverter CONVERTER = new PaimonPredicateConverter(new RowType(DATA_FIELDS));

    @Test
    public void testNull() {
        Predicate result = CONVERTER.convert(null);
        Assertions.assertNull(result);
    }

    @Test
    public void testEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof Equal);
        Assertions.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testNotEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.NE, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof NotEqual);
        Assertions.assertEquals(5, leafPredicate.literals().get(0));
        ConstantOperator bool = ConstantOperator.createBoolean(false);
        op = new BinaryPredicateOperator(BinaryType.NE, F4, bool);
        result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof NotEqual);
        Assertions.assertEquals(false, leafPredicate.literals().get(0));
    }

    @Test
    public void testLessEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.LE, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof LessOrEqual);
        Assertions.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testLessThan() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.LT, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof LessThan);
        Assertions.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testGreaterEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.GE, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof GreaterOrEqual);
        Assertions.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testGreaterThan() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.GT, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof GreaterThan);
        Assertions.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testNullOp() {
        ScalarOperator op = new IsNullPredicateOperator(false, F0);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof IsNull);
    }

    @Test
    public void testNotNullOp() {
        ScalarOperator op = new IsNullPredicateOperator(true, F0);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof IsNotNull);
    }

    @Test
    public void testIn() {
        List<ScalarOperator> inOp = Lists.newArrayList();
        inOp.add(F0);
        inOp.add(ConstantOperator.createInt(11));
        inOp.add(ConstantOperator.createInt(22));
        inOp.add(ConstantOperator.createInt(333));
        InPredicateOperator op = new InPredicateOperator(false, inOp);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate compoundPredicate = (CompoundPredicate) result;
        Assertions.assertTrue(compoundPredicate.function() instanceof Or);
        Assertions.assertEquals(2, compoundPredicate.children().size());

        Assertions.assertTrue(compoundPredicate.children().get(0) instanceof CompoundPredicate);
        CompoundPredicate child1 = (CompoundPredicate) compoundPredicate.children().get(0);

        Assertions.assertEquals(2, child1.children().size());
        LeafPredicate child11 = (LeafPredicate) child1.children().get(0);
        Assertions.assertTrue(child11.function() instanceof  Equal);
        Assertions.assertEquals(1, child11.literals().size());
        Assertions.assertEquals(11, child11.literals().get(0));

        LeafPredicate child12 = (LeafPredicate) child1.children().get(1);
        Assertions.assertTrue(child12.function() instanceof  Equal);
        Assertions.assertEquals(1, child12.literals().size());
        Assertions.assertEquals(22, child12.literals().get(0));

        Assertions.assertTrue(compoundPredicate.children().get(1) instanceof LeafPredicate);
        LeafPredicate child2 = (LeafPredicate) compoundPredicate.children().get(1);
        Assertions.assertTrue(child2.function() instanceof Equal);
        Assertions.assertEquals(1, child2.literals().size());
        Assertions.assertEquals(333, child2.literals().get(0));
    }

    @Test
    public void testLike() {
        ConstantOperator value = ConstantOperator.createVarchar("ttt%");
        ScalarOperator op = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, F1, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertTrue(leafPredicate.function() instanceof StartsWith);
        Assertions.assertEquals("ttt", leafPredicate.literals().get(0).toString());
    }

    @Test
    public void testAnd() {
        BinaryPredicateOperator op1 = new BinaryPredicateOperator(
                BinaryType.GT, F0, ConstantOperator.createInt(2));
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(
                BinaryType.LT, F0, ConstantOperator.createInt(5));
        ScalarOperator op = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, op1, op2);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate compoundPredicate = (CompoundPredicate) result;
        Assertions.assertTrue(compoundPredicate.function() instanceof And);
        Assertions.assertEquals(2, compoundPredicate.children().size());

        Assertions.assertTrue(compoundPredicate.children().get(0) instanceof LeafPredicate);
        LeafPredicate p1 = (LeafPredicate) compoundPredicate.children().get(0);
        Assertions.assertTrue(p1.function() instanceof GreaterThan);
        Assertions.assertEquals(2, p1.literals().get(0));

        Assertions.assertTrue(compoundPredicate.children().get(1) instanceof LeafPredicate);
        LeafPredicate p2 = (LeafPredicate) compoundPredicate.children().get(1);
        Assertions.assertTrue(p2.function() instanceof LessThan);
        Assertions.assertEquals(5, p2.literals().get(0));
    }

    @Test
    public void testOr() {
        BinaryPredicateOperator op1 = new BinaryPredicateOperator(
                BinaryType.GE, F0, ConstantOperator.createInt(44));
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(
                BinaryType.LE, F0, ConstantOperator.createInt(22));
        ScalarOperator op = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, op1, op2);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate compoundPredicate = (CompoundPredicate) result;
        Assertions.assertTrue(compoundPredicate.function() instanceof Or);
        Assertions.assertEquals(2, compoundPredicate.children().size());

        Assertions.assertTrue(compoundPredicate.children().get(0) instanceof LeafPredicate);
        LeafPredicate p1 = (LeafPredicate) compoundPredicate.children().get(0);
        Assertions.assertTrue(p1.function() instanceof GreaterOrEqual);
        Assertions.assertEquals(44, p1.literals().get(0));

        Assertions.assertTrue(compoundPredicate.children().get(1) instanceof LeafPredicate);
        LeafPredicate p2 = (LeafPredicate) compoundPredicate.children().get(1);
        Assertions.assertTrue(p2.function() instanceof LessOrEqual);
        Assertions.assertEquals(22, p2.literals().get(0));
    }

    @Test
    public void testBinaryString() {
        ConstantOperator value = ConstantOperator.createVarchar("ttt");
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F1, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertEquals(BinaryString.fromString("ttt"), leafPredicate.literals().get(0));
    }

    @Test
    public void testDecimal() {
        ConstantOperator value = ConstantOperator.createDecimal(new BigDecimal(14.11), Type.DEFAULT_DECIMAL128);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F2, value);
        Predicate result = CONVERTER.convert(op);
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assertions.assertEquals(14.11, leafPredicate.literals().get(0));
    }

    @Test
    public void testCastOnColumnIsNotPushedDown() {
        // a cast on the column can change the comparison (rounding, narrowing, string formats), so the
        // predicate stays with StarRocks, same as the BE converter which only accepts bare slots
        Object[][] cases = {
                {Type.INT, F0, ConstantOperator.createDouble(11.11)},
                {Type.DATE, F1, ConstantOperator.createVarchar("2025-01-01")},
                {Type.DOUBLE, F2, ConstantOperator.createFloat(11.11)},
                {Type.STRING, F3, ConstantOperator.createDate(LocalDate.parse("2025-01-01").atTime(0, 0))},
                {Type.INT, F1, ConstantOperator.createBoolean(true)},
                {Type.INT, F0, ConstantOperator.createBoolean(false)},
                {Type.VARCHAR, F1, ConstantOperator.createDatetime(LocalDate.parse("2025-01-01").atTime(0, 0))},
                {Type.BOOLEAN, F4, ConstantOperator.createTinyInt((byte) 0)},
                {Type.DATETIME, F5, ConstantOperator.createVarchar("2025-01-01 00:00:00")},
                {Type.BIGINT, F6, ConstantOperator.createInt(200)},
                {Type.BIGINT, F8, ConstantOperator.createInt(200)},
                {Type.BIGINT, F9, ConstantOperator.createInt(10)},
                {Type.DEFAULT_DECIMAL128, F7, ConstantOperator.createDouble(14.11)},
        };
        for (Object[] c : cases) {
            CastOperator cast = new CastOperator((Type) c[0], (ColumnRefOperator) c[1]);
            Predicate result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast, (ConstantOperator) c[2]));
            Assertions.assertNull(result, "cast to " + c[0] + " on " + ((ColumnRefOperator) c[1]).getName());
        }
        Assertions.assertNull(CONVERTER.convert(new IsNullPredicateOperator(false, new CastOperator(Type.BIGINT, F0))));
        Assertions.assertNull(CONVERTER.convert(new InPredicateOperator(false, new CastOperator(Type.BIGINT, F0),
                ConstantOperator.createInt(1), ConstantOperator.createInt(2))));

        // CAST(d AS DECIMAL(15,1)) = 1.2 on DECIMAL(15,2): stored 1.16 matches the SQL predicate after
        // rounding but not a pushed-down d = 1.20, so it must not be pushed; the bare column still is
        RowType rowType = new RowType(List.of(new DataField(0, "d", new DecimalType(15, 2))));
        PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
        ColumnRefOperator d = new ColumnRefOperator(0,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 2), "d", true, false);
        ConstantOperator lit = ConstantOperator.createDecimal(new BigDecimal("1.2"),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 1));
        CastOperator rounding = new CastOperator(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 1), d);
        Assertions.assertNull(converter.convert(new BinaryPredicateOperator(BinaryType.EQ, rounding, lit)));
        assertDecimalLeaf(converter.convert(new BinaryPredicateOperator(BinaryType.EQ, d, lit)), Equal.class, "1.20", 15, 2);
    }

    @Test
    public void testOrWithFunction() {
        //    (f0 = 44 and (case when f0 = 44 then 'test' end) = 'test')
        // OR (f0 <= 46 and (case when f0 = 44 then 'test' end) = 'test')
        // OR((case when f0 = 44 then 'test' end) = 'test' and f1 like 'ttt%')
        // return f0 = 44 OR f0 <= 46 OR f0 <= 20
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(
                BinaryType.EQ, F0, ConstantOperator.createInt(44));
        CaseWhenOperator caseWhenOperator = new CaseWhenOperator(Type.INT, op2, null,
                Lists.newArrayList(op2, ConstantOperator.createVarchar("test")));
        BinaryPredicateOperator test =
                new BinaryPredicateOperator(BinaryType.EQ, caseWhenOperator, ConstantOperator.createVarchar("test"));
        ScalarOperator op20 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, op2,
                test.clone());

        BinaryPredicateOperator op12 = new BinaryPredicateOperator(
                BinaryType.LE, F0, ConstantOperator.createInt(46));
        ScalarOperator op21 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, op12,
                test.clone());

        ConstantOperator value = ConstantOperator.createVarchar("ttt%");
        ScalarOperator op13 = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, F1, value);
        ScalarOperator op22 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                test.clone(), op13);

        CompoundPredicateOperator compoundPredicateOperator =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, op20, op21);
        CompoundPredicateOperator compoundPredicateOperator1 =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, compoundPredicateOperator,
                        op22);
        CompoundPredicate convert = (CompoundPredicate) CONVERTER.convert(compoundPredicateOperator1);
        Assertions.assertTrue(
                "Or([Or([Equal(f0, 44), LessOrEqual(f0, 46)]), StartsWith(f1, ttt)])".equals(convert.toString()));

        // (case when f0 = 44 then 'test' end) = 'test'
        // OR (f0 <= 46 and (case when f0 = 44 then 'test' end) = 'test')
        // return null
        ScalarOperator op40 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, test.clone(),
                op21.clone());
        CompoundPredicate convert1 = (CompoundPredicate) CONVERTER.convert(op40.clone());
        Assertions.assertTrue(convert1 == null);

        // NOT ((case when f0 = 44 then 'test' end) = 'test' and  f1 like 'ttt%')
        // return null
        ScalarOperator op52 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, op22.clone());
        Predicate convert2 = CONVERTER.convert(op52);
        Assertions.assertTrue(convert2 == null);
    }

    private static Predicate convertDecimalPredicate(PrimitiveType srType, int precision, int scale,
                                                     BinaryType op, ConstantOperator literal) {
        RowType rowType = new RowType(List.of(new DataField(0, "d", new DecimalType(precision, scale))));
        ColumnRefOperator col = new ColumnRefOperator(0,
                ScalarType.createDecimalV3Type(srType, precision, scale), "d", true, false);
        return new PaimonPredicateConverter(rowType).convert(new BinaryPredicateOperator(op, col, literal));
    }

    private static void assertDecimalLeaf(Predicate result, Class<?> function, String expected, int precision,
                                          int scale) {
        Assertions.assertNotNull(result, "decimal predicate must be pushed down");
        Assertions.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leaf = (LeafPredicate) result;
        Assertions.assertTrue(function.isInstance(leaf.function()));
        Decimal literal = (Decimal) leaf.literals().get(0);
        Assertions.assertEquals(precision, literal.precision());
        Assertions.assertEquals(scale, literal.scale());
        Assertions.assertEquals(new BigDecimal(expected), literal.toBigDecimal());
    }

    @Test
    public void testDecimalColumnPredicateUsesColumnPrecisionAndScale() {
        Predicate d64 = convertDecimalPredicate(PrimitiveType.DECIMAL64, 15, 2, BinaryType.LT,
                ConstantOperator.createDecimal(new BigDecimal("5.00"),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 2)));
        assertDecimalLeaf(d64, LessThan.class, "5.00", 15, 2);

        Predicate d32 = convertDecimalPredicate(PrimitiveType.DECIMAL32, 9, 2, BinaryType.EQ,
                ConstantOperator.createDecimal(new BigDecimal("123.45"),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2)));
        assertDecimalLeaf(d32, Equal.class, "123.45", 9, 2);

        Predicate d128 = convertDecimalPredicate(PrimitiveType.DECIMAL128, 38, 9, BinaryType.GE,
                ConstantOperator.createDecimal(new BigDecimal("12345678901234567890.123456789"),
                        Type.DEFAULT_DECIMAL128));
        assertDecimalLeaf(d128, GreaterOrEqual.class, "12345678901234567890.123456789", 38, 9);
    }

    @Test
    public void testDecimalColumnWithIntegerLiteral() {
        Predicate result = convertDecimalPredicate(PrimitiveType.DECIMAL64, 15, 2, BinaryType.LT,
                ConstantOperator.createInt(5));
        assertDecimalLeaf(result, LessThan.class, "5.00", 15, 2);
    }

    @Test
    public void testDecimalLiteralSurvivesPredicateSerialization() throws Exception {
        // the JNI reader gets the predicate through this serialization; a wrong scale used to become 5e14 here
        Predicate pushed = convertDecimalPredicate(PrimitiveType.DECIMAL64, 15, 2, BinaryType.GT,
                ConstantOperator.createDecimal(new BigDecimal("0.05"),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 2)));
        assertDecimalLeaf(pushed, GreaterThan.class, "0.05", 15, 2);
        Predicate roundTrip = InstantiationUtil.deserializeObject(InstantiationUtil.serializeObject(pushed),
                getClass().getClassLoader());
        assertDecimalLeaf(roundTrip, GreaterThan.class, "0.05", 15, 2);
        InternalRow minValues = GenericRow.of(Decimal.fromBigDecimal(new BigDecimal("0.00"), 15, 2));
        InternalRow maxValues = GenericRow.of(Decimal.fromBigDecimal(new BigDecimal("0.10"), 15, 2));
        Assertions.assertTrue(roundTrip.test(100, minValues, maxValues, new GenericArray(new Long[] {0L})));

        Predicate trailingZeros = convertDecimalPredicate(PrimitiveType.DECIMAL64, 15, 2, BinaryType.LT,
                ConstantOperator.createDecimal(new BigDecimal("5.000"),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 3)));
        assertDecimalLeaf(trailingZeros, LessThan.class, "5.00", 15, 2);
    }

    @Test
    public void testDecimalLiteralExceedingColumnPrecisionIsNotPushedDown() {
        // 21 digits do not fit DECIMAL(15,2); Decimal.fromBigDecimal returns null instead of truncating
        Predicate result = convertDecimalPredicate(PrimitiveType.DECIMAL64, 15, 2, BinaryType.GT,
                ConstantOperator.createDecimal(new BigDecimal("100000000000000000000"),
                        Type.DEFAULT_DECIMAL128));
        Assertions.assertNull(result);
        // 13 integer digits is the most DECIMAL(15,2) holds
        Predicate fits = convertDecimalPredicate(PrimitiveType.DECIMAL64, 15, 2, BinaryType.GT,
                ConstantOperator.createDecimal(new BigDecimal("9999999999999.99"),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 2)));
        assertDecimalLeaf(fits, GreaterThan.class, "9999999999999.99", 15, 2);
    }

    @Test
    public void testDecimalLiteralWiderScaleThanColumnIsNotPushedDown() {
        Predicate result = convertDecimalPredicate(PrimitiveType.DECIMAL64, 15, 2, BinaryType.LT,
                ConstantOperator.createDecimal(new BigDecimal("5.005"),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 3)));
        Assertions.assertNull(result);
    }
}
