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
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
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
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertNull(result);
    }

    @Test
    public void testEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof Equal);
        Assert.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testNotEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.NE, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof NotEqual);
        Assert.assertEquals(5, leafPredicate.literals().get(0));
        ConstantOperator bool = ConstantOperator.createBoolean(false);
        op = new BinaryPredicateOperator(BinaryType.NE, F4, bool);
        result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof NotEqual);
        Assert.assertEquals(false, leafPredicate.literals().get(0));
    }

    @Test
    public void testLessEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.LE, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof LessOrEqual);
        Assert.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testLessThan() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.LT, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof LessThan);
        Assert.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testGreaterEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.GE, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof GreaterOrEqual);
        Assert.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testGreaterThan() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.GT, F0, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof GreaterThan);
        Assert.assertEquals(5, leafPredicate.literals().get(0));
    }

    @Test
    public void testNullOp() {
        ScalarOperator op = new IsNullPredicateOperator(false, F0);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof IsNull);
    }

    @Test
    public void testNotNullOp() {
        ScalarOperator op = new IsNullPredicateOperator(true, F0);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof IsNotNull);
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
        Assert.assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate compoundPredicate = (CompoundPredicate) result;
        Assert.assertTrue(compoundPredicate.function() instanceof Or);
        Assert.assertEquals(2, compoundPredicate.children().size());

        Assert.assertTrue(compoundPredicate.children().get(0) instanceof CompoundPredicate);
        CompoundPredicate child1 = (CompoundPredicate) compoundPredicate.children().get(0);

        Assert.assertEquals(2, child1.children().size());
        LeafPredicate child11 = (LeafPredicate) child1.children().get(0);
        Assert.assertTrue(child11.function() instanceof  Equal);
        Assert.assertEquals(1, child11.literals().size());
        Assert.assertEquals(11, child11.literals().get(0));

        LeafPredicate child12 = (LeafPredicate) child1.children().get(1);
        Assert.assertTrue(child12.function() instanceof  Equal);
        Assert.assertEquals(1, child12.literals().size());
        Assert.assertEquals(22, child12.literals().get(0));

        Assert.assertTrue(compoundPredicate.children().get(1) instanceof LeafPredicate);
        LeafPredicate child2 = (LeafPredicate) compoundPredicate.children().get(1);
        Assert.assertTrue(child2.function() instanceof Equal);
        Assert.assertEquals(1, child2.literals().size());
        Assert.assertEquals(333, child2.literals().get(0));
    }

    @Test
    public void testLike() {
        ConstantOperator value = ConstantOperator.createVarchar("ttt%");
        ScalarOperator op = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, F1, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertTrue(leafPredicate.function() instanceof StartsWith);
        Assert.assertEquals("ttt", leafPredicate.literals().get(0).toString());
    }

    @Test
    public void testAnd() {
        BinaryPredicateOperator op1 = new BinaryPredicateOperator(
                BinaryType.GT, F0, ConstantOperator.createInt(2));
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(
                BinaryType.LT, F0, ConstantOperator.createInt(5));
        ScalarOperator op = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, op1, op2);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate compoundPredicate = (CompoundPredicate) result;
        Assert.assertTrue(compoundPredicate.function() instanceof And);
        Assert.assertEquals(2, compoundPredicate.children().size());

        Assert.assertTrue(compoundPredicate.children().get(0) instanceof LeafPredicate);
        LeafPredicate p1 = (LeafPredicate) compoundPredicate.children().get(0);
        Assert.assertTrue(p1.function() instanceof GreaterThan);
        Assert.assertEquals(2, p1.literals().get(0));

        Assert.assertTrue(compoundPredicate.children().get(1) instanceof LeafPredicate);
        LeafPredicate p2 = (LeafPredicate) compoundPredicate.children().get(1);
        Assert.assertTrue(p2.function() instanceof LessThan);
        Assert.assertEquals(5, p2.literals().get(0));
    }

    @Test
    public void testOr() {
        BinaryPredicateOperator op1 = new BinaryPredicateOperator(
                BinaryType.GE, F0, ConstantOperator.createInt(44));
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(
                BinaryType.LE, F0, ConstantOperator.createInt(22));
        ScalarOperator op = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, op1, op2);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate compoundPredicate = (CompoundPredicate) result;
        Assert.assertTrue(compoundPredicate.function() instanceof Or);
        Assert.assertEquals(2, compoundPredicate.children().size());

        Assert.assertTrue(compoundPredicate.children().get(0) instanceof LeafPredicate);
        LeafPredicate p1 = (LeafPredicate) compoundPredicate.children().get(0);
        Assert.assertTrue(p1.function() instanceof GreaterOrEqual);
        Assert.assertEquals(44, p1.literals().get(0));

        Assert.assertTrue(compoundPredicate.children().get(1) instanceof LeafPredicate);
        LeafPredicate p2 = (LeafPredicate) compoundPredicate.children().get(1);
        Assert.assertTrue(p2.function() instanceof LessOrEqual);
        Assert.assertEquals(22, p2.literals().get(0));
    }

    @Test
    public void testBinaryString() {
        ConstantOperator value = ConstantOperator.createVarchar("ttt");
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F1, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertEquals(BinaryString.fromString("ttt"), leafPredicate.literals().get(0));
    }

    @Test
    public void testDecimal() {
        ConstantOperator value = ConstantOperator.createDecimal(new BigDecimal(14.11), Type.DEFAULT_DECIMAL128);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F2, value);
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertEquals(14.11, leafPredicate.literals().get(0));
    }

    @Test
    public void testPaimonCastPredicate() {
        // double to int
        ConstantOperator doubleValue = ConstantOperator.createDouble(11.11);
        CastOperator cast0 = new CastOperator(Type.INT, F0);
        Predicate result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast0, doubleValue));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate0 = (LeafPredicate) result;
        Assert.assertEquals(11, leafPredicate0.literals().get(0));
        // string to date
        ConstantOperator string = ConstantOperator.createVarchar("2025-01-01");
        CastOperator cast1 = new CastOperator(Type.DATE, F1);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast1, string));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate1 = (LeafPredicate) result;
        Assert.assertEquals(BinaryString.fromString("2025-01-01"), leafPredicate1.literals().get(0));
        // float to double
        ConstantOperator floatValue = ConstantOperator.createFloat(11.11);
        CastOperator cast2 = new CastOperator(Type.DOUBLE, F2);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast2, floatValue));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate2 = (LeafPredicate) result;
        Assert.assertEquals(11.11, leafPredicate2.literals().get(0));
        // date to string
        ConstantOperator date = ConstantOperator.createDate(
                LocalDate.parse("2025-01-01").atTime(0, 0, 0, 0));
        CastOperator cast3 = new CastOperator(Type.STRING, F3);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast3, date));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate3 = (LeafPredicate) result;
        Assert.assertEquals(20089, leafPredicate3.literals().get(0));
        // bool to string
        ConstantOperator bool = ConstantOperator.createBoolean(true);
        CastOperator cast4 = new CastOperator(Type.INT, F1);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast4, bool));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate4 = (LeafPredicate) result;
        Assert.assertEquals(BinaryString.fromString("1"), leafPredicate4.literals().get(0));
        // bool to int
        ConstantOperator bool2 = ConstantOperator.createBoolean(false);
        CastOperator cast5 = new CastOperator(Type.INT, F0);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast5, bool2));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate5 = (LeafPredicate) result;
        Assert.assertEquals(0, leafPredicate5.literals().get(0));
        // datetime to string
        ConstantOperator ts = ConstantOperator.createDatetime(
                LocalDate.parse("2025-01-01").atTime(0, 0, 0, 0));
        CastOperator cast6 = new CastOperator(Type.VARCHAR, F1);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast6, ts));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate6 = (LeafPredicate) result;
        Assert.assertEquals(BinaryString.fromString("2025-01-01 00:00:00"), leafPredicate6.literals().get(0));
        // tinyInt to bool
        ConstantOperator stringBool = ConstantOperator.createTinyInt((byte) 0);
        CastOperator cast7 = new CastOperator(Type.BOOLEAN, F4);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast7, stringBool));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate7 = (LeafPredicate) result;
        Assert.assertEquals(false, leafPredicate7.literals().get(0));
        // string to datetime
        ConstantOperator stringTime = ConstantOperator.createVarchar("2025-01-01 00:00:00");
        CastOperator cast8 = new CastOperator(Type.DATETIME, F5);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast8, stringTime));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate8 = (LeafPredicate) result;
        Assert.assertEquals(1735689600000L, ((Timestamp) (leafPredicate8.literals().get(0))).getMillisecond());
        // smallInt to string
        ConstantOperator si = ConstantOperator.createSmallInt((short) 200);
        CastOperator cast9 = new CastOperator(Type.VARCHAR, F1);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast9, si));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate9 = (LeafPredicate) result;
        Assert.assertEquals(BinaryString.fromString("200"), leafPredicate9.literals().get(0));
        // int to long
        ConstantOperator i = ConstantOperator.createInt(200);
        CastOperator cast10 = new CastOperator(Type.BIGINT, F6);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast10, i));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate10 = (LeafPredicate) result;
        Assert.assertEquals(200L, leafPredicate10.literals().get(0));
        // int to smallint
        ConstantOperator is = ConstantOperator.createInt(200);
        CastOperator cast11 = new CastOperator(Type.BIGINT, F8);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast11, is));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate11 = (LeafPredicate) result;
        Assert.assertEquals((short) 200, leafPredicate11.literals().get(0));
        // int to tinyint
        ConstantOperator it = ConstantOperator.createInt(10);
        CastOperator cast12 = new CastOperator(Type.BIGINT, F9);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast12, it));
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate12 = (LeafPredicate) result;
        Assert.assertEquals((byte) 10, leafPredicate12.literals().get(0));
        // can not cast to decimal
        ConstantOperator d = ConstantOperator.createDouble(14.11);
        CastOperator cast99 = new CastOperator(Type.DEFAULT_DECIMAL128, F7);
        result = CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, cast99, d));
        Assert.assertNull(result);
    }
}
