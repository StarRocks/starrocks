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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PaimonPredicateConverterTest {

    private static final List<DataField> DATA_FIELDS =
            Arrays.asList(
                    new DataField(0, "f0", new IntType()),
                    new DataField(1, "f1", new VarCharType()),
                    new DataField(2, "f2", new FloatType()));
    private static final ColumnRefOperator F0 = new ColumnRefOperator(0, Type.INT, "f0", true, false);
    private static final ColumnRefOperator F1 = new ColumnRefOperator(0, Type.VARCHAR, "f1", true, false);
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
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F1, value);;
        Predicate result = CONVERTER.convert(op);
        Assert.assertTrue(result instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        Assert.assertEquals(BinaryString.fromString("ttt"), leafPredicate.literals().get(0));
    }
}
