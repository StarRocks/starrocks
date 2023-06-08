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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScalarEquivalenceExtractorTest {

    public static final ColumnRefOperator COLUMN_A = new ColumnRefOperator(1, Type.INT, "a", true);
    public static final ColumnRefOperator COLUMN_B = new ColumnRefOperator(2, Type.INT, "b", true);
    public static final ColumnRefOperator COLUMN_C = new ColumnRefOperator(3, Type.INT, "c", true);
    public static final ConstantOperator CONSTANT_1 = ConstantOperator.createInt(1);
    public static final ConstantOperator CONSTANT_2 = ConstantOperator.createInt(2);
    public static final ConstantOperator CONSTANT_3 = ConstantOperator.createInt(3);

    @Test
    public void equivalentReplaceEQTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_B, COLUMN_C);
        BinaryPredicateOperator bpo3 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_B, CONSTANT_2);
        BinaryPredicateOperator bpo4 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_C, CONSTANT_3);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2, bpo3, bpo4));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(4, list.size());
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, CONSTANT_2)));
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, CONSTANT_3)));
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C)));
    }

    @Test
    public void equivalentReplaceLTTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_B, COLUMN_C);
        BinaryPredicateOperator bpo3 = new BinaryPredicateOperator(BinaryType.LT,
                COLUMN_B, CONSTANT_1);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2, bpo3));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(3, list.size());
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.LT, COLUMN_A, CONSTANT_1)));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C)));
    }

    @Test
    public void equivalentReplaceLTFunctionTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.LT,
                COLUMN_B, new CallOperator("abs", Type.INT, Lists.newArrayList(ConstantOperator.createInt(2))));

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(2, list.size());
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.LT, COLUMN_A,
                        new CallOperator("abs", Type.INT, Lists.newArrayList(ConstantOperator.createInt(2))))));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
    }

    @Test
    public void equivalentReplaceLTFunctionRefTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.LT,
                COLUMN_B, new CallOperator("abs", Type.INT, Lists.newArrayList(COLUMN_C)));

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(1, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
    }

    @Test
    public void equivalentReplaceEQFunctionNest() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, new CallOperator("abs", Type.INT, Lists.newArrayList(COLUMN_C)));
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_C, new CallOperator("abs", Type.INT, Lists.newArrayList(CONSTANT_2)));

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(1, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                        new CallOperator("abs", Type.INT, Lists.newArrayList(COLUMN_C)))));
    }

    @Test
    public void equivalentReplaceEQFunctionTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, new CallOperator("abs", Type.INT, Lists.newArrayList(COLUMN_C)));
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_C, CONSTANT_2);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);
        System.out.println(list);

        assertEquals(2, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                        new CallOperator("abs", Type.INT, Lists.newArrayList(COLUMN_C)))));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                        new CallOperator("abs", Type.INT, Lists.newArrayList(CONSTANT_2)))));
    }

    @Test
    public void equivalentReplaceInTransit() {
        BinaryPredicateOperator bpo1 =
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C);
        InPredicateOperator bpo2 = new InPredicateOperator(COLUMN_C, CONSTANT_2, CONSTANT_1);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(2, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C)));
        assertTrue(
                list.contains(new InPredicateOperator(COLUMN_A, CONSTANT_2, CONSTANT_1)));
    }

    @Test
    public void equivalentReplaceInFunctionTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, new CallOperator("abs", Type.INT, Lists.newArrayList(COLUMN_C)));
        InPredicateOperator bpo2 = new InPredicateOperator(COLUMN_C, CONSTANT_2, CONSTANT_1);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(1, list.size());
        assertTrue(list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                new CallOperator("abs", Type.INT, Lists.newArrayList(COLUMN_C)))));
    }
}