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


package com.starrocks.sql.optimizer.operator.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.structure.Pair;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class CompoundPredicateOperatorTest {


    @ParameterizedTest
    @MethodSource("compareOperatorList")
    void predicateCompare(Pair<Boolean, List<ScalarOperator>> pair) {
        boolean isEqual = pair.first;
        ScalarOperator left = pair.second.get(0);
        ScalarOperator right = pair.second.get(1);
        if (isEqual) {
            Assert.assertEquals(left, right);
        } else {
            Assert.assertNotEquals(left, right);
        }

    }

    private static Stream<Arguments> compareOperatorList() {
        ConstantOperator constA = new ConstantOperator(1, Type.INT);
        ConstantOperator constB = new ConstantOperator("abc", Type.STRING);

        ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "c1", false);
        ColumnRefOperator col2 = new ColumnRefOperator(2, Type.INT, "c2", false);
        ColumnRefOperator col3 = new ColumnRefOperator(3, Type.INT, "c3", false);
        ColumnRefOperator col4 = new ColumnRefOperator(4, Type.STRING, "c4", false);
        ColumnRefOperator col5 = new ColumnRefOperator(5, Type.STRING, "c5", false);
        ColumnRefOperator col6 = new ColumnRefOperator(6, Type.BOOLEAN, "c6", false);


        CallOperator call1 = new CallOperator(FunctionSet.SUM, Type.BIGINT, ImmutableList.of(constA));
        CallOperator call2 = new CallOperator(FunctionSet.ADD, Type.BIGINT, ImmutableList.of(constA, constA));

        BinaryPredicateOperator gt1 = new BinaryPredicateOperator(BinaryType.GT, col1, col2);
        BinaryPredicateOperator gt2 = new BinaryPredicateOperator(BinaryType.GT, col3, col4);

        BinaryPredicateOperator lt1 = new BinaryPredicateOperator(BinaryType.LT, col4, col6);
        BinaryPredicateOperator lt2 = new BinaryPredicateOperator(BinaryType.LT, col5, col6);

        BinaryPredicateOperator ge1 = new BinaryPredicateOperator(BinaryType.GE, col1, constA);
        BinaryPredicateOperator ge2 = new BinaryPredicateOperator(BinaryType.GE, col4, constB);

        BinaryPredicateOperator le1 = new BinaryPredicateOperator(BinaryType.LE, col1, call1);
        BinaryPredicateOperator le2 = new BinaryPredicateOperator(BinaryType.LE, col2, call2);

        CompoundPredicateOperator and1 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, gt1, gt2);
        CompoundPredicateOperator and2 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, lt1, lt2);
        CompoundPredicateOperator and3 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, ge1, ge2);
        CompoundPredicateOperator and4 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, le1, le2);


        CompoundPredicateOperator or1 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, gt1, gt2);
        CompoundPredicateOperator or2 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, lt1, lt2);
        CompoundPredicateOperator or3 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, ge1, ge2);
        CompoundPredicateOperator or4 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, le1, le2);

        List<List<ScalarOperator>> equalList = Lists.newArrayList();
        List<List<ScalarOperator>> notEqualList = Lists.newArrayList();

        ScalarOperator fullAnd = Utils.compoundAnd(and1, and2, and3, and4);
        equalList.add(ImmutableList.of(fullAnd, Utils.compoundAnd(and1, and2, and4, and3)));
        equalList.add(ImmutableList.of(fullAnd, Utils.compoundAnd(and1, and3, and2, and4)));
        equalList.add(ImmutableList.of(fullAnd, Utils.compoundAnd(and2, and4, and1, and3)));
        equalList.add(ImmutableList.of(fullAnd, Utils.compoundAnd(and3, and1, and2, and4)));

        ScalarOperator fullOr = Utils.compoundOr(or1, or2, or3, or4);
        equalList.add(ImmutableList.of(fullOr, Utils.compoundOr(or1, or2, or3, or4)));
        equalList.add(ImmutableList.of(fullOr, Utils.compoundOr(or1, or3, or2, or4)));
        equalList.add(ImmutableList.of(fullOr, Utils.compoundOr(or2, or3, or1, or4)));
        equalList.add(ImmutableList.of(fullOr, Utils.compoundOr(or3, or1, or2, or4)));

        ScalarOperator halfAnd = Utils.compoundAnd(Utils.compoundOr(and1, and2), and3, or4);
        equalList.add(ImmutableList.of(halfAnd, Utils.compoundAnd(and3, or4, Utils.compoundOr(and1, and2))));
        equalList.add(ImmutableList.of(halfAnd, Utils.compoundAnd(or4, and3, Utils.compoundOr(and2, and1))));
        equalList.add(ImmutableList.of(halfAnd, Utils.compoundAnd(or4, Utils.compoundOr(and2, and1), and3)));

        ScalarOperator halfOr = Utils.compoundOr(Utils.compoundAnd(and1, and2), or3, or4);
        equalList.add(ImmutableList.of(halfOr, Utils.compoundOr(or3, or4, Utils.compoundAnd(and1, and2))));
        equalList.add(ImmutableList.of(halfOr, Utils.compoundOr(or4, or3, Utils.compoundAnd(and2, and1))));
        equalList.add(ImmutableList.of(halfOr, Utils.compoundOr(or4, Utils.compoundAnd(and2, and1), or3)));


        notEqualList.add(ImmutableList.of(fullAnd, Utils.compoundAnd(and1, or2, and3, and4)));
        notEqualList.add(ImmutableList.of(fullAnd, Utils.compoundAnd(and1, or1, and2, or4)));

        notEqualList.add(ImmutableList.of(fullOr, Utils.compoundOr(and1, or1, and2, or4)));
        notEqualList.add(ImmutableList.of(fullOr, Utils.compoundOr(and1, or1, and2, or4)));

        notEqualList.add(ImmutableList.of(halfAnd, Utils.compoundAnd(Utils.compoundOr(and1, and2), and3, and4)));
        notEqualList.add(ImmutableList.of(halfAnd, Utils.compoundAnd(or4, and3, Utils.compoundOr(or2, and1))));

        notEqualList.add(ImmutableList.of(halfOr, Utils.compoundOr(Utils.compoundAnd(and1, and2), and3, and4)));
        notEqualList.add(ImmutableList.of(halfOr, Utils.compoundOr(Utils.compoundAnd(and1, or2), or3, and4)));

        List<Pair<Boolean, List<ScalarOperator>>> cases = Lists.newArrayList();
        equalList.stream().forEach(e -> cases.add(Pair.create(true, e)));
        notEqualList.stream().forEach(e -> cases.add(Pair.create(false, e)));

        return cases.stream().map(Arguments::of);
    }
}
