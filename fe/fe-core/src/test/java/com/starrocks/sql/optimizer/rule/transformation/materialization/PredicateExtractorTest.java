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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class PredicateExtractorTest {
    private static Map<ScalarType, List<ScalarOperator>> DATA = ImmutableMap.<ScalarType, List<ScalarOperator>>builder()
            .put(Type.TINYINT, Lists.newArrayList(ConstantOperator.createTinyInt((byte) 10),
                    ConstantOperator.createTinyInt((byte) 20), ConstantOperator.createTinyInt((byte) 30),
                    ConstantOperator.createTinyInt((byte) 50), ConstantOperator.createTinyInt((byte) 50)))
            .put(Type.SMALLINT, Lists.newArrayList(ConstantOperator.createSmallInt((short) 10),
                    ConstantOperator.createSmallInt((short) 20), ConstantOperator.createSmallInt((short) 30),
                    ConstantOperator.createSmallInt((short) 50), ConstantOperator.createSmallInt((short) 50)))
            .put(Type.INT, Lists.newArrayList(ConstantOperator.createInt(10),
                    ConstantOperator.createInt(20), ConstantOperator.createInt(30),
                    ConstantOperator.createInt(50), ConstantOperator.createInt(50)))
            .put(Type.BIGINT, Lists.newArrayList(ConstantOperator.createBigint(10),
                    ConstantOperator.createBigint(20), ConstantOperator.createBigint(30),
                    ConstantOperator.createBigint(50), ConstantOperator.createBigint(50)))
            .put(Type.LARGEINT, Lists.newArrayList(ConstantOperator.createLargeInt(BigInteger.valueOf(10)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(20)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(30)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(50)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(50))))
            .put(Type.FLOAT, Lists.newArrayList(ConstantOperator.createFloat(10),
                    ConstantOperator.createFloat(20), ConstantOperator.createFloat(30),
                    ConstantOperator.createFloat(50), ConstantOperator.createFloat(50)))
            .put(Type.DOUBLE, Lists.newArrayList(ConstantOperator.createDouble(10),
                    ConstantOperator.createDouble(20), ConstantOperator.createDouble(30),
                    ConstantOperator.createDouble(50), ConstantOperator.createDouble(50)))
            .put(Type.DATE, Lists.newArrayList(ConstantOperator.createDate(LocalDateTime.of(2023, 9, 10, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 11, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 12, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 15, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 15, 0, 0))))
            .put(Type.DATETIME, Lists.newArrayList(ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 10, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 11, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 12, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 15, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 15, 0, 0))))
            .build();

    @Test
    public void testRangePredicates() {
        for (Type type : DATA.keySet()) {
            ColumnRefOperator col1 = new ColumnRefOperator(1, type, "col1", false);
            ColumnRefOperator col2 = new ColumnRefOperator(2, type, "col2", false);
            BinaryPredicateOperator binary1 = BinaryPredicateOperator.ge(col1, DATA.get(type).get(0));
            BinaryPredicateOperator binary2 = BinaryPredicateOperator.lt(col2, DATA.get(type).get(1));
            CompoundPredicateOperator compound1 = CompoundPredicateOperator.or(binary1,  binary2).cast();

            BinaryPredicateOperator binary3 = BinaryPredicateOperator.lt(col1, DATA.get(type).get(2));
            BinaryPredicateOperator binary4 = BinaryPredicateOperator.gt(col2, DATA.get(type).get(3));
            CompoundPredicateOperator compound2 = CompoundPredicateOperator.or(binary3,  binary4).cast();
            CompoundPredicateOperator compound3 = CompoundPredicateOperator.or(compound1, compound2).cast();

            PredicateExtractor extractor = new PredicateExtractor();
            RangePredicate rangeOperator = compound3.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
            Assert.assertTrue(rangeOperator instanceof ColumnRangePredicate);
            ColumnRangePredicate columnRangePredicate1 = (ColumnRangePredicate) rangeOperator;
            Assert.assertEquals("col2", columnRangePredicate1.getColumnRef().getName());

            BinaryPredicateOperator binary5 = BinaryPredicateOperator.ge(col1, DATA.get(type).get(4));
            CompoundPredicateOperator compound4 = CompoundPredicateOperator.or(binary2,  binary5).cast();
            CompoundPredicateOperator compound5 = CompoundPredicateOperator.or(compound4, compound2).cast();
            RangePredicate rangeOperator2 = compound5.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
            Assert.assertTrue(rangeOperator2 instanceof OrRangePredicate);
            OrRangePredicate orRangePredicate1 = (OrRangePredicate) rangeOperator2;
            Assert.assertEquals(2, orRangePredicate1.getChildPredicates().size());
            Assert.assertTrue(orRangePredicate1.getChildPredicates().get(0) instanceof ColumnRangePredicate);
            Assert.assertTrue(orRangePredicate1.getChildPredicates().get(1) instanceof ColumnRangePredicate);
        }
    }

    @Test
    public void testColumnPredicate() {
        {
            ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "col1", false);
            Range<ConstantOperator> range1 = Range.atLeast(ConstantOperator.createInt(10));
            TreeRangeSet<ConstantOperator> columnRange1 = TreeRangeSet.create();
            columnRange1.add(range1);
            ColumnRangePredicate columnRangePredicate1 = new ColumnRangePredicate(col1, columnRange1);

            Range<ConstantOperator> range2 = Range.atMost(ConstantOperator.createInt(100));
            TreeRangeSet<ConstantOperator> columnRange2 = TreeRangeSet.create();
            columnRange2.add(range2);
            ColumnRangePredicate columnRangePredicate2 = new ColumnRangePredicate(col1, columnRange2);

            ColumnRangePredicate orRange = ColumnRangePredicate.orRange(columnRangePredicate1, columnRangePredicate2);
            Assert.assertEquals(ConstantOperator.TRUE, orRange.toScalarOperator());

            ColumnRangePredicate andRange = ColumnRangePredicate.andRange(columnRangePredicate1, columnRangePredicate2);
            Assert.assertEquals("1: col1 >= 10 AND 1: col1 <= 100", andRange.toScalarOperator().toString());
        }

        {
            ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "col1", false);
            Range<ConstantOperator> range1 = Range.atLeast(ConstantOperator.createInt(100));
            TreeRangeSet<ConstantOperator> columnRange1 = TreeRangeSet.create();
            columnRange1.add(range1);
            ColumnRangePredicate columnRangePredicate1 = new ColumnRangePredicate(col1, columnRange1);

            Range<ConstantOperator> range2 = Range.atMost(ConstantOperator.createInt(10));
            TreeRangeSet<ConstantOperator> columnRange2 = TreeRangeSet.create();
            columnRange2.add(range2);
            ColumnRangePredicate columnRangePredicate2 = new ColumnRangePredicate(col1, columnRange2);

            ColumnRangePredicate andRange = ColumnRangePredicate.andRange(columnRangePredicate1, columnRangePredicate2);
            Assert.assertEquals(ConstantOperator.FALSE, andRange.toScalarOperator());
        }
    }
}
