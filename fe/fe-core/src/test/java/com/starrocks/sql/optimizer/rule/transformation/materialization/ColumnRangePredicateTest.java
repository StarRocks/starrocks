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

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;

public class ColumnRangePredicateTest {

    @Test
    public void testCastDate() {
        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.VARCHAR, "dt", true);
            CastOperator dateOp = new CastOperator(Type.DATE, columnRef);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            range = range.intersection(Range.atMost(upperConstant));
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(dateOp, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assert.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assert.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assert.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            ConstantOperator up = ConstantOperator.createVarchar("20231001");
            Range<ConstantOperator> r = Range.atLeast(low);
            r = r.intersection(Range.atMost(up));
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assert.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.VARCHAR, "dt", true);
            CastOperator dateOp = new CastOperator(Type.DATE, columnRef);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atMost(upperConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(dateOp, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assert.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assert.assertEquals("2023-10-01", ((ConstantOperator) ranges1.get(0).upperEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assert.assertEquals("20231001", ((ConstantOperator) ranges2.get(0).upperEndpoint()).getVarchar());

            ConstantOperator up = ConstantOperator.createVarchar("2023-10-01");
            Range<ConstantOperator> r = Range.atMost(up);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assert.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.VARCHAR, "dt", true);
            CastOperator dateOp = new CastOperator(Type.DATE, columnRef);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(dateOp, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assert.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assert.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assert.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            Range<ConstantOperator> r = Range.atLeast(low);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assert.assertEquals(ConstantOperator.TRUE, ret);
        }
    }

    @Test
    public void testStr2Date() {
        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.VARCHAR, "dt", true);
            ConstantOperator format = ConstantOperator.createVarchar("%Y-%m-%d");
            List<ScalarOperator> args = Lists.newArrayList(columnRef, format);
            CallOperator call = new CallOperator(FunctionSet.STR2DATE, Type.DATE, args);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            range = range.intersection(Range.atMost(upperConstant));
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(call, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assert.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assert.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assert.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            ConstantOperator up = ConstantOperator.createVarchar("20231001");
            Range<ConstantOperator> r = Range.atLeast(low);
            r = r.intersection(Range.atMost(up));
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assert.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.VARCHAR, "dt", true);
            ConstantOperator format = ConstantOperator.createVarchar("%Y-%m-%d");
            List<ScalarOperator> args = Lists.newArrayList(columnRef, format);
            CallOperator call = new CallOperator(FunctionSet.STR2DATE, Type.DATE, args);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atMost(upperConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(call, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assert.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assert.assertEquals("2023-10-01", ((ConstantOperator) ranges1.get(0).upperEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assert.assertEquals("20231001", ((ConstantOperator) ranges2.get(0).upperEndpoint()).getVarchar());

            ConstantOperator up = ConstantOperator.createVarchar("2023-10-01");
            Range<ConstantOperator> r = Range.atMost(up);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assert.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.VARCHAR, "dt", true);
            ConstantOperator format = ConstantOperator.createVarchar("%Y-%m-%d");
            List<ScalarOperator> args = Lists.newArrayList(columnRef, format);
            CallOperator call = new CallOperator(FunctionSet.STR2DATE, Type.DATE, args);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(call, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assert.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assert.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assert.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            Range<ConstantOperator> r = Range.atLeast(low);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assert.assertEquals(ConstantOperator.TRUE, ret);
        }
    }
}
