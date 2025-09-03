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

package com.starrocks.qe.feedback.parameterization;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.feedback.ParameterizedPredicate;
import com.starrocks.qe.feedback.skeleton.ScanNode;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AndRangePredicate;
import com.starrocks.sql.optimizer.rule.transformation.materialization.ColumnRangePredicate;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OrRangePredicate;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RangePredicate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;

public class ParameterizedPredicateTest {
    private ColumnRefOperator colRef1;
    private ColumnRefOperator colRef2;
    private ColumnRefOperator colRef3;
    private ColumnRefOperator stringCol;
    private ColumnRefOperator dateCol;
    private ColumnRefOperator datetimeCol;
    private ColumnRefOperator stringDateCol;

    @BeforeEach
    public void setUp() {
        colRef1 = new ColumnRefOperator(1, Type.INT, "int_col", false);
        colRef2 = new ColumnRefOperator(2, Type.DOUBLE, "double_col", false);
        colRef3 = new ColumnRefOperator(3, Type.BIGINT, "bigint_col", false);
        stringCol = new ColumnRefOperator(4, Type.VARCHAR, "string_col", false);
        dateCol = new ColumnRefOperator(5, Type.DATE, "date_col", false);
        datetimeCol = new ColumnRefOperator(6, Type.DATETIME, "datetime_col", false);
        stringDateCol = new ColumnRefOperator(7, Type.VARCHAR, "string_date_col", false);
    }

    private ScanNode createScanNode(ScalarOperator predicate) {
        PhysicalScanOperator scanOp = PhysicalOlapScanOperator.builder()
                .setPredicate(predicate)
                .setColRefToColumnMetaMap(Map.of())
                .build();
        OptExpression optExpression = new OptExpression(scanOp);
        return new ScanNode(optExpression);
    }

    private BinaryPredicateOperator createBinaryPredicate(ColumnRefOperator col, BinaryType type, ConstantOperator constant) {
        return new BinaryPredicateOperator(type, col, constant);
    }

    private CompoundPredicateOperator createCompoundPredicate(CompoundType type, ScalarOperator left, ScalarOperator right) {
        return new CompoundPredicateOperator(type, left, right);
    }

    private CallOperator createStr2DateCall(ColumnRefOperator stringCol, String format) {
        List<ScalarOperator> args = new ArrayList<>();
        args.add(stringCol);
        args.add(ConstantOperator.createVarchar(format));
        return new CallOperator("str2date", Type.DATE, args);
    }

    private ColumnRangePredicate createColumnRangePredicate(ColumnRefOperator col, int lower, int upper) {
        TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closed(ConstantOperator.createInt(lower), ConstantOperator.createInt(upper)));
        BinaryPredicateOperator expr = createBinaryPredicate(col, BinaryType.GE, ConstantOperator.createInt(lower));
        return new ColumnRangePredicate(expr, rangeSet);
    }

    private ColumnRangePredicate createColumnRangePredicate(ColumnRefOperator col, double lower, double upper) {
        TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closed(ConstantOperator.createDouble(lower), ConstantOperator.createDouble(upper)));
        BinaryPredicateOperator expr = createBinaryPredicate(col, BinaryType.GE, ConstantOperator.createDouble(lower));
        return new ColumnRangePredicate(expr, rangeSet);
    }

    @Test
    public void testIsValidRangePredicate() {
        ColumnRangePredicate singleColPred = createColumnRangePredicate(colRef1, 10, 100);
        ParameterizedPredicate parameterizedPredicate = new ParameterizedPredicate(singleColPred.toScalarOperator());
        Assertions.assertTrue(parameterizedPredicate.isValidRangePredicate(singleColPred));

        List<RangePredicate> andChildren = new ArrayList<>();
        andChildren.add(createColumnRangePredicate(colRef1, 10, 100));
        andChildren.add(createColumnRangePredicate(colRef2, 1.0, 10.0));
        AndRangePredicate andPred = new AndRangePredicate(andChildren);
        parameterizedPredicate = new ParameterizedPredicate(andPred.toScalarOperator());
        Assertions.assertTrue(parameterizedPredicate.isValidRangePredicate(andPred));

        List<RangePredicate> orChildren = new ArrayList<>();
        orChildren.add(createColumnRangePredicate(colRef1, 10, 100));
        orChildren.add(createColumnRangePredicate(colRef2, 1.0, 10.0));
        OrRangePredicate orPred = new OrRangePredicate(orChildren);
        parameterizedPredicate = new ParameterizedPredicate(orPred.toScalarOperator());
        Assertions.assertFalse(parameterizedPredicate.isValidRangePredicate(orPred));

        List<RangePredicate> duplicateChildren = new ArrayList<>();
        duplicateChildren.add(createColumnRangePredicate(colRef1, 10, 100));
        duplicateChildren.add(createColumnRangePredicate(colRef1, 200, 300));
        AndRangePredicate duplicatePred = new AndRangePredicate(duplicateChildren);
        parameterizedPredicate = new ParameterizedPredicate(duplicatePred.toScalarOperator());
        Assertions.assertFalse(parameterizedPredicate.isValidRangePredicate(duplicatePred));
    }

    @Test
    public void testMergeRangeNullCases() {
        ColumnRangePredicate pred1 = createColumnRangePredicate(colRef1, 10, 100);
        ParameterizedPredicate parameterizedPredicate = new ParameterizedPredicate(pred1.toScalarOperator());
        Assertions.assertEquals(pred1, parameterizedPredicate.mergeRanges(pred1, null));
        Assertions.assertNull(parameterizedPredicate.mergeRanges(null, pred1));

        Assertions.assertNull(parameterizedPredicate.mergeRanges(null, null));

        ColumnRangePredicate pred2 = createColumnRangePredicate(colRef2, 1.0, 10.0);
        Assertions.assertNull(parameterizedPredicate.mergeRanges(pred1, pred2));
    }

    @Test
    public void testMergeRangeSinglePoints() {
        ColumnRangePredicate point1 = createColumnRangePredicate(colRef1, 10, 10);
        ColumnRangePredicate point2 = createColumnRangePredicate(colRef1, 10, 10);
        ParameterizedPredicate parameterizedPredicate1 = new ParameterizedPredicate(point1.toScalarOperator());
        ParameterizedPredicate parameterizedPredicate2 = new ParameterizedPredicate(point2.toScalarOperator());
        parameterizedPredicate1.mergeColumnRange(parameterizedPredicate2);

        ColumnRangePredicate merged = parameterizedPredicate1.getColumnRangePredicates().get(colRef1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        Range<ConstantOperator> range = merged.getColumnRanges().asRanges().iterator().next();
        Assertions.assertEquals(ConstantOperator.createInt(10), range.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(10), range.upperEndpoint());

        ColumnRangePredicate point3 = createColumnRangePredicate(colRef1, 10, 10);
        ColumnRangePredicate point4 = createColumnRangePredicate(colRef1, 20, 20);
        ParameterizedPredicate parameterizedPredicate3 = new ParameterizedPredicate(point3.toScalarOperator());
        ParameterizedPredicate parameterizedPredicate4 = new ParameterizedPredicate(point4.toScalarOperator());
        parameterizedPredicate3.mergeColumnRange(parameterizedPredicate4);
        merged = parameterizedPredicate3.getColumnRangePredicates().get(colRef1);

        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        range = merged.getColumnRanges().asRanges().iterator().next();
        Assertions.assertEquals(ConstantOperator.createInt(10), range.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(20), range.upperEndpoint());

        parameterizedPredicate3 = new ParameterizedPredicate(point4.toScalarOperator());
        parameterizedPredicate4 = new ParameterizedPredicate(point3.toScalarOperator());
        parameterizedPredicate3.mergeColumnRange(parameterizedPredicate4);
        merged = parameterizedPredicate3.getColumnRangePredicates().get(colRef1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        range = merged.getColumnRanges().asRanges().iterator().next();
        Assertions.assertEquals(ConstantOperator.createInt(10), range.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(20), range.upperEndpoint());
    }

    @Test
    public void testMergeRangePointAndRange() {
        ColumnRangePredicate range1 = createColumnRangePredicate(colRef1, 10, 100);
        ColumnRangePredicate point1 = createColumnRangePredicate(colRef1, 50, 50);
        ParameterizedPredicate parameterizedPredicate1 = new ParameterizedPredicate(range1.toScalarOperator());
        ParameterizedPredicate parameterizedPredicate2 = new ParameterizedPredicate(point1.toScalarOperator());
        parameterizedPredicate1.mergeColumnRange(parameterizedPredicate2);

        ColumnRangePredicate merged = parameterizedPredicate1.getColumnRangePredicates().get(colRef1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        Range<ConstantOperator> range = merged.getColumnRanges().asRanges().iterator().next();
        Assertions.assertEquals(ConstantOperator.createInt(10), range.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(100), range.upperEndpoint());

        ColumnRangePredicate range2 = createColumnRangePredicate(colRef1, 10, 100);
        ColumnRangePredicate point2 = createColumnRangePredicate(colRef1, 5, 5);
        parameterizedPredicate1 = new ParameterizedPredicate(range2.toScalarOperator());
        parameterizedPredicate2 = new ParameterizedPredicate(point2.toScalarOperator());
        parameterizedPredicate1.mergeColumnRange(parameterizedPredicate2);
        merged = parameterizedPredicate1.getColumnRangePredicates().get(colRef1);

        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        range = merged.getColumnRanges().asRanges().iterator().next();
        Assertions.assertEquals(ConstantOperator.createInt(5), range.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(100), range.upperEndpoint());

        ColumnRangePredicate range3 = createColumnRangePredicate(colRef1, 10, 100);
        ColumnRangePredicate point3 = createColumnRangePredicate(colRef1, 150, 150);
        ParameterizedPredicate parameterizedPredicate3 = new ParameterizedPredicate(range3.toScalarOperator());
        ParameterizedPredicate parameterizedPredicate4 = new ParameterizedPredicate(point3.toScalarOperator());
        parameterizedPredicate3.mergeColumnRange(parameterizedPredicate4);
        merged = parameterizedPredicate3.getColumnRangePredicates().get(colRef1);

        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        range = merged.getColumnRanges().asRanges().iterator().next();
        Assertions.assertEquals(ConstantOperator.createInt(10), range.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(150), range.upperEndpoint());
    }

    @Test
    public void testMergeRangeUnboundedRanges() {
        TreeRangeSet<ConstantOperator> rangeSet1 = TreeRangeSet.create();
        rangeSet1.add(Range.lessThan(ConstantOperator.createInt(100)));
        BinaryPredicateOperator expr1 = createBinaryPredicate(colRef1, BinaryType.LT, ConstantOperator.createInt(100));
        ColumnRangePredicate unboundedLower = new ColumnRangePredicate(expr1, rangeSet1);

        TreeRangeSet<ConstantOperator> rangeSet2 = TreeRangeSet.create();
        rangeSet2.add(Range.greaterThan(ConstantOperator.createInt(10)));
        BinaryPredicateOperator expr2 = createBinaryPredicate(colRef1, BinaryType.GT, ConstantOperator.createInt(10));
        ColumnRangePredicate unboundedUpper = new ColumnRangePredicate(expr2, rangeSet2);

        ColumnRangePredicate point = createColumnRangePredicate(colRef1, 50, 50);

        ParameterizedPredicate target = new ParameterizedPredicate(unboundedLower.toScalarOperator());
        ParameterizedPredicate source = new ParameterizedPredicate(point.toScalarOperator());
        target.mergeColumnRange(source);
        ColumnRangePredicate merged = target.getColumnRangePredicates().get(colRef1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        Assertions.assertEquals(100, merged.getColumnRanges().asRanges().iterator().next().upperEndpoint().getInt());

        target = new ParameterizedPredicate(unboundedUpper.toScalarOperator());
        source = new ParameterizedPredicate(point.toScalarOperator());
        target.mergeColumnRange(source);
        merged = target.getColumnRangePredicates().get(colRef1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());

        TreeRangeSet<ConstantOperator> rangeSet3 = TreeRangeSet.create();
        rangeSet3.add(Range.all());
        BinaryPredicateOperator expr3 = createBinaryPredicate(colRef1, BinaryType.EQ, ConstantOperator.createInt(1));
        ColumnRangePredicate unboundedAll = new ColumnRangePredicate(expr3, rangeSet3);

        merged = source.mergeRanges(unboundedAll, point);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(1, merged.getColumnRanges().asRanges().size());
        Assertions.assertFalse(merged.getColumnRanges().asRanges().iterator().next().hasLowerBound());
        Assertions.assertFalse(merged.getColumnRanges().asRanges().iterator().next().hasUpperBound());
    }

    @Test
    public void testMergeRangeMultipleRanges() {
        TreeRangeSet<ConstantOperator> rangeSet1 = TreeRangeSet.create();
        rangeSet1.add(Range.closed(ConstantOperator.createInt(10), ConstantOperator.createInt(20)));
        rangeSet1.add(Range.closed(ConstantOperator.createInt(30), ConstantOperator.createInt(40)));
        BinaryPredicateOperator expr1 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));
        ColumnRangePredicate multiRange1 = new ColumnRangePredicate(expr1, rangeSet1);

        TreeRangeSet<ConstantOperator> rangeSet2 = TreeRangeSet.create();
        rangeSet2.add(Range.closed(ConstantOperator.createInt(15), ConstantOperator.createInt(25)));
        rangeSet2.add(Range.closed(ConstantOperator.createInt(35), ConstantOperator.createInt(45)));
        BinaryPredicateOperator expr2 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(15));
        ColumnRangePredicate multiRange2 = new ColumnRangePredicate(expr2, rangeSet2);

        ParameterizedPredicate target = new ParameterizedPredicate(multiRange1.toScalarOperator());
        ParameterizedPredicate source = new ParameterizedPredicate(multiRange2.toScalarOperator());
        target.mergeColumnRange(source);
        ColumnRangePredicate merged = target.getColumnRangePredicates().get(colRef1);

        Assertions.assertNotNull(merged);
        Assertions.assertEquals(2, merged.getColumnRanges().asRanges().size());
        List<Range<ConstantOperator>> ranges = Lists.newArrayList(merged.getColumnRanges().asRanges());
        Range<ConstantOperator> range1 = ranges.get(0);
        Assertions.assertEquals(ConstantOperator.createInt(10), range1.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(25), range1.upperEndpoint());

        Range<ConstantOperator> range2 = ranges.get(1);
        Assertions.assertEquals(ConstantOperator.createInt(30), range2.lowerEndpoint());
        Assertions.assertEquals(ConstantOperator.createInt(45), range2.upperEndpoint());
    }

    @Test
    public void testMergePredicateRangeSimplePredicates() {
        BinaryPredicateOperator pred1 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));
        BinaryPredicateOperator pred2 = createBinaryPredicate(colRef1, BinaryType.LE, ConstantOperator.createInt(100));
        ParameterizedPredicate target = new ParameterizedPredicate(pred1);
        ParameterizedPredicate source = new ParameterizedPredicate(pred2);
        target.mergeColumnRange(source);
        ColumnRangePredicate merged = target.getColumnRangePredicates().get(colRef1);
        ScalarOperator predicates = merged.toScalarOperator();
        Assertions.assertTrue(predicates.isTrue());
    }

    @Test
    public void testMergePredicateRangeCompoundPredicates() {
        BinaryPredicateOperator intPred1 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));
        BinaryPredicateOperator doublePred1 = createBinaryPredicate(colRef2, BinaryType.GE, ConstantOperator.createDouble(1.0));
        CompoundPredicateOperator compound1 = createCompoundPredicate(CompoundType.AND, intPred1, doublePred1);

        BinaryPredicateOperator intPred2 = createBinaryPredicate(colRef1, BinaryType.LE, ConstantOperator.createInt(100));
        BinaryPredicateOperator doublePred2 = createBinaryPredicate(colRef2, BinaryType.LE, ConstantOperator.createDouble(10.0));
        CompoundPredicateOperator compound2 = createCompoundPredicate(CompoundType.AND, intPred2, doublePred2);

        ParameterizedPredicate target = new ParameterizedPredicate(compound1);
        ParameterizedPredicate source = new ParameterizedPredicate(compound2);
        target.mergeColumnRange(source);
        ColumnRangePredicate merged = target.getColumnRangePredicates().get(colRef1);
        ScalarOperator predicates = merged.toScalarOperator();
        Assertions.assertTrue(predicates.isTrue());
    }

    @Test
    public void testMergePredicateRangeNestedCompoundPredicates() {
        BinaryPredicateOperator intPred1 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));
        BinaryPredicateOperator doublePred1 = createBinaryPredicate(colRef2, BinaryType.GE, ConstantOperator.createDouble(1.0));
        CompoundPredicateOperator innerCompound1 = createCompoundPredicate(CompoundType.AND, intPred1, doublePred1);
        BinaryPredicateOperator bigintPred1 = createBinaryPredicate(colRef3, BinaryType.GE, ConstantOperator.createBigint(100));
        CompoundPredicateOperator outerCompound1 = createCompoundPredicate(CompoundType.AND, innerCompound1, bigintPred1);

        BinaryPredicateOperator intPred2 = createBinaryPredicate(colRef1, BinaryType.LE, ConstantOperator.createInt(100));
        BinaryPredicateOperator doublePred2 = createBinaryPredicate(colRef2, BinaryType.LE, ConstantOperator.createDouble(10.0));
        CompoundPredicateOperator innerCompound2 = createCompoundPredicate(CompoundType.AND, intPred2, doublePred2);
        BinaryPredicateOperator bigintPred2 = createBinaryPredicate(colRef3, BinaryType.LE, ConstantOperator.createBigint(1000));
        CompoundPredicateOperator outerCompound2 = createCompoundPredicate(CompoundType.AND, innerCompound2, bigintPred2);

        ParameterizedPredicate target = new ParameterizedPredicate(outerCompound1);
        ParameterizedPredicate source = new ParameterizedPredicate(outerCompound2);
        target.mergeColumnRange(source);
        Assertions.assertTrue(target.getColumnRangePredicates().get(colRef1).toScalarOperator().isTrue());
        Assertions.assertTrue(target.getColumnRangePredicates().get(colRef2).toScalarOperator().isTrue());
        Assertions.assertTrue(target.getColumnRangePredicates().get(colRef3).toScalarOperator().isTrue());
    }

    @Test
    public void testMergePredicateRangeWithOrPredicates() {
        BinaryPredicateOperator intPred1 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));
        BinaryPredicateOperator doublePred1 = createBinaryPredicate(colRef2, BinaryType.GE, ConstantOperator.createDouble(1.0));
        CompoundPredicateOperator orPred = createCompoundPredicate(CompoundType.OR, intPred1, doublePred1);

        BinaryPredicateOperator intPred2 = createBinaryPredicate(colRef1, BinaryType.LE, ConstantOperator.createInt(100));
        ParameterizedPredicate target = new ParameterizedPredicate(orPred);
        ParameterizedPredicate source = new ParameterizedPredicate(intPred2);
        target.mergeColumnRange(source);
        Assertions.assertTrue(target.getColumnRangePredicates().isEmpty());
    }

    @Test
    public void testMergePredicateRangeWithDuplicateColumns() {
        BinaryPredicateOperator intPred1 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));
        BinaryPredicateOperator intPred2 = createBinaryPredicate(colRef1, BinaryType.LE, ConstantOperator.createInt(100));
        CompoundPredicateOperator compound = createCompoundPredicate(CompoundType.AND, intPred1, intPred2);

        BinaryPredicateOperator intPred3 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(20));
        BinaryPredicateOperator intPred4 = createBinaryPredicate(colRef1, BinaryType.LE, ConstantOperator.createInt(80));
        CompoundPredicateOperator compound2 = createCompoundPredicate(CompoundType.AND, intPred3, intPred4);
        ParameterizedPredicate target = new ParameterizedPredicate(compound);
        ParameterizedPredicate source = new ParameterizedPredicate(compound2);
        target.mergeColumnRange(source);
        ColumnRangePredicate merged = target.getColumnRangePredicates().get(colRef1);
        Assertions.assertEquals(compound, merged.toScalarOperator());
    }

    @Test
    public void testMergeRangeWithStringType() {
        BinaryPredicateOperator pred1 = createBinaryPredicate(stringCol, BinaryType.GE, ConstantOperator.createVarchar("appl"));
        BinaryPredicateOperator pred2 = createBinaryPredicate(stringCol, BinaryType.LE, ConstantOperator.createVarchar("orang"));

        ParameterizedPredicate target = new ParameterizedPredicate(pred1);
        ParameterizedPredicate source = new ParameterizedPredicate(pred2);
        target.mergeColumnRange(source);
        Assertions.assertTrue(target.getColumnRangePredicates().get(stringCol).toScalarOperator().isTrue());
    }

    @Test
    public void testMergeRangeWithDateType() {
        LocalDateTime startDate = LocalDateTime.of(2023, 1, 1, 0, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(2023, 12, 31, 0, 0, 0);
        BinaryPredicateOperator pred1 = createBinaryPredicate(dateCol, BinaryType.GE,
                ConstantOperator.createDate(startDate));
        BinaryPredicateOperator pred2 = createBinaryPredicate(dateCol, BinaryType.LE,
                ConstantOperator.createDate(endDate));
        ParameterizedPredicate target = new ParameterizedPredicate(pred1);
        ParameterizedPredicate source = new ParameterizedPredicate(pred2);
        target.mergeColumnRange(source);
        Assertions.assertTrue(target.getColumnRangePredicates().get(dateCol).toScalarOperator().isTrue());
    }

    @Test
    public void testMergeRangeWithDateTimeType() {
        LocalDateTime startDateTime = LocalDateTime.of(2023, 1, 1, 0, 0, 0);
        LocalDateTime endDateTime = LocalDateTime.of(2022, 12, 31, 23, 59, 59);

        BinaryPredicateOperator pred1 = createBinaryPredicate(datetimeCol, BinaryType.GE,
                ConstantOperator.createDatetime(startDateTime));
        BinaryPredicateOperator pred2 = createBinaryPredicate(datetimeCol, BinaryType.LE,
                ConstantOperator.createDatetime(endDateTime));

        ParameterizedPredicate target = new ParameterizedPredicate(pred1);
        ParameterizedPredicate source = new ParameterizedPredicate(pred2);
        target.mergeColumnRange(source);
        Assertions.assertEquals(2, target.getColumnRangePredicates().get(datetimeCol).getColumnRanges().asRanges().size());
    }

    @Test
    public void testMergeRangeWithStr2DateFunction() {
        CallOperator str2dateCall = createStr2DateCall(stringDateCol, "%Y-%m-%d");

        LocalDateTime startDate = LocalDateTime.of(2023, 1, 1, 0, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(2023, 12, 31, 0, 0, 0);

        BinaryPredicateOperator pred1 = new BinaryPredicateOperator(BinaryType.GE, str2dateCall,
                ConstantOperator.createDate(startDate));
        BinaryPredicateOperator pred2 = new BinaryPredicateOperator(BinaryType.GE, str2dateCall,
                ConstantOperator.createDate(endDate));
        ParameterizedPredicate target = new ParameterizedPredicate(pred1);
        ParameterizedPredicate source = new ParameterizedPredicate(pred2);
        target.mergeColumnRange(source);
        Assertions.assertEquals(ConstantOperator.createVarchar("2023-01-01").toString(),
                target.getColumnRangePredicates().get(stringDateCol).getColumnRanges().asRanges().iterator().next()
                        .lowerEndpoint().toString());
    }

    @Test
    public void testMergeRangeWithMixedTypes() {
        BinaryPredicateOperator intPred = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));
        BinaryPredicateOperator stringPred = createBinaryPredicate(stringCol, BinaryType.GE,
                ConstantOperator.createVarchar("apple"));
        BinaryPredicateOperator datePred = createBinaryPredicate(dateCol, BinaryType.GE,
                ConstantOperator.createDate(LocalDateTime.of(2023, 1, 1, 0, 0, 0)));

        CompoundPredicateOperator compound1 = createCompoundPredicate(CompoundType.AND, intPred, stringPred);
        CompoundPredicateOperator mixedPred1 = createCompoundPredicate(CompoundType.AND, compound1, datePred);

        BinaryPredicateOperator intPred2 = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(100));
        BinaryPredicateOperator stringPred2 = createBinaryPredicate(stringCol, BinaryType.GE,
                ConstantOperator.createVarchar("orange"));
        BinaryPredicateOperator datePred2 = createBinaryPredicate(dateCol, BinaryType.GE,
                ConstantOperator.createDate(LocalDateTime.of(2023, 12, 31, 0, 0, 0)));

        CompoundPredicateOperator compound2 = createCompoundPredicate(CompoundType.AND, intPred2, stringPred2);
        CompoundPredicateOperator mixedPred2 = createCompoundPredicate(CompoundType.AND, compound2, datePred2);
        ParameterizedPredicate target = new ParameterizedPredicate(mixedPred1);
        ParameterizedPredicate source = new ParameterizedPredicate(mixedPred2);
        target.mergeColumnRange(source);
        Range<ConstantOperator> intColRange = target.getColumnRangePredicates().get(colRef1).getColumnRanges()
                .asRanges().iterator().next();
        Range<ConstantOperator> stringColRange = target.getColumnRangePredicates().get(stringCol).getColumnRanges()
                .asRanges().iterator().next();
        Range<ConstantOperator> dateColRange = target.getColumnRangePredicates().get(dateCol).getColumnRanges()
                .asRanges().iterator().next();
        Assertions.assertEquals(10, intColRange.lowerEndpoint().getInt());
        Assertions.assertEquals("apple", stringColRange.lowerEndpoint().toString());
        Assertions.assertEquals("2023-01-01", dateColRange.lowerEndpoint().toString());

    }

    @Test
    public void testMergeRangeWithEmptyRanges() {
        BinaryPredicateOperator pred1 = createBinaryPredicate(colRef1, BinaryType.GT, ConstantOperator.createInt(100));
        BinaryPredicateOperator pred2 = createBinaryPredicate(colRef1, BinaryType.LT, ConstantOperator.createInt(50));
        ParameterizedPredicate target = new ParameterizedPredicate(pred1);
        ParameterizedPredicate source = new ParameterizedPredicate(pred2);
        target.mergeColumnRange(source);
        Assertions.assertEquals(2, target.getColumnRangePredicates().get(colRef1).getColumnRanges().asRanges().size());
    }

    @Test
    public void testParameterizedMode() {
        ScanNode scanNode = createScanNode(null);
        Assertions.assertFalse(scanNode.isEnableParameterizedMode());

        scanNode.enableParameterizedMode();
        Assertions.assertTrue(scanNode.isEnableParameterizedMode());

        scanNode.disableParameterizedMode();
        Assertions.assertFalse(scanNode.isEnableParameterizedMode());
    }

    @Test
    public void testDifferentBinaryPredicateTypes() {
        BinaryPredicateOperator eqPred = createBinaryPredicate(colRef1, BinaryType.EQ, ConstantOperator.createInt(50));
        BinaryPredicateOperator nePred = createBinaryPredicate(colRef1, BinaryType.NE, ConstantOperator.createInt(50));
        BinaryPredicateOperator ltPred = createBinaryPredicate(colRef1, BinaryType.LT, ConstantOperator.createInt(100));
        BinaryPredicateOperator lePred = createBinaryPredicate(colRef1, BinaryType.LE, ConstantOperator.createInt(100));
        BinaryPredicateOperator gtPred = createBinaryPredicate(colRef1, BinaryType.GT, ConstantOperator.createInt(10));
        BinaryPredicateOperator gePred = createBinaryPredicate(colRef1, BinaryType.GE, ConstantOperator.createInt(10));

        ParameterizedPredicate target = new ParameterizedPredicate(eqPred);
        ParameterizedPredicate source = new ParameterizedPredicate(ltPred);
        target.mergeColumnRange(source);
        Assertions.assertEquals("1: int_col < 100", target.getColumnRangePredicates().get(colRef1).toScalarOperator().toString());

        target = new ParameterizedPredicate(gePred);
        source = new ParameterizedPredicate(lePred);
        target.mergeColumnRange(source);
        Assertions.assertEquals(ConstantOperator.TRUE, target.getColumnRangePredicates().get(colRef1).toScalarOperator());

        target = new ParameterizedPredicate(gtPred);
        source = new ParameterizedPredicate(nePred);
        target.mergeColumnRange(source);
        Assertions.assertEquals(ConstantOperator.TRUE, target.getColumnRangePredicates().get(colRef1).toScalarOperator());
    }
}


