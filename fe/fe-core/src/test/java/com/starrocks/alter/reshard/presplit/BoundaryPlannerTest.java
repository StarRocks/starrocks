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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintTuple;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.compositeTuple;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.varcharColumn;

public class BoundaryPlannerTest {

    private static SampleSet sampleSetOf(List<Tuple> tuples) {
        return new SampleSet(tuples, new Estimates(tuples.size() * 64L, tuples.size()));
    }

    @Test
    public void testUniformSampleWithFourTablets() throws Exception {
        // Sample = 0..99; tabletCount=4; expect cuts at row 25, 50, 75 → values 25, 50, 75.
        List<Tuple> tuples = new ArrayList<>(100);
        for (long value = 0; value < 100; value++) {
            tuples.add(bigintTuple(value));
        }
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 4, List.of(bigintColumn("k")));

        Assertions.assertEquals(4, result.getEffectiveTabletCount());
        Assertions.assertEquals(3, result.getBoundaries().size());
        Assertions.assertEquals(bigintTuple(25), result.getBoundaries().get(0));
        Assertions.assertEquals(bigintTuple(50), result.getBoundaries().get(1));
        Assertions.assertEquals(bigintTuple(75), result.getBoundaries().get(2));
    }

    @Test
    public void testZipfSkewWithMinorityHeavyTailProducesCuts() throws Exception {
        // 40% of rows have key=0 (the minimum), the rest are spread 1..60. tabletCount=4.
        // The 0-valued first quartile candidate is filtered out; the remaining 2 cuts
        // straddle the value range and are strictly increasing.
        List<Tuple> tuples = new ArrayList<>(100);
        for (int i = 0; i < 40; i++) {
            tuples.add(bigintTuple(0));
        }
        for (long value = 1; value <= 60; value++) {
            tuples.add(bigintTuple(value));
        }
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 4, List.of(bigintColumn("k")));

        Assertions.assertTrue(
                result.getEffectiveTabletCount() >= 2 && result.getEffectiveTabletCount() <= 4,
                "effectiveTabletCount=" + result.getEffectiveTabletCount());
        List<Tuple> boundaries = result.getBoundaries();
        Assertions.assertFalse(boundaries.isEmpty(), "expected at least one boundary above the minimum");
        for (Tuple boundary : boundaries) {
            Assertions.assertTrue(boundary.compareTo(bigintTuple(0)) > 0,
                    "no boundary should equal the minimum, got: " + boundary);
        }
        for (int i = 1; i < boundaries.size(); i++) {
            Assertions.assertTrue(boundaries.get(i - 1).compareTo(boundaries.get(i)) < 0,
                    "boundaries not strictly increasing: " + boundaries);
        }
    }

    @Test
    public void testHeavyMinimumPrefixStillProducesUsefulCuts() throws Exception {
        // 80 rows at value 0, then 20 distinct values 1..20. tabletCount=4 → quantile
        // positions 25, 50, 75 all land in the zero prefix. The planner must scan forward
        // past the duplicates and emit cuts at the next distinct keys.
        List<Tuple> tuples = new ArrayList<>(100);
        for (int i = 0; i < 80; i++) {
            tuples.add(bigintTuple(0));
        }
        for (long value = 1; value <= 20; value++) {
            tuples.add(bigintTuple(value));
        }
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 4, List.of(bigintColumn("k")));

        Assertions.assertFalse(result.isNoSplit(),
                "heavy-minimum prefix with distinct tail should still produce cuts");
        List<Tuple> boundaries = result.getBoundaries();
        for (Tuple boundary : boundaries) {
            Assertions.assertTrue(boundary.compareTo(bigintTuple(0)) > 0,
                    "no boundary should equal the minimum, got: " + boundary);
        }
        for (int i = 1; i < boundaries.size(); i++) {
            Assertions.assertTrue(boundaries.get(i - 1).compareTo(boundaries.get(i)) < 0,
                    "boundaries not strictly increasing: " + boundaries);
        }
    }

    @Test
    public void testAllEqualSampleIsNoSplit() throws Exception {
        // Every quantile picks the same (minimum) tuple; emitting it as a boundary
        // would create an empty leading tablet (-inf, 42), so it is filtered out.
        List<Tuple> tuples = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            tuples.add(bigintTuple(42));
        }
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 4, List.of(bigintColumn("k")));

        Assertions.assertTrue(result.isNoSplit(), "expected no useful split for an all-equal sample");
        Assertions.assertEquals(1, result.getEffectiveTabletCount());
    }

    @Test
    public void testDuplicateMinimumPrefixDropsLeadingBoundary() throws Exception {
        // Sample = [1, 1, 1, 5, 9, 9, 9]; tabletCount=4 → quantile positions 1, 3, 5 → values 1, 5, 9.
        // The boundary at 1 equals the minimum and would create an empty leading tablet, so drop it.
        List<Tuple> tuples = List.of(
                bigintTuple(1), bigintTuple(1), bigintTuple(1),
                bigintTuple(5),
                bigintTuple(9), bigintTuple(9), bigintTuple(9));
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 4, List.of(bigintColumn("k")));

        Assertions.assertEquals(3, result.getEffectiveTabletCount());
        Assertions.assertEquals(List.of(bigintTuple(5), bigintTuple(9)), result.getBoundaries());
    }

    @Test
    public void testRequestedTabletsExceedingSampleSizeDoesNotEmitMinimum() throws Exception {
        // sampleSize=2, tabletCount=4 → quantile positions 0, 1, 1 → tuples 1, 5, 5.
        // The 0-position value equals the minimum (1) and is filtered.
        List<Tuple> tuples = List.of(bigintTuple(1), bigintTuple(5));
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 4, List.of(bigintColumn("k")));

        Assertions.assertEquals(2, result.getEffectiveTabletCount());
        Assertions.assertEquals(List.of(bigintTuple(5)), result.getBoundaries());
    }

    @Test
    public void testSingleRowSampleIsNoSplit() throws Exception {
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(List.of(bigintTuple(7))), 4, List.of(bigintColumn("k")));

        Assertions.assertEquals(1, result.getEffectiveTabletCount());
        Assertions.assertTrue(result.isNoSplit());
        Assertions.assertTrue(result.getBoundaries().isEmpty());
    }

    @Test
    public void testEmptySampleIsNoSplit() throws Exception {
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                SampleSet.EMPTY, 4, List.of(bigintColumn("k")));

        Assertions.assertEquals(1, result.getEffectiveTabletCount());
        Assertions.assertTrue(result.isNoSplit());
    }

    @Test
    public void testTwoTabletsProduceSingleMidpointBoundary() throws Exception {
        // tabletCount=2 → one cut at row sampleSize/2.
        List<Tuple> tuples = new ArrayList<>();
        for (long value = 0; value < 10; value++) {
            tuples.add(bigintTuple(value));
        }
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 2, List.of(bigintColumn("k")));

        Assertions.assertEquals(2, result.getEffectiveTabletCount());
        Assertions.assertEquals(1, result.getBoundaries().size());
        Assertions.assertEquals(bigintTuple(5), result.getBoundaries().get(0));
    }

    @Test
    public void testSixtyFourTabletsOnLargeUniformSample() throws Exception {
        // 6400-row uniform sample; tabletCount=64 → 63 boundaries, all distinct, evenly spaced.
        List<Tuple> tuples = new ArrayList<>(6400);
        for (long value = 0; value < 6400; value++) {
            tuples.add(bigintTuple(value));
        }
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 64, List.of(bigintColumn("k")));

        Assertions.assertEquals(64, result.getEffectiveTabletCount());
        Assertions.assertEquals(63, result.getBoundaries().size());
        for (int i = 1; i < 63; i++) {
            Assertions.assertTrue(
                    result.getBoundaries().get(i - 1).compareTo(result.getBoundaries().get(i)) < 0);
        }
        for (int i = 1; i <= 63; i++) {
            long expected = (long) i * 6400 / 64;
            Assertions.assertEquals(bigintTuple(expected), result.getBoundaries().get(i - 1));
        }
    }

    @Test
    public void testArityMismatchRejected() {
        // Tuple has 2 values but schema is 1 column.
        Tuple malformed = new Tuple(List.of(
                Variant.of(IntegerType.BIGINT, "1"),
                Variant.of(IntegerType.BIGINT, "2")));
        List<Tuple> tuples = List.of(malformed);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> BoundaryPlanner.planRowQuantileBoundaries(
                        sampleSetOf(tuples), 4, List.of(bigintColumn("k"))));
        Assertions.assertTrue(exception.getMessage().contains("arity"),
                "expected arity error, got: " + exception.getMessage());
    }

    /**
     * Test-only Variant subclass with a settable decimal Type. Production code
     * has no DecimalVariant today — Variant.of(Type, String) rejects decimal
     * primitive types — so we have to hand-roll one to exercise the planner's
     * decimal precision/scale validator. When DecimalVariant lands, this can
     * be replaced with the production class.
     */
    private static final class TestDecimalVariant extends Variant {
        TestDecimalVariant(ScalarType type) {
            super(type);
        }

        @Override
        public String getStringValue() {
            return "";
        }

        @Override
        public long getLongValue() {
            return 0L;
        }

        @Override
        protected int compareToImpl(Variant other) {
            return 0;
        }
    }

    @Test
    public void testDecimalScaleMismatchRejected() {
        // Schema column is DECIMAL(10, 4); tuple value is DECIMAL(10, 2).
        ScalarType schemaType = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 4);
        ScalarType variantType = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        Column schemaColumn = new Column("d", schemaType);

        Tuple malformed = new Tuple(List.of(new TestDecimalVariant(variantType)));
        List<Tuple> tuples = List.of(malformed);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> BoundaryPlanner.planRowQuantileBoundaries(
                        sampleSetOf(tuples), 2, List.of(schemaColumn)));
        Assertions.assertTrue(exception.getMessage().toLowerCase().contains("scale")
                        || exception.getMessage().toLowerCase().contains("precision"),
                "expected decimal-precision-or-scale error, got: " + exception.getMessage());
    }

    @Test
    public void testDecimalMatchingPrecisionAndScaleAccepted() throws Exception {
        // Same precision and scale on schema + variant — planner should accept (no schema-mismatch error).
        ScalarType decimalType = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 4);
        Column schemaColumn = new Column("d", decimalType);

        List<Tuple> tuples = List.of(
                new Tuple(List.of(new TestDecimalVariant(decimalType))),
                new Tuple(List.of(new TestDecimalVariant(decimalType))));
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 2, List.of(schemaColumn));

        // All sampled tuples are equal under the stub compareToImpl → the only candidate cut
        // equals the minimum → filtered out → NO_SPLIT.
        Assertions.assertTrue(result.isNoSplit());
    }

    @Test
    public void testNullTupleRejectedAtSampleSetConstruction() {
        // ImmutableList.copyOf inside SampleSet rejects null entries, so the planner
        // never has to defend against them.
        List<Tuple> withNull = new ArrayList<>();
        withNull.add(null);
        Assertions.assertThrows(NullPointerException.class, () -> sampleSetOf(withNull));
    }

    @Test
    public void testPartialTupleRejected() {
        // Composite sort key {varchar, bigint} but tuple has only 1 value.
        Tuple partial = new Tuple(List.of(Variant.of(VarcharType.VARCHAR, "t000000")));
        List<Tuple> tuples = List.of(partial);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> BoundaryPlanner.planRowQuantileBoundaries(
                        sampleSetOf(tuples), 2, List.of(varcharColumn("g"), bigintColumn("p"))));
        Assertions.assertTrue(exception.getMessage().contains("arity"),
                "expected arity error, got: " + exception.getMessage());
    }

    @Test
    public void testCompositeKeyUniform() throws Exception {
        // Composite key (varchar, bigint), 60 rows = 6 tenants × 10 positions.
        // tabletCount=3 → 2 boundaries at row 20 and 40 → (tenant_000002, 0) and (tenant_000004, 0).
        List<Tuple> tuples = new ArrayList<>(60);
        for (int tenant = 0; tenant < 6; tenant++) {
            for (int position = 0; position < 10; position++) {
                tuples.add(compositeTuple(String.format("tenant_%06d", tenant), position));
            }
        }
        BoundaryPlannerResult result = BoundaryPlanner.planRowQuantileBoundaries(
                sampleSetOf(tuples), 3, List.of(varcharColumn("g"), bigintColumn("p")));

        Assertions.assertEquals(3, result.getEffectiveTabletCount());
        Assertions.assertEquals(2, result.getBoundaries().size());
        Assertions.assertEquals(compositeTuple("tenant_000002", 0), result.getBoundaries().get(0));
        Assertions.assertEquals(compositeTuple("tenant_000004", 0), result.getBoundaries().get(1));
    }

    @Test
    public void testRequestedTabletCountBelowTwoIsRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> BoundaryPlanner.planRowQuantileBoundaries(
                        sampleSetOf(Collections.emptyList()), 1, List.of(bigintColumn("k"))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> BoundaryPlanner.planRowQuantileBoundaries(
                        sampleSetOf(Collections.emptyList()), 0, List.of(bigintColumn("k"))));
    }

    @Test
    public void testEmptySortKeyRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> BoundaryPlanner.planRowQuantileBoundaries(
                        sampleSetOf(List.of(bigintTuple(0))), 2, Collections.emptyList()));
    }
}
