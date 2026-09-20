// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.analyzer.PartitionDescAnalyzer;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public class RangePartitionInfoIndexTest {

    private static List<Column> intColumns() {
        return Lists.newArrayList(new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", ""));
    }

    private static PartitionKey intKey(List<Column> cols, int v) throws AnalysisException {
        return PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue(Integer.toString(v))), cols);
    }

    private static Range<PartitionKey> intRange(List<Column> cols, int lo, int hi) throws AnalysisException {
        return Range.closedOpen(intKey(cols, lo), intKey(cols, hi));
    }

    /**
     * The invariant every write point must keep: the index holds exactly the map's entries, ordered by lower
     * endpoint. getSortedRangeMap returns the index contents, so it is the window used to check this.
     */
    static void assertIndexMatches(RangePartitionInfo info, boolean isTemp) {
        List<Map.Entry<Long, Range<PartitionKey>>> expected = Lists.newArrayList(info.getIdToRange(isTemp).entrySet());
        expected.sort(RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        List<Map.Entry<Long, Range<PartitionKey>>> actual = info.getSortedRangeMap(isTemp);
        Assertions.assertEquals(expected.size(), actual.size(), "index size, temp=" + isTemp);
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i).getKey(), actual.get(i).getKey(), "id at " + i);
            Assertions.assertEquals(expected.get(i).getValue(), actual.get(i).getValue(), "range at " + i);
        }
    }

    static void assertBothIndexesMatch(RangePartitionInfo info) {
        assertIndexMatches(info, false);
        assertIndexMatches(info, true);
    }

    @Test
    public void testIndexFollowsEveryWritePoint() throws Exception {
        List<Column> cols = intColumns();
        RangePartitionInfo info = new RangePartitionInfo(cols);
        assertBothIndexesMatch(info);

        // add formal partitions out of order
        info.addPartition(3L, false, intRange(cols, 30, 40), null, (short) 1, null);
        info.addPartition(1L, false, intRange(cols, 10, 20), null, (short) 1, null);
        info.addPartition(2L, false, intRange(cols, 20, 30), null, (short) 1, null);
        assertBothIndexesMatch(info);

        // temp partitions may reuse formal ranges: they live in their own index
        info.addPartition(11L, true, intRange(cols, 10, 20), null, (short) 1, null);
        info.addPartition(12L, true, intRange(cols, 20, 30), null, (short) 1, null);
        assertBothIndexesMatch(info);

        // re-setting an existing id must move it in the index, not leave a second entry behind
        // (RangePartitionInfoTest.testFixedRange reuses one partition id for every desc)
        info.setRange(3L, false, intRange(cols, 50, 60));
        assertBothIndexesMatch(info);

        // drop formal, drop temp
        info.dropPartition(2L);
        assertBothIndexesMatch(info);
        info.dropPartition(12L);
        assertBothIndexesMatch(info);

        // move temp -> formal (the formal partition covering [10,20) is dropped first, as REPLACE PARTITION does)
        info.dropPartition(1L);
        info.moveRangeFromTempToFormal(11L);
        assertBothIndexesMatch(info);
        Assertions.assertNull(info.getIdToRange(true).get(11L));
        Assertions.assertEquals(intRange(cols, 10, 20), info.getIdToRange(false).get(11L));

        // dropping an unknown id is a no-op
        info.dropPartition(999L);
        assertBothIndexesMatch(info);
    }

    @Test
    public void testIndexSurvivesCopyCloneGsonAndRestore() throws Exception {
        List<Column> cols = intColumns();
        RangePartitionInfo info = new RangePartitionInfo(cols);
        info.addPartition(1L, false, intRange(cols, 10, 20), null, (short) 1, null);
        info.addPartition(2L, false, intRange(cols, 20, 30), null, (short) 1, null);
        info.addPartition(21L, true, intRange(cols, 20, 30), null, (short) 1, null);

        // copy constructor
        RangePartitionInfo copied = new RangePartitionInfo(info);
        assertBothIndexesMatch(copied);

        // clone must not share index instances with the original: dropping from one must not disturb the other,
        // which assertBothIndexesMatch detects as an index/map mismatch on whichever side is wrong
        RangePartitionInfo cloned = (RangePartitionInfo) info.clone();
        assertBothIndexesMatch(cloned);
        info.dropPartition(1L);
        assertBothIndexesMatch(info);
        assertBothIndexesMatch(cloned);

        // gson round trip rebuilds from serializedIdToRange / serializedIdToTempRange
        String json = GsonUtils.GSON.toJson(cloned);
        RangePartitionInfo loaded = GsonUtils.GSON.fromJson(json, RangePartitionInfo.class);
        assertBothIndexesMatch(loaded);

        // restore remaps ids; index values must carry the new ids
        Map<Long, Long> oldToNew = Maps.newHashMap();
        oldToNew.put(1L, 101L);
        oldToNew.put(2L, 102L);
        oldToNew.put(21L, 121L);
        loaded.setPartitionIdsForRestore(oldToNew);
        assertBothIndexesMatch(loaded);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Oracle: verbatim copy of the pre-index linear implementation (RangePartitionInfo.createAndCheckNewRange +
    // checkNewRange at commit e6dfe146ac9). Only lives here so the differential tests can prove equivalence.
    // ---------------------------------------------------------------------------------------------------------

    static Range<PartitionKey> oracleCreateAndCheckNewRange(RangePartitionInfo info, Map<ColumnId, Column> schema,
                                                            PartitionKeyDesc partKeyDesc,
                                                            List<Map.Entry<Long, Range<PartitionKey>>> sortedRanges)
            throws AnalysisException, DdlException {
        Range<PartitionKey> newRange = null;
        List<Column> partitionColumns = info.getPartitionColumns(schema);
        PartitionKey newRangeUpper;
        if (partKeyDesc.isMax()) {
            newRangeUpper = PartitionKey.createInfinityPartitionKey(partitionColumns, true);
        } else {
            newRangeUpper = PartitionKey.createPartitionKey(partKeyDesc.getUpperValues(), partitionColumns);
        }
        if (newRangeUpper.isMinValue()) {
            throw new DdlException("Partition's upper value should not be MIN VALUE: " + partKeyDesc);
        }
        Range<PartitionKey> lastRange = null;
        Range<PartitionKey> currentRange = null;
        for (Map.Entry<Long, Range<PartitionKey>> entry : sortedRanges) {
            currentRange = entry.getValue();
            PartitionKey upperKey = currentRange.upperEndpoint();
            if (upperKey.compareTo(newRangeUpper) >= 0) {
                newRange = oracleCheckNewRange(partitionColumns, partKeyDesc, newRangeUpper, lastRange, currentRange);
                break;
            } else {
                lastRange = currentRange;
            }
        }
        if (newRange == null) {
            newRange = oracleCheckNewRange(partitionColumns, partKeyDesc, newRangeUpper, lastRange, currentRange);
        }
        return newRange;
    }

    static Range<PartitionKey> oracleCheckNewRange(List<Column> partitionColumns, PartitionKeyDesc partKeyDesc,
                                                   PartitionKey newRangeUpper, Range<PartitionKey> lastRange,
                                                   Range<PartitionKey> currentRange)
            throws AnalysisException, DdlException {
        PartitionKey lowKey;
        if (partKeyDesc.hasLowerValues()) {
            lowKey = PartitionKey.createPartitionKey(partKeyDesc.getLowerValues(), partitionColumns);
        } else if (lastRange == null) {
            lowKey = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
        } else {
            lowKey = lastRange.upperEndpoint();
        }
        if (lowKey.compareTo(newRangeUpper) >= 0) {
            throw new AnalysisException("The lower values must smaller than upper values");
        }
        Range<PartitionKey> newRange = Range.closedOpen(lowKey, newRangeUpper);
        if (lastRange != null) {
            RangeUtils.checkRangeIntersect(newRange, lastRange);
        }
        if (currentRange != null) {
            RangeUtils.checkRangeIntersect(newRange, currentRange);
        }
        return newRange;
    }

    /** Same wrapping as RangePartitionInfo.checkAndCreateRange(schema, desc, isTemp). */
    static String oracleOutcome(RangePartitionInfo info, List<Column> cols, SingleRangePartitionDesc desc,
                                boolean isTemp) {
        List<Map.Entry<Long, Range<PartitionKey>>> sorted = Lists.newArrayList(info.getIdToRange(isTemp).entrySet());
        sorted.sort(RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        try {
            return "RANGE " + oracleCreateAndCheckNewRange(info, MetaUtils.buildIdToColumn(cols),
                    desc.getPartitionKeyDesc(), sorted);
        } catch (AnalysisException e) {
            return "DDL Invalid range value format: " + e.getMessage();
        } catch (DdlException e) {
            return "DDL " + e.getMessage();
        } catch (IllegalArgumentException | IllegalStateException e) {
            return e.getClass().getSimpleName();
        }
    }

    static String newOutcome(RangePartitionInfo info, List<Column> cols, SingleRangePartitionDesc desc,
                             boolean isTemp) {
        try {
            return "RANGE " + info.checkAndCreateRange(MetaUtils.buildIdToColumn(cols), desc, isTemp);
        } catch (DdlException e) {
            return "DDL " + e.getMessage();
        } catch (IllegalArgumentException | IllegalStateException e) {
            return e.getClass().getSimpleName();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Random generators. A "domain" maps an int in [0, 100) to partition values monotonically, so consecutive
    // boundaries always produce disjoint, ordered ranges regardless of column count / type.
    // ---------------------------------------------------------------------------------------------------------

    static final class Domain {
        final List<Column> cols;
        final IntFunction<List<PartitionValue>> values;
        final boolean hasShadow;

        Domain(List<Column> cols, IntFunction<List<PartitionValue>> values, boolean hasShadow) {
            this.cols = cols;
            this.values = values;
            this.hasShadow = hasShadow;
        }

        PartitionKey key(int idx) throws AnalysisException {
            return PartitionKey.createPartitionKey(values.apply(idx), cols);
        }

        @Override
        public String toString() {
            return cols.size() + "col shadow=" + hasShadow;
        }
    }

    static Domain intDomain() {
        return new Domain(intColumns(), v -> Lists.newArrayList(new PartitionValue(Integer.toString(v))), false);
    }

    static Domain dateDomainWithShadow() {
        List<Column> cols = Lists.newArrayList(new Column("dt", new ScalarType(PrimitiveType.DATE), true, null, "", ""));
        return new Domain(cols,
                v -> Lists.newArrayList(new PartitionValue(LocalDate.of(2024, 1, 1).plusDays(v).toString())), true);
    }

    static Domain twoIntColumnDomain() {
        List<Column> cols = Lists.newArrayList(
                new Column("a", new ScalarType(PrimitiveType.INT), true, null, "", ""),
                new Column("b", new ScalarType(PrimitiveType.INT), true, null, "", ""));
        // (v / 10, v % 10) is lexicographically monotone in v
        return new Domain(cols, v -> Lists.newArrayList(
                new PartitionValue(Integer.toString(Math.floorDiv(v, 10))),
                new PartitionValue(Integer.toString(Math.floorMod(v, 10)))), false);
    }

    static List<Domain> allDomains() {
        return Lists.newArrayList(intDomain(), dateDomainWithShadow(), twoIntColumnDomain());
    }

    /** Builds a partition info with random disjoint ranges over the domain; returns the boundaries used. */
    static Integer[] populateRandom(RangePartitionInfo info, Domain d, Random rnd, boolean isTemp)
            throws Exception {
        TreeSet<Integer> bounds = new TreeSet<>();
        int n = rnd.nextInt(8);
        while (bounds.size() < n) {
            bounds.add(rnd.nextInt(100));
        }
        Integer[] b = bounds.toArray(new Integer[0]);
        long id = 1;
        if (d.hasShadow && !isTemp) {
            PartitionKey shadow = PartitionKey.createShadowPartitionKey(d.cols);
            info.addPartition(id++, false, Range.closedOpen(shadow, shadow), null, (short) 1, null);
        }
        if (b.length > 0 && rnd.nextBoolean()) {
            info.addPartition(id++, isTemp,
                    Range.closedOpen(PartitionKey.createInfinityPartitionKey(d.cols, false), d.key(b[0])),
                    null, (short) 1, null);
        }
        for (int i = 0; i + 1 < b.length; i++) {
            if (rnd.nextInt(4) != 0) { // leave a gap 25% of the time
                info.addPartition(id++, isTemp, Range.closedOpen(d.key(b[i]), d.key(b[i + 1])), null, (short) 1, null);
            }
        }
        if (b.length > 0 && rnd.nextBoolean()) {
            info.addPartition(id++, isTemp,
                    Range.closedOpen(d.key(b[b.length - 1]), PartitionKey.createInfinityPartitionKey(d.cols, true)),
                    null, (short) 1, null);
        }
        return b;
    }

    static List<PartitionValue> randomValues(Domain d, Random rnd, Integer[] bounds) {
        int v;
        if (bounds.length > 0 && rnd.nextInt(3) != 0) {
            v = bounds[rnd.nextInt(bounds.length)] + rnd.nextInt(3) - 1; // on / just around a boundary
        } else {
            v = rnd.nextInt(112) - 6; // anywhere, including beyond both ends
        }
        List<PartitionValue> values = d.values.apply(v);
        if (values.size() > 1 && rnd.nextBoolean()) {
            return Lists.newArrayList(values.get(0)); // partial key: remaining columns are filled with MIN
        }
        return values;
    }

    /** Returns null when the analyzer rejects the desc (then there is nothing to compare). */
    static SingleRangePartitionDesc randomDesc(Domain d, Random rnd, Integer[] bounds) {
        PartitionKeyDesc keyDesc;
        switch (rnd.nextInt(4)) {
            case 0:
                keyDesc = new PartitionKeyDesc(randomValues(d, rnd, bounds));                       // LESS THAN v
                break;
            case 1:
                keyDesc = PartitionKeyDesc.createMaxKeyDesc();                                      // LESS THAN MAXVALUE
                break;
            case 2:
                keyDesc = new PartitionKeyDesc(randomValues(d, rnd, bounds), randomValues(d, rnd, bounds)); // FIXED
                break;
            default:
                keyDesc = new PartitionKeyDesc(randomValues(d, rnd, bounds),
                        Lists.newArrayList(PartitionValue.MAX_VALUE));                             // FIXED to MAXVALUE
                break;
        }
        SingleRangePartitionDesc desc = new SingleRangePartitionDesc(false, "p_new", keyDesc, null);
        try {
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(desc, d.cols.size(), null);
        } catch (AnalysisException e) {
            return null;
        }
        return desc;
    }

    @Test
    public void testNeighbourLocationMatchesLinearOracle() throws Exception {
        for (Domain d : allDomains()) {
            for (boolean isTemp : new boolean[] {false, true}) {
                Random rnd = new Random(20260920L + (isTemp ? 1 : 0));
                int compared = 0;
                for (int iter = 0; iter < 400; iter++) {
                    RangePartitionInfo info = new RangePartitionInfo(d.cols);
                    Integer[] bounds = populateRandom(info, d, rnd, isTemp);
                    assertBothIndexesMatch(info);
                    for (int k = 0; k < 3; k++) {
                        SingleRangePartitionDesc desc = randomDesc(d, rnd, bounds);
                        if (desc == null) {
                            continue;
                        }
                        String expected = oracleOutcome(info, d.cols, desc, isTemp);
                        String actual = newOutcome(info, d.cols, desc, isTemp);
                        Assertions.assertEquals(expected, actual, () -> "domain=" + d + " temp=" + isTemp
                                + " existing=" + info.getSortedRangeMap(isTemp) + " desc=" + desc.getPartitionKeyDesc());
                        compared++;
                    }
                }
                Assertions.assertTrue(compared > 500, "too few comparisons: " + compared);
            }
        }
    }

    @Test
    public void testBatchAddMatchesSequentialAdd() throws Exception {
        // checkNewRangePartitionDescs must see earlier descs of the same batch, exactly like adding one by one.
        Domain d = intDomain();
        Random rnd = new Random(7L);
        for (int iter = 0; iter < 200; iter++) {
            RangePartitionInfo base = new RangePartitionInfo(d.cols);
            Integer[] bounds = populateRandom(base, d, rnd, false);
            List<SingleRangePartitionDesc> batch = new ArrayList<>();
            int batchSize = 1 + rnd.nextInt(4);
            while (batch.size() < batchSize) {
                SingleRangePartitionDesc desc = randomDesc(d, rnd, bounds);
                if (desc != null) {
                    batch.add(desc);
                }
            }
            // sequential: apply through handleNewSinglePartitionDesc on a copy, record the outcome
            RangePartitionInfo sequential = new RangePartitionInfo(base);
            String expected;
            try {
                long id = 1000;
                StringBuilder sb = new StringBuilder();
                for (SingleRangePartitionDesc desc : batch) {
                    sb.append(sequential.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(d.cols), desc, id++,
                            false)).append(';');
                }
                expected = "OK " + sb;
            } catch (DdlException e) {
                expected = "DDL " + e.getMessage();
            }
            // batch: checkNewRangePartitionDescs on the untouched base
            List<Pair<Partition, PartitionDesc>> pairs = new ArrayList<>();
            long id = 1000;
            for (SingleRangePartitionDesc desc : batch) {
                pairs.add(Pair.create(new Partition(id++, desc.getPartitionName(), null), desc));
            }
            String actual;
            try {
                Map<Long, Range<PartitionKey>> ranges =
                        base.checkNewRangePartitionDescs(MetaUtils.buildIdToColumn(d.cols), pairs, false);
                StringBuilder sb = new StringBuilder();
                for (long pid = 1000; pid < 1000 + batch.size(); pid++) {
                    sb.append(ranges.get(pid)).append(';');
                }
                actual = "OK " + sb;
            } catch (DdlException e) {
                actual = "DDL " + e.getMessage();
            }
            Assertions.assertEquals(expected, actual, "batch=" + batch.stream()
                    .map(x -> x.getPartitionKeyDesc().toString()).toList());
            assertBothIndexesMatch(base); // checking must not mutate the base
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Deterministic boundary cases. The random differential test hits infinity keys only by chance; these pin the
    // interesting shapes down explicitly and still compare against the linear oracle.
    // ---------------------------------------------------------------------------------------------------------

    private static PartitionKey minKey(List<Column> cols) throws AnalysisException {
        return PartitionKey.createInfinityPartitionKey(cols, false);
    }

    private static PartitionKey maxKey(List<Column> cols) throws AnalysisException {
        return PartitionKey.createInfinityPartitionKey(cols, true);
    }

    @SafeVarargs
    private static RangePartitionInfo infoWith(List<Column> cols, boolean isTemp, Range<PartitionKey>... ranges) {
        RangePartitionInfo info = new RangePartitionInfo(cols);
        long id = 1;
        for (Range<PartitionKey> r : ranges) {
            info.addPartition(id++, isTemp, r, null, (short) 1, null);
        }
        return info;
    }

    private static SingleRangePartitionDesc lessThan(List<Column> cols, String v) throws AnalysisException {
        SingleRangePartitionDesc d = new SingleRangePartitionDesc(false, "p_new",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue(v))), null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(d, cols.size(), null);
        return d;
    }

    private static SingleRangePartitionDesc lessThanMax(List<Column> cols) throws AnalysisException {
        SingleRangePartitionDesc d = new SingleRangePartitionDesc(false, "p_new",
                PartitionKeyDesc.createMaxKeyDesc(), null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(d, cols.size(), null);
        return d;
    }

    private static SingleRangePartitionDesc fixed(List<Column> cols, String lo, String hi) throws AnalysisException {
        SingleRangePartitionDesc d = new SingleRangePartitionDesc(false, "p_new",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue(lo)),
                        Lists.newArrayList(new PartitionValue(hi))), null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(d, cols.size(), null);
        return d;
    }

    private static SingleRangePartitionDesc fixedToMax(List<Column> cols, String lo) throws AnalysisException {
        SingleRangePartitionDesc d = new SingleRangePartitionDesc(false, "p_new",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue(lo)),
                        Lists.newArrayList(PartitionValue.MAX_VALUE)), null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(d, cols.size(), null);
        return d;
    }

    private static void assertSameAsOracle(String label, RangePartitionInfo info, List<Column> cols,
                                           SingleRangePartitionDesc desc, boolean isTemp) {
        Assertions.assertEquals(oracleOutcome(info, cols, desc, isTemp), newOutcome(info, cols, desc, isTemp), label);
    }

    @Test
    public void testInfinityBoundaries() throws Exception {
        List<Column> c = intColumns();
        PartitionKey min = minKey(c);
        PartitionKey max = maxKey(c);

        for (boolean isTemp : new boolean[] {false, true}) {
            String t = " temp=" + isTemp;
            // empty index
            assertSameAsOracle("empty + LESS THAN 10" + t, infoWith(c, isTemp), c, lessThan(c, "10"), isTemp);
            assertSameAsOracle("empty + LESS THAN MAXVALUE" + t, infoWith(c, isTemp), c, lessThanMax(c), isTemp);
            assertSameAsOracle("empty + FIXED [10,20)" + t, infoWith(c, isTemp), c, fixed(c, "10", "20"), isTemp);
            assertSameAsOracle("empty + FIXED [10,MAXVALUE)" + t, infoWith(c, isTemp), c, fixedToMax(c, "10"), isTemp);

            // one partition spanning everything
            RangePartitionInfo whole = infoWith(c, isTemp, Range.closedOpen(min, max));
            assertSameAsOracle("[MIN,MAX) + LESS THAN 10" + t, whole, c, lessThan(c, "10"), isTemp);
            assertSameAsOracle("[MIN,MAX) + LESS THAN MAXVALUE" + t, whole, c, lessThanMax(c), isTemp);
            assertSameAsOracle("[MIN,MAX) + FIXED [10,20)" + t, whole, c, fixed(c, "10", "20"), isTemp);

            // lower bound is -inf
            RangePartitionInfo lowInf = infoWith(c, isTemp, Range.closedOpen(min, intKey(c, 10)));
            assertSameAsOracle("[MIN,10) + LESS THAN 20" + t, lowInf, c, lessThan(c, "20"), isTemp);
            assertSameAsOracle("[MIN,10) + LESS THAN 10" + t, lowInf, c, lessThan(c, "10"), isTemp);
            assertSameAsOracle("[MIN,10) + LESS THAN 5" + t, lowInf, c, lessThan(c, "5"), isTemp);
            assertSameAsOracle("[MIN,10) + FIXED [10,20)" + t, lowInf, c, fixed(c, "10", "20"), isTemp);
            assertSameAsOracle("[MIN,10) + FIXED [5,20)" + t, lowInf, c, fixed(c, "5", "20"), isTemp);

            // upper bound is +inf
            RangePartitionInfo highInf = infoWith(c, isTemp, Range.closedOpen(intKey(c, 10), max));
            assertSameAsOracle("[10,MAX) + LESS THAN 5" + t, highInf, c, lessThan(c, "5"), isTemp);
            assertSameAsOracle("[10,MAX) + LESS THAN 20" + t, highInf, c, lessThan(c, "20"), isTemp);
            assertSameAsOracle("[10,MAX) + LESS THAN MAXVALUE" + t, highInf, c, lessThanMax(c), isTemp);
            assertSameAsOracle("[10,MAX) + FIXED [0,10)" + t, highInf, c, fixed(c, "0", "10"), isTemp);
            assertSameAsOracle("[10,MAX) + FIXED [0,11)" + t, highInf, c, fixed(c, "0", "11"), isTemp);

            // both ends infinite, split in the middle
            RangePartitionInfo bothEnds = infoWith(c, isTemp,
                    Range.closedOpen(min, intKey(c, 10)), Range.closedOpen(intKey(c, 20), max));
            assertSameAsOracle("[MIN,10)[20,MAX) + FIXED [10,20)" + t, bothEnds, c, fixed(c, "10", "20"), isTemp);
            assertSameAsOracle("[MIN,10)[20,MAX) + LESS THAN 15" + t, bothEnds, c, lessThan(c, "15"), isTemp);
            assertSameAsOracle("[MIN,10)[20,MAX) + LESS THAN MAXVALUE" + t, bothEnds, c, lessThanMax(c), isTemp);
            assertSameAsOracle("[MIN,10)[20,MAX) + FIXED [10,MAXVALUE)" + t, bothEnds, c, fixedToMax(c, "10"), isTemp);

            // exact-boundary touches against a plain range
            RangePartitionInfo plain = infoWith(c, isTemp, Range.closedOpen(intKey(c, 10), intKey(c, 20)));
            assertSameAsOracle("[10,20) + LESS THAN 10" + t, plain, c, lessThan(c, "10"), isTemp);
            assertSameAsOracle("[10,20) + LESS THAN 20" + t, plain, c, lessThan(c, "20"), isTemp);
            assertSameAsOracle("[10,20) + LESS THAN 21" + t, plain, c, lessThan(c, "21"), isTemp);
            assertSameAsOracle("[10,20) + FIXED [20,MAXVALUE)" + t, plain, c, fixedToMax(c, "20"), isTemp);
            assertSameAsOracle("[10,20) + FIXED [5,10)" + t, plain, c, fixed(c, "5", "10"), isTemp);
            assertSameAsOracle("[10,20) + FIXED [5,15)" + t, plain, c, fixed(c, "5", "15"), isTemp);
            assertSameAsOracle("[10,20) + FIXED [15,25)" + t, plain, c, fixed(c, "15", "25"), isTemp);
        }
    }

    @Test
    public void testShadowPartitionBoundary() throws Exception {
        // Auto-partitioned tables carry an empty shadow range [0000-00-00, 0000-00-00). It sorts before every real
        // date and the LESS THAN lower bound is derived from it, so it must stay in the index.
        List<Column> c = Lists.newArrayList(new Column("dt", new ScalarType(PrimitiveType.DATE), true, null, "", ""));
        RangePartitionInfo info = new RangePartitionInfo(c);
        info.createAutomaticShadowPartition(c, 7L, "1");
        PartitionKey d1 = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("2024-01-01")), c);
        PartitionKey d2 = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("2024-02-01")), c);
        info.addPartition(1L, false, Range.closedOpen(d1, d2), null, (short) 1, null);
        assertBothIndexesMatch(info);
        Assertions.assertEquals(7L, info.getSortedRangeMap(false).get(0).getKey());
        Assertions.assertTrue(info.getSortedRangeMap(false).get(0).getValue().isEmpty());

        SingleRangePartitionDesc d = new SingleRangePartitionDesc(false, "p_new",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("2024-03-01"))), null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(d, 1, null);
        assertSameAsOracle("shadow + real partition, LESS THAN 2024-03-01", info, c, d, false);
    }

    @Test
    public void testUnboundedRangeBehaviourIsUnchanged() throws Exception {
        // Guava ranges without a lower endpoint cannot be indexed. Confirm this is NOT a regression: the pre-index
        // code hit the same IllegalStateException through RANGE_MAP_ENTRY_COMPARATOR.
        List<Column> c = intColumns();
        RangePartitionInfo info = new RangePartitionInfo(c);
        info.addPartition(1L, false, intRange(c, 100, 200), null, (short) 1, null);

        for (Range<PartitionKey> unbounded : Lists.newArrayList(
                Range.lessThan(intKey(c, 20)), Range.<PartitionKey>all())) {
            // old path: sorting two entries invokes the comparator, which calls lowerEndpoint()
            Map<Long, Range<PartitionKey>> raw = new LinkedHashMap<>();
            raw.put(1L, unbounded);
            raw.put(2L, intRange(c, 100, 200));
            List<Map.Entry<Long, Range<PartitionKey>>> list = Lists.newArrayList(raw.entrySet());
            Assertions.assertThrows(IllegalStateException.class,
                    () -> list.sort(RangeUtils.RANGE_MAP_ENTRY_COMPARATOR), "old sort should already throw");
            // new path: the index put calls lowerEndpoint() too
            Assertions.assertThrows(IllegalStateException.class,
                    () -> info.addPartition(2L, false, unbounded, null, (short) 1, null), "index put should throw");
        }

        // A range with a lower endpoint but no upper endpoint indexes fine; only reading its upper endpoint fails,
        // which the pre-index scan did as well.
        RangePartitionInfo atLeastInfo = new RangePartitionInfo(c);
        atLeastInfo.addPartition(1L, false, Range.atLeast(intKey(c, 10)), null, (short) 1, null);
        assertIndexMatches(atLeastInfo, false);
        SingleRangePartitionDesc d = lessThan(c, "500");
        Assertions.assertEquals(oracleOutcome(atLeastInfo, c, d, false), newOutcome(atLeastInfo, c, d, false));
    }

    /** HEAD's getSortedPartitions, verbatim, as the oracle. */
    static List<Long> oracleSortedPartitions(RangePartitionInfo info, boolean asc) {
        List<Map.Entry<Long, Range<PartitionKey>>> sortedList =
                Lists.newArrayList(info.getIdToRange(false).entrySet());
        sortedList.sort(asc ? RangeUtils.RANGE_MAP_ENTRY_COMPARATOR : RangeUtils.RANGE_MAP_ENTRY_COMPARATOR.reversed());
        if (sortedList.isEmpty()) {
            return Lists.newArrayList();
        }
        return sortedList.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    @Test
    public void testGetSortedPartitionsMatchesOracle() throws Exception {
        List<Column> c = intColumns();
        RangePartitionInfo empty = new RangePartitionInfo(c);
        Assertions.assertEquals(oracleSortedPartitions(empty, true), empty.getSortedPartitions(true));
        Assertions.assertEquals(oracleSortedPartitions(empty, false), empty.getSortedPartitions(false));

        // many, inserted out of order, with both infinity ends
        RangePartitionInfo many = new RangePartitionInfo(c);
        many.addPartition(5L, false, Range.closedOpen(intKey(c, 40), maxKey(c)), null, (short) 1, null);
        many.addPartition(2L, false, intRange(c, 20, 30), null, (short) 1, null);
        many.addPartition(9L, false, Range.closedOpen(minKey(c), intKey(c, 10)), null, (short) 1, null);
        many.addPartition(7L, false, intRange(c, 30, 40), null, (short) 1, null);
        many.addPartition(1L, false, intRange(c, 10, 20), null, (short) 1, null);
        Assertions.assertEquals(Lists.newArrayList(9L, 1L, 2L, 7L, 5L), many.getSortedPartitions(true));
        Assertions.assertEquals(oracleSortedPartitions(many, false), many.getSortedPartitions(false));

        // temp partitions must not leak into the formal result
        many.addPartition(100L, true, intRange(c, 10, 20), null, (short) 1, null);
        Assertions.assertEquals(oracleSortedPartitions(many, true), many.getSortedPartitions(true));

        // callers mutate the returned list (PartitionColumnMinMaxRewriteRule does retainAll)
        List<Long> mutable = many.getSortedPartitions(false);
        mutable.retainAll(Lists.newArrayList(1L, 5L));
        Assertions.assertEquals(Lists.newArrayList(5L, 1L), mutable);

        Random rnd = new Random(4242L);
        for (Domain d : allDomains()) {
            for (int iter = 0; iter < 200; iter++) {
                RangePartitionInfo info = new RangePartitionInfo(d.cols);
                populateRandom(info, d, rnd, false);
                Assertions.assertEquals(oracleSortedPartitions(info, true), info.getSortedPartitions(true),
                        "asc, domain=" + d);
                Assertions.assertEquals(oracleSortedPartitions(info, false), info.getSortedPartitions(false),
                        "desc, domain=" + d);
            }
        }
    }
}
