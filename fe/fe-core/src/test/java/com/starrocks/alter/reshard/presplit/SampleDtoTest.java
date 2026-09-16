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
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.DUMMY_CONTEXT;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintTuple;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.varcharColumn;

public class SampleDtoTest {

    @Test
    public void testSampleRequestConstructorDefensiveCopiesSortKey() {
        Column column = varcharColumn("k");
        List<Column> mutableSortKey = new java.util.ArrayList<>(List.of(column));
        SampleRequest request = new SampleRequest(DUMMY_CONTEXT, mutableSortKey, 1024L, 42L);

        // Mutating the caller's list must not affect the request.
        mutableSortKey.add(varcharColumn("extra"));
        Assertions.assertEquals(1, request.getSortKey().size());
        Assertions.assertSame(column, request.getSortKey().get(0));
        Assertions.assertEquals(1024L, request.getSampleByteLimit());
        Assertions.assertEquals(42L, request.getSeed());
        Assertions.assertSame(DUMMY_CONTEXT, request.getScanContext());
    }

    @Test
    public void testSampleRequestRejectsNullScanContext() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new SampleRequest(null, List.of(varcharColumn("k")), 1024L, 0L));
    }

    @Test
    public void testSampleRequestRejectsEmptySortKey() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SampleRequest(DUMMY_CONTEXT, Collections.emptyList(), 1024L, 0L));
    }

    @Test
    public void testSampleRequestRejectsNonPositiveByteLimit() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SampleRequest(DUMMY_CONTEXT, List.of(varcharColumn("k")), 0L, 0L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SampleRequest(DUMMY_CONTEXT, List.of(varcharColumn("k")), -1L, 0L));
    }

    @Test
    public void testSampleSetConstructorDefensiveCopiesTuples() {
        List<Tuple> mutableTuples = new java.util.ArrayList<>(List.of(bigintTuple(1L)));
        SampleSet sampleSet = new SampleSet(mutableTuples, new Estimates(100L, 10L));

        mutableTuples.add(bigintTuple(2L));
        Assertions.assertEquals(1, sampleSet.getTuples().size());
        Assertions.assertEquals(new Estimates(100L, 10L), sampleSet.getEstimates());
        Assertions.assertFalse(sampleSet.isEmpty());
    }

    @Test
    public void testEstimatesRejectsNegatives() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Estimates(-1L, 0L));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Estimates(0L, -1L));
    }

    @Test
    public void testSampleSetEmptyConstant() {
        Assertions.assertTrue(SampleSet.EMPTY.isEmpty());
        Assertions.assertEquals(Estimates.ZERO, SampleSet.EMPTY.getEstimates());
        Assertions.assertTrue(SampleSet.EMPTY.getTuples().isEmpty());
    }

    @Test
    public void testSamplerInterfaceUsableAsLambda() throws Exception {
        Sampler stubSampler = request -> new SampleSet(
                List.of(bigintTuple(request.getSeed())),
                new Estimates(request.getSampleByteLimit(), 1L));
        SampleRequest request = new SampleRequest(DUMMY_CONTEXT, List.of(varcharColumn("k")), 4096L, 7L);
        SampleSet sampleSet = stubSampler.sample(request);

        Assertions.assertEquals(1, sampleSet.getTuples().size());
        Assertions.assertEquals(new Estimates(4096L, 1L), sampleSet.getEstimates());
    }

    @Test
    public void testSampleRequestDefaultConstructorsHaveEmptySecondaryIndexSortKeys() {
        SampleRequest fourArg = new SampleRequest(DUMMY_CONTEXT, List.of(varcharColumn("k")), 1024L, 0L);
        SampleRequest fiveArg = new SampleRequest(
                DUMMY_CONTEXT, List.of(varcharColumn("k")), List.of(), 1024L, 0L);

        Assertions.assertTrue(fourArg.getSecondaryIndexSortKeys().isEmpty());
        Assertions.assertTrue(fiveArg.getSecondaryIndexSortKeys().isEmpty());
    }

    @Test
    public void testSampleRequestWithQueryTimeoutSecondsPreservesSecondaryIndexSortKeys() {
        SecondaryIndexSpec secondarySpec = new SecondaryIndexSpec(7L, List.of(varcharColumn("r")));
        SampleRequest request = new SampleRequest(
                DUMMY_CONTEXT, List.of(varcharColumn("k")), List.of(secondarySpec), List.of(), 1024L, 0L);

        SampleRequest withTimeout = request.withQueryTimeoutSeconds(30);

        Assertions.assertEquals(List.of(secondarySpec), withTimeout.getSecondaryIndexSortKeys());
        Assertions.assertEquals(30, withTimeout.getQueryTimeoutSeconds());
    }

    @Test
    public void testSampleRowOfSortKeyHasEmptySecondaryTuples() {
        SampleRow row = SampleRow.ofSortKey(List.of(Variant.of(IntegerType.BIGINT, "1")));

        Assertions.assertNotNull(row.secondaryIndexTuples());
        Assertions.assertTrue(row.secondaryIndexTuples().isEmpty());
        Assertions.assertTrue(row.partitionSourceTuple().isEmpty());
    }

    @Test
    public void testSampleRowTwoArgConstructorDefaultsEmptySecondaryTuples() {
        List<Variant> sortKeyTuple = List.of(Variant.of(IntegerType.BIGINT, "1"));
        List<Variant> partitionSourceTuple = List.of(Variant.of(IntegerType.BIGINT, "2"));

        SampleRow row = new SampleRow(sortKeyTuple, partitionSourceTuple);

        Assertions.assertNotNull(row.secondaryIndexTuples());
        Assertions.assertTrue(row.secondaryIndexTuples().isEmpty());
        Assertions.assertEquals(sortKeyTuple, row.sortKeyTuple());
        Assertions.assertEquals(partitionSourceTuple, row.partitionSourceTuple());
    }

    @Test
    public void testSampleSetBaseOnlyGetSecondaryIndexTuplesAndMetaIdsEmpty() {
        SampleSet twoArg = new SampleSet(List.of(bigintTuple(1L)), new Estimates(100L, 1L));
        SampleSet threeArg = new SampleSet(List.of(bigintTuple(1L)), List.of(), new Estimates(100L, 1L));

        Assertions.assertTrue(twoArg.getSecondaryIndexTuples().isEmpty());
        Assertions.assertTrue(twoArg.getSecondaryIndexMetaIds().isEmpty());
        Assertions.assertTrue(threeArg.getSecondaryIndexTuples().isEmpty());
        Assertions.assertTrue(threeArg.getSecondaryIndexMetaIds().isEmpty());
    }

    @Test
    public void testSampleSetWithSecondariesMetaIdsMatchSpecOrder() {
        IndexTuple indexTupleA = new IndexTuple(5L, List.of(Variant.of(IntegerType.BIGINT, "10")));
        IndexTuple indexTupleB = new IndexTuple(9L, List.of(Variant.of(IntegerType.BIGINT, "20")));
        SampleSet sampleSet = new SampleSet(
                List.of(bigintTuple(1L)), List.of(),
                List.of(5L, 9L), List.of(List.of(indexTupleA, indexTupleB)),
                new Estimates(100L, 1L));

        Assertions.assertEquals(List.of(5L, 9L), sampleSet.getSecondaryIndexMetaIds());
        Assertions.assertEquals(1, sampleSet.getSecondaryIndexTuples().size());
        Assertions.assertEquals(List.of(indexTupleA, indexTupleB), sampleSet.getSecondaryIndexTuples().get(0));
    }

    @Test
    public void testSampleSetWithSecondariesMetaIdsPresentEvenForZeroRowSample() {
        SampleSet sampleSet = new SampleSet(List.of(), List.of(), List.of(5L), List.of(), Estimates.ZERO);

        Assertions.assertTrue(sampleSet.isEmpty());
        Assertions.assertEquals(List.of(5L), sampleSet.getSecondaryIndexMetaIds());
        Assertions.assertTrue(sampleSet.getSecondaryIndexTuples().isEmpty());
    }

    @Test
    public void testSampleSetMalformedSecondaryRowRejected() {
        IndexTuple idFive = new IndexTuple(5L, List.of(Variant.of(IntegerType.BIGINT, "10")));

        // Row is missing id 9 entirely (authoritative ids are {5, 9}).
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SampleSet(
                List.of(bigintTuple(1L)), List.of(),
                List.of(5L, 9L), List.of(List.of(idFive)),
                new Estimates(100L, 1L)));

        // Row carries an id (42) outside the authoritative set ({5}).
        IndexTuple unknownId = new IndexTuple(42L, List.of(Variant.of(IntegerType.BIGINT, "10")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SampleSet(
                List.of(bigintTuple(1L)), List.of(),
                List.of(5L), List.of(List.of(unknownId)),
                new Estimates(100L, 1L)));

        // Row carries id 5 twice (duplicate within one row).
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SampleSet(
                List.of(bigintTuple(1L)), List.of(),
                List.of(5L), List.of(List.of(idFive, idFive)),
                new Estimates(100L, 1L)));

        // Outer secondaryIndexTuples list is shorter than tuples (a missing row).
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SampleSet(
                List.of(bigintTuple(1L), bigintTuple(2L)), List.of(),
                List.of(5L), List.of(List.of(idFive)),
                new Estimates(100L, 1L)));
    }
}
