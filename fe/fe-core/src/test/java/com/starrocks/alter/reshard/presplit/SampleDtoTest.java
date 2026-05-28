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
}
