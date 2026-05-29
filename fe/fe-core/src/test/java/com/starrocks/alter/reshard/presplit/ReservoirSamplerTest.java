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
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.DUMMY_CONTEXT;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintRow;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.compositeRow;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.varcharColumn;

public class ReservoirSamplerTest {

    private static SampleRequest requestWithByteLimit(long byteLimit, List<Column> sortKey) {
        return new SampleRequest(DUMMY_CONTEXT, sortKey, byteLimit, 0L);
    }

    private static final class FakeExecutor implements SampleSubqueryExecutor {
        private final List<List<Variant>> rows;
        private final Estimates estimates;

        FakeExecutor(List<List<Variant>> rows, Estimates estimates) {
            this.rows = rows;
            this.estimates = estimates;
        }

        @Override
        public SampleExecution execute(SampleRequest request) {
            // The fake executor models the pre-partition-source row shape — each canned row
            // is sort-key only. SampleRow.ofSortKey wraps it with an empty partition-source
            // tuple so the executor contract matches the production type.
            Iterator<List<Variant>> sourceIterator = rows.iterator();
            Iterator<SampleRow> rowIterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return sourceIterator.hasNext();
                }

                @Override
                public SampleRow next() {
                    List<Variant> next = sourceIterator.next();
                    return next == null ? null : SampleRow.ofSortKey(next);
                }
            };
            return new SampleExecution(rowIterator, estimates);
        }
    }

    @Test
    public void testCollectsAllRowsWhenUnderByteLimit() throws Exception {
        List<List<Variant>> input = new ArrayList<>();
        for (long value = 0; value < 10; value++) {
            input.add(bigintRow(value));
        }
        FakeExecutor executor = new FakeExecutor(input, new Estimates(1024L, 1000L));
        Sampler sampler = new ReservoirSampler(executor);

        SampleSet result = sampler.sample(requestWithByteLimit(64 * 1024L, List.of(bigintColumn("k"))));

        Assertions.assertEquals(10, result.getTuples().size());
        Assertions.assertEquals(new Estimates(1024L, 1000L), result.getEstimates());
        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals(1, result.getTuples().get(i).getValues().size());
            Assertions.assertEquals(Long.toString(i),
                    result.getTuples().get(i).getValues().get(0).getStringValue());
        }
    }

    @Test
    public void testEmptyExecutorReturnsSampleSetEmpty() throws Exception {
        Sampler sampler = new ReservoirSampler(new FakeExecutor(Collections.emptyList(), Estimates.ZERO));

        SampleSet result = sampler.sample(requestWithByteLimit(1024L, List.of(bigintColumn("k"))));

        Assertions.assertSame(SampleSet.EMPTY, result);
    }

    @Test
    public void testHonorsByteLimitStopsConsumingMidStream() throws Exception {
        // Each row's string value is 1 char ("0", "1", ...). At byteLimit=5, the
        // sampler accumulates 5 rows (5 bytes total) and stops before the 6th.
        List<List<Variant>> input = new ArrayList<>();
        for (long value = 0; value < 10; value++) {
            input.add(bigintRow(value));
        }
        Sampler sampler = new ReservoirSampler(new FakeExecutor(input, new Estimates(1024L, 1000L)));

        SampleSet result = sampler.sample(requestWithByteLimit(5L, List.of(bigintColumn("k"))));

        Assertions.assertEquals(5, result.getTuples().size());
    }

    @Test
    public void testAlwaysAdmitsFirstRowEvenIfBelowByteLimit() throws Exception {
        // Single oversized row; byteLimit is 1 but row is 10 bytes wide.
        // The first row is always admitted so SampleSet is never empty when input is non-empty.
        List<List<Variant>> input = List.of(bigintRow(1234567890L));
        Sampler sampler = new ReservoirSampler(new FakeExecutor(input, new Estimates(100L, 1L)));

        SampleSet result = sampler.sample(requestWithByteLimit(1L, List.of(bigintColumn("k"))));

        Assertions.assertEquals(1, result.getTuples().size());
        Assertions.assertEquals("1234567890",
                result.getTuples().get(0).getValues().get(0).getStringValue());
    }

    @Test
    public void testCompositeKeyTupleAssembly() throws Exception {
        List<List<Variant>> input = new ArrayList<>();
        input.add(compositeRow("tenant_000000", 42L));
        input.add(compositeRow("tenant_000001", 17L));
        Sampler sampler = new ReservoirSampler(new FakeExecutor(input, new Estimates(200L, 100L)));

        SampleSet result = sampler.sample(
                requestWithByteLimit(64 * 1024L, List.of(varcharColumn("g"), bigintColumn("p"))));

        Assertions.assertEquals(2, result.getTuples().size());
        Assertions.assertEquals(2, result.getTuples().get(0).getValues().size());
        Assertions.assertEquals("tenant_000000",
                result.getTuples().get(0).getValues().get(0).getStringValue());
        Assertions.assertEquals("42",
                result.getTuples().get(0).getValues().get(1).getStringValue());
        Assertions.assertEquals("tenant_000001",
                result.getTuples().get(1).getValues().get(0).getStringValue());
    }

    @Test
    public void testNullRowFromExecutorThrows() {
        List<List<Variant>> input = new ArrayList<>();
        input.add(null);
        Sampler sampler = new ReservoirSampler(new FakeExecutor(input, Estimates.ZERO));

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> sampler.sample(requestWithByteLimit(1024L, List.of(bigintColumn("k")))));
        Assertions.assertTrue(exception.getMessage().contains("null row"),
                "expected 'null row' in message, got: " + exception.getMessage());
    }

    @Test
    public void testExecutorExceptionPropagated() {
        SampleSubqueryExecutor failing = request -> {
            throw new StarRocksException("BE rpc unavailable");
        };
        Sampler sampler = new ReservoirSampler(failing);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> sampler.sample(requestWithByteLimit(1024L, List.of(bigintColumn("k")))));
        Assertions.assertEquals("BE rpc unavailable", exception.getMessage());
    }

    @Test
    public void testRejectsNullExecutorAtConstruction() {
        Assertions.assertThrows(NullPointerException.class, () -> new ReservoirSampler(null));
    }

    @Test
    public void testNullVariantInsideRowThrows() {
        // A row containing a null variant is a producer contract violation; the
        // sampler surfaces a structured error rather than NPE'ing later.
        List<Variant> rowWithNullValue = new ArrayList<>();
        rowWithNullValue.add(null);
        List<List<Variant>> input = new ArrayList<>();
        input.add(rowWithNullValue);
        Sampler sampler = new ReservoirSampler(new FakeExecutor(input, Estimates.ZERO));

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> sampler.sample(requestWithByteLimit(1024L, List.of(bigintColumn("k")))));
        Assertions.assertTrue(exception.getMessage().contains("null value"),
                "expected 'null value' in message, got: " + exception.getMessage());
    }

    @Test
    public void testExecutorReusingRowListDoesNotCorruptSamples() throws Exception {
        // Production-side executors may stream rows by mutating one reusable buffer
        // across .next() calls. The sampler must defensively copy values into the Tuple.
        List<Variant> reusableBuffer = new ArrayList<>();
        reusableBuffer.add(Variant.of(IntegerType.BIGINT, "1"));
        SampleSubqueryExecutor mutatingExecutor = request -> {
            Iterator<SampleRow> iter = new Iterator<>() {
                int remaining = 2;

                @Override
                public boolean hasNext() {
                    return remaining > 0;
                }

                @Override
                public SampleRow next() {
                    reusableBuffer.set(0, Variant.of(IntegerType.BIGINT, Integer.toString(3 - remaining)));
                    remaining--;
                    return SampleRow.ofSortKey(reusableBuffer);
                }
            };
            return new SampleSubqueryExecutor.SampleExecution(iter, new Estimates(100L, 2L));
        };
        Sampler sampler = new ReservoirSampler(mutatingExecutor);

        SampleSet result = sampler.sample(requestWithByteLimit(64L, List.of(bigintColumn("k"))));

        Assertions.assertEquals(2, result.getTuples().size());
        Assertions.assertEquals("1", result.getTuples().get(0).getValues().get(0).getStringValue(),
                "first stored tuple should retain value at its capture time");
        Assertions.assertEquals("2", result.getTuples().get(1).getValues().get(0).getStringValue());
    }

    @Test
    public void testPassesEstimatesThroughFromExecutor() throws Exception {
        // Tablet-count selection downstream uses these estimates, not the in-sample size — so they must pass through.
        List<List<Variant>> input = List.of(bigintRow(0));
        Estimates fullInput = new Estimates(32L * 1024L * 1024L * 1024L, 1_000_000L);
        Sampler sampler = new ReservoirSampler(new FakeExecutor(input, fullInput));

        SampleSet result = sampler.sample(requestWithByteLimit(1024L, List.of(bigintColumn("k"))));

        Assertions.assertEquals(fullInput, result.getEstimates());
    }
}
