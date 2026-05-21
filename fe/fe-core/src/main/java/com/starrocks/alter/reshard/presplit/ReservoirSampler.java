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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Data-tier {@link Sampler}. Accumulates rows from a
 * {@link SampleSubqueryExecutor} until the soft byte limit is hit. Unbiased
 * sampling is the executor's job — this class only handles FE-side
 * accumulation. The first row is always admitted so an unusually wide first
 * row still yields a non-empty {@link SampleSet}.
 *
 * <p>The byte limit is enforced against an approximate width: the sum of each
 * value's {@code Variant.getStringValue().length()} in UTF-16 code units. For
 * ASCII this matches the on-wire UTF-8 byte count; for multi-byte characters
 * (CJK, emoji) the real UTF-8 byte count can be 2-3x higher. The limit is a
 * soft FE-memory guard, not a precise byte counter.
 */
public final class ReservoirSampler implements Sampler {

    private final SampleSubqueryExecutor executor;

    public ReservoirSampler(SampleSubqueryExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public SampleSet sample(SampleRequest request) throws StarRocksException {
        Objects.requireNonNull(request, "request");

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);
        long byteLimit = request.getSampleByteLimit();
        Iterator<List<Variant>> rowIterator = execution.rows();

        List<Tuple> tuples = new ArrayList<>();
        long accumulatedBytes = 0L;

        while (rowIterator.hasNext()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new StarRocksException("Sampling interrupted");
            }
            List<Variant> values = rowIterator.next();
            if (values == null) {
                throw new StarRocksException("Sampling executor returned a null row");
            }
            long rowBytes = estimateRowBytes(values);
            // Always admit the first row so a single oversize row doesn't return an empty
            // SampleSet; for subsequent rows enforce the soft limit.
            if (!tuples.isEmpty() && accumulatedBytes + rowBytes > byteLimit) {
                break;
            }
            // Defensive copy: production executors may reuse the row's value list across
            // iterator calls. Wrap into an immutable copy so the stored Tuple is stable.
            tuples.add(new Tuple(ImmutableList.copyOf(values)));
            accumulatedBytes += rowBytes;
        }

        return tuples.isEmpty() ? SampleSet.EMPTY : new SampleSet(tuples, execution.estimates());
    }

    /**
     * Sums each value's {@code String.length()} (UTF-16 code units) and also
     * rejects null entries — folded into one pass to keep per-row work tight.
     */
    private static long estimateRowBytes(List<Variant> values) throws StarRocksException {
        long total = 0L;
        for (Variant value : values) {
            if (value == null) {
                throw new StarRocksException("Sampling executor returned a row with a null value");
            }
            total += value.getStringValue().length();
        }
        return total;
    }
}
