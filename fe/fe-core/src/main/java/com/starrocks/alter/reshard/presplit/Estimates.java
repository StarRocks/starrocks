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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Variant;

import java.util.List;

/**
 * Estimates for the FULL input the sampler is summarizing — not the size of
 * the produced sample. Used by the downstream coordinator's tablet-count
 * selection.
 *
 * <p>{@code partitionSourceBytes} is the input's exact byte count per partition-source tuple when
 * the sampler knows it without reading the data, and empty otherwise.
 */
public record Estimates(long totalBytes, long totalRows, List<PartitionSourceBytes> partitionSourceBytes) {

    public static final Estimates ZERO = new Estimates(0L, 0L);

    /**
     * Bytes of the input whose partition-source tuple is {@code values}. Only a data-tier sample that
     * scanned a subset of its files, and read its partition source from the file path, carries these:
     * every file's partition is then known from its path, whereas the sample's share of rows per
     * partition is skewed by the per-partition file quotas.
     */
    public record PartitionSourceBytes(List<Variant> values, long bytes) {
        public PartitionSourceBytes {
            values = List.copyOf(values);
            Preconditions.checkArgument(bytes >= 0, "bytes must be non-negative, was %s", bytes);
        }
    }

    public Estimates {
        Preconditions.checkArgument(totalBytes >= 0, "totalBytes must be non-negative, was %s", totalBytes);
        Preconditions.checkArgument(totalRows >= 0, "totalRows must be non-negative, was %s", totalRows);
        partitionSourceBytes = List.copyOf(partitionSourceBytes);
    }

    public Estimates(long totalBytes, long totalRows) {
        this(totalBytes, totalRows, List.of());
    }
}
