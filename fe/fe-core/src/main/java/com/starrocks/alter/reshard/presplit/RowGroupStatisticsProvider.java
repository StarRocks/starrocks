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

import com.starrocks.common.StarRocksException;

import java.util.List;

/**
 * Pluggable backend for {@link ParquetMetadataSampler}. Reads per-row-group
 * statistics from a {@link SampleRequest}'s scan context.
 *
 * <p>Production implementation enumerates Parquet (or ORC) source files via
 * the same connector helper used by the {@code FILES(...)} schema-inference
 * path, then pulls per-row-group column statistics from each file footer.
 * Test implementations return canned {@link RowGroupStatistics} lists.
 *
 * <p>The provider is responsible for projecting only the sort-key column
 * statistics — the {@link RowGroupStatistics#getMinTuple} / {@code maxTuple}
 * carry tuples of exactly the sort-key columns in
 * {@link SampleRequest#getSortKey} order.
 */
public interface RowGroupStatisticsProvider {
    List<RowGroupStatistics> fetch(SampleRequest request) throws StarRocksException;
}
