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

import java.util.List;
import java.util.Objects;

/**
 * Placeholder meta-tier statistics provider: reports meta tier unavailable so the
 * pipeline falls through to data tier. The production implementation reads
 * Parquet/ORC footers (via the connector for INSERT-from-FILES, via the
 * broker for Broker Load).
 */
final class PendingRowGroupStatisticsProvider implements RowGroupStatisticsProvider {

    private final LoadKind loadKind;

    PendingRowGroupStatisticsProvider(LoadKind loadKind) {
        this.loadKind = Objects.requireNonNull(loadKind, "loadKind");
    }

    @Override
    public List<RowGroupStatistics> fetch(SampleRequest request) throws MetaTierUnavailableException {
        throw new MetaTierUnavailableException(
                "row-group statistics provider not yet wired for " + loadKind.displayName());
    }
}
