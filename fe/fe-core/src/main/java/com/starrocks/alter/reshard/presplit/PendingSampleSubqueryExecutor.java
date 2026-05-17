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

import java.util.Objects;

/**
 * Placeholder Tier 2 sub-query executor: throws so the coordinator returns
 * {@link SkipReason#SAMPLE_FAILED} and the load proceeds without pre-split.
 * The production implementation builds a sampling sub-query on top of a
 * {@code FileScanNode} (configured per the load kind).
 */
final class PendingSampleSubqueryExecutor implements SampleSubqueryExecutor {

    private final LoadKind loadKind;

    PendingSampleSubqueryExecutor(LoadKind loadKind) {
        this.loadKind = Objects.requireNonNull(loadKind, "loadKind");
    }

    @Override
    public SampleExecution execute(SampleRequest request) throws StarRocksException {
        throw new StarRocksException(
                "sample sub-query executor not yet wired for " + loadKind.displayName());
    }
}
