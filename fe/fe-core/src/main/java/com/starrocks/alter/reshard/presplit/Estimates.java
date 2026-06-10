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

/**
 * Estimates for the FULL input the sampler is summarizing — not the size of
 * the produced sample. Used by the downstream coordinator's tablet-count
 * selection.
 */
public record Estimates(long totalBytes, long totalRows) {

    public static final Estimates ZERO = new Estimates(0L, 0L);

    public Estimates {
        Preconditions.checkArgument(totalBytes >= 0, "totalBytes must be non-negative, was %s", totalBytes);
        Preconditions.checkArgument(totalRows >= 0, "totalRows must be non-negative, was %s", totalRows);
    }
}
