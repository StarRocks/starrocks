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

import com.starrocks.catalog.Variant;

import java.util.List;

/**
 * One secondary index's sampled sort-key tuple, tagged with the owning
 * index's meta id. A {@link SampleRow} carries zero or more of these -- one
 * per {@link SecondaryIndexSpec} in the originating {@link SampleRequest} --
 * so a downstream multi-index grouper can derive per-index split boundaries
 * from the same sampled row set instead of re-sampling per index.
 */
public record IndexTuple(long indexMetaId, List<Variant> values) {
}
