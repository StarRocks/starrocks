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

package com.starrocks.common.util;

/**
 * Display-only snapshot of a single {@link Counter} entry within a serialised
 * profile tree.
 *
 * <p>Both profile formatters ({@code DefaultProfileFormatter} and
 * {@code JsonProfileFormatter}) consult exactly two Counter fields:
 * {@link Counter#getValue()} and {@link Counter#getType()}.  Merge-time fields
 * (strategy, min/max, display_threshold) are never read during formatting and
 * are therefore intentionally absent from this record.</p>
 *
 * <p>{@code nameId} and {@code parentId} are local dictionary indices assigned
 * by {@link ProfileSerializer} — they map to counter name strings via the
 * per-tree dictionary stored in the binary header.</p>
 *
 * @param nameId   local dictionary index of this counter's name
 * @param parentId local dictionary index of the parent counter's name
 * @param value    counter value ({@link Counter#getValue()})
 * @param typeVal  TUnit ordinal ({@link com.starrocks.thrift.TUnit#getValue()})
 */
record ProfileRawCounter(int nameId, int parentId, long value, int typeVal) {
}
