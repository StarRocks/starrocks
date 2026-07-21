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

import java.util.List;

/**
 * Resolved single-tablet pre-split target for one visible index (base or
 * rollup) of a range-distribution table's unique physical partition.
 *
 * @param indexMetaId stable meta id of the index, matches the key in
 *                     {@code OlapTable.indexMetaIdToMeta} across reshard
 *                     (unlike the physical index id, which changes).
 * @param oldTabletId the index's single base-index-tablet-per-partition
 *                     tablet id before the split.
 * @param sortKey     the index's own sort-key columns, resolved via
 *                     {@link com.starrocks.sql.common.MetaUtils#getRangeDistributionColumns(
 *                     com.starrocks.catalog.OlapTable, long)}.
 */
record IndexPreSplitTarget(long indexMetaId, long oldTabletId, List<Column> sortKey) {
}
