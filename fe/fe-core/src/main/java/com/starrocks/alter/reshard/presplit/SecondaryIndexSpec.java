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
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * One visible secondary index (rollup) whose sort key the multi-partition
 * data-tier sampler should project and sample alongside the base sort key.
 * Carried on {@link SampleRequest#getSecondaryIndexSortKeys()}; the
 * {@code indexMetaId} tags the resulting {@link IndexTuple} so it can be
 * routed back to the correct index downstream.
 */
public record SecondaryIndexSpec(long indexMetaId, List<Column> sortKey) {

    /**
     * Builds one spec per visible rollup index of {@code target} (every visible index except the
     * base), each carrying its own sort key. Shared by the FILES / Broker / INSERT-from-table
     * pre-split sources so the "every visible index except base" enumeration lives in one place.
     */
    static List<SecondaryIndexSpec> forVisibleRollups(OlapTable target) {
        List<SecondaryIndexSpec> specs = new ArrayList<>();
        for (MaterializedIndexMeta meta : target.getVisibleIndexMetas()) {
            if (meta.getIndexMetaId() == target.getBaseIndexMetaId()) {
                continue;
            }
            specs.add(new SecondaryIndexSpec(meta.getIndexMetaId(),
                    MetaUtils.getRangeDistributionColumns(target, meta.getIndexMetaId())));
        }
        return specs;
    }
}
