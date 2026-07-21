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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SecondaryIndexSpecTest {

    @Test
    void forVisibleRollupsIncludesEveryRollupExceptBase() {
        // A target with a base index (id 1) and one rollup (id 2) must produce exactly
        // one SecondaryIndexSpec for the rollup, carrying its own (divergent) sort key.
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(1L);
        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getIndexMetaId()).thenReturn(1L);
        MaterializedIndexMeta rollupMeta = mock(MaterializedIndexMeta.class);
        when(rollupMeta.getIndexMetaId()).thenReturn(2L);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(baseMeta, rollupMeta));

        List<Column> rollupSortKey = List.of(bigintColumn("r"));
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, 2L)).thenReturn(rollupSortKey);

            List<SecondaryIndexSpec> specs = SecondaryIndexSpec.forVisibleRollups(table);

            Assertions.assertEquals(1, specs.size(), "exactly one rollup spec expected");
            Assertions.assertEquals(2L, specs.get(0).indexMetaId());
            Assertions.assertEquals(rollupSortKey, specs.get(0).sortKey());
        }
    }

    @Test
    void forVisibleRollupsEmptyForSingleIndexTarget() {
        // A single-index (base only) target must produce no secondary specs.
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(1L);
        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getIndexMetaId()).thenReturn(1L);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(baseMeta));

        Assertions.assertTrue(SecondaryIndexSpec.forVisibleRollups(table).isEmpty(),
                "a single-index target must produce no secondary index specs");
    }
}
