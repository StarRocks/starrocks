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

package com.starrocks.connector.partitiontraits;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ADBCPartitionTraitsTest {
    private ConnectorPartitionTraits createTraits() {
        ADBCTable table = new ADBCTable(1, "tbl0", List.of(), "db", "adbc0", Map.of());
        return ConnectorPartitionTraits.buildWithCache(null, null, table);
    }

    @Test
    public void testSyntheticPartitionHasUnknownVersion() {
        ConnectorPartitionTraits traits = createTraits();
        assertEquals(List.of("tbl0"), traits.getPartitionNames());
        Map<String, PartitionInfo> partitions = traits.getPartitionNameWithPartitionInfo();
        assertEquals(Set.of("tbl0"), partitions.keySet());
        assertEquals(-1L, partitions.get("tbl0").getModifiedTime());
        assertEquals(-1L, partitions.get("tbl0").getVersion());
        assertEquals(Set.of("tbl0"), traits.getPartitionNameWithPartitionInfo(List.of("tbl0")).keySet());
        assertTrue(traits.getPartitions(List.of()).isEmpty());
        assertTrue(traits.getPartitionNameWithPartitionInfo(List.of()).isEmpty());
    }

    @Test
    public void testFreshnessRemainsUnknown() {
        assertUnknownFreshness(createTraits());
    }

    @Test
    public void testCachedFreshnessRemainsUnknown() {
        QueryMaterializationContext context = new QueryMaterializationContext();
        ConnectorPartitionTraits traits = new CachedPartitionTraits(Caffeine.newBuilder().build(),
                createTraits(), context.new QueryCacheStats(), null);
        assertUnknownFreshness(traits);
        assertUnknownFreshness(traits);
    }

    private void assertUnknownFreshness(ConnectorPartitionTraits traits) {
        assertFalse(traits.isSupportPCTRefresh());
        assertNull(traits.getUpdatedPartitionNames(List.of(), new MaterializedView.AsyncRefreshContext()));
        assertNull(traits.getUpdatedPartitionNames(LocalDateTime.of(2026, 1, 1, 0, 0), 0));
        assertNull(traits.getTableLastUpdateTime(0));
        assertEquals(Optional.empty(), traits.maxPartitionRefreshTs());
    }
}
