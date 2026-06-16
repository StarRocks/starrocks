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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ConnectorStatsInvalidationTest {
    @Test
    public void testInvalidateConnectorTableColumnStatisticsByUuid() {
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        String uuid = "hive0.partitioned_db.t1.1234";
        ConnectorTableColumnKey target = new ConnectorTableColumnKey(uuid, "c1");
        ConnectorTableColumnKey otherColumn = new ConnectorTableColumnKey(uuid, "c2");
        ConnectorTableColumnKey otherTable = new ConnectorTableColumnKey("hive0.partitioned_db.t2.1235", "c1");
        cachedStatisticStorage.connectorTableCachedStatistics.synchronous().put(target, Optional.empty());
        cachedStatisticStorage.connectorTableCachedStatistics.synchronous().put(otherColumn, Optional.empty());
        cachedStatisticStorage.connectorTableCachedStatistics.synchronous().put(otherTable, Optional.empty());

        cachedStatisticStorage.invalidateConnectorTableColumnStatistics(null, ImmutableList.of("c1"));
        cachedStatisticStorage.invalidateConnectorTableColumnStatistics("", ImmutableList.of("c1"));
        cachedStatisticStorage.invalidateConnectorTableColumnStatistics(uuid, null);
        Assertions.assertNotNull(cachedStatisticStorage.connectorTableCachedStatistics.synchronous().getIfPresent(target));

        cachedStatisticStorage.invalidateConnectorTableColumnStatistics(uuid, ImmutableList.of("c1"));
        Assertions.assertNull(cachedStatisticStorage.connectorTableCachedStatistics.synchronous().getIfPresent(target));
        Assertions.assertNotNull(cachedStatisticStorage.connectorTableCachedStatistics.synchronous().getIfPresent(otherColumn));
        Assertions.assertNotNull(cachedStatisticStorage.connectorTableCachedStatistics.synchronous().getIfPresent(otherTable));
    }

    @Test
    public void testInvalidateConnectorHistogramStatisticsByUuid() {
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        String uuid = "hive0.partitioned_db.t1.1234";
        ConnectorTableColumnKey target = new ConnectorTableColumnKey(uuid, "c1");
        ConnectorTableColumnKey otherColumn = new ConnectorTableColumnKey(uuid, "c2");
        ConnectorTableColumnKey otherTable = new ConnectorTableColumnKey("hive0.partitioned_db.t2.1235", "c1");
        cachedStatisticStorage.connectorHistogramCache.synchronous().put(target, Optional.empty());
        cachedStatisticStorage.connectorHistogramCache.synchronous().put(otherColumn, Optional.empty());
        cachedStatisticStorage.connectorHistogramCache.synchronous().put(otherTable, Optional.empty());

        cachedStatisticStorage.invalidateConnectorHistogramStatistics(null, ImmutableList.of("c1"));
        cachedStatisticStorage.invalidateConnectorHistogramStatistics("", ImmutableList.of("c1"));
        cachedStatisticStorage.invalidateConnectorHistogramStatistics(uuid, null);
        Assertions.assertNotNull(cachedStatisticStorage.connectorHistogramCache.synchronous().getIfPresent(target));

        cachedStatisticStorage.invalidateConnectorHistogramStatistics(uuid, ImmutableList.of("c1"));
        Assertions.assertNull(cachedStatisticStorage.connectorHistogramCache.synchronous().getIfPresent(target));
        Assertions.assertNotNull(cachedStatisticStorage.connectorHistogramCache.synchronous().getIfPresent(otherColumn));
        Assertions.assertNotNull(cachedStatisticStorage.connectorHistogramCache.synchronous().getIfPresent(otherTable));
    }

}
