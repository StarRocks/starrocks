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

package com.starrocks.connector.starrocks;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StarRocksStatsUtilsTest {

    @Test
    public void testListCanonicalKeyNullDoesNotCollideWithEmptyString() {
        // A NULL list-partition value and a real empty-string value must map to
        // DISTINCT canonical keys: a collision would drop one partition id from the
        // canonical-key map and lose its rows from the estimate.
        StarRocksRemoteTableStats.Snapshot snapshot = new StarRocksRemoteTableStats.Snapshot();
        snapshot.partitionType = StarRocksRemoteTableStats.PARTITION_TYPE_LIST;
        StarRocksRemoteTableStats.PartitionMeta nullPartition = new StarRocksRemoteTableStats.PartitionMeta();
        nullPartition.id = 1L;
        nullPartition.name = "p_null";
        nullPartition.listValues = Collections.singletonList(Collections.singletonList(null));
        StarRocksRemoteTableStats.PartitionMeta emptyPartition = new StarRocksRemoteTableStats.PartitionMeta();
        emptyPartition.id = 2L;
        emptyPartition.name = "p_empty";
        emptyPartition.listValues = Collections.singletonList(Collections.singletonList(""));
        snapshot.partitions = Arrays.asList(nullPartition, emptyPartition);

        List<Column> partitionColumns =
                Collections.singletonList(new Column("p", StarRocksFeClient.parseType("varchar(10)"), true));
        Map<Long, PartitionKey> keys = StarRocksStatsUtils.buildCanonicalKeys(snapshot, partitionColumns);

        Assertions.assertEquals(2, keys.size());
        Assertions.assertNotEquals(keys.get(1L), keys.get(2L));
    }

    @Test
    public void testRangeCanonicalKeysAreDistinctPerLowerBound() {
        StarRocksRemoteTableStats.Snapshot snapshot = new StarRocksRemoteTableStats.Snapshot();
        snapshot.partitionType = StarRocksRemoteTableStats.PARTITION_TYPE_RANGE;
        StarRocksRemoteTableStats.PartitionMeta p1 = new StarRocksRemoteTableStats.PartitionMeta();
        p1.id = 1L;
        p1.name = "p1";
        p1.rangeLower = rangeBound("10");
        StarRocksRemoteTableStats.PartitionMeta p2 = new StarRocksRemoteTableStats.PartitionMeta();
        p2.id = 2L;
        p2.name = "p2";
        p2.rangeLower = rangeBound("20");
        snapshot.partitions = Arrays.asList(p1, p2);

        List<Column> partitionColumns =
                Collections.singletonList(new Column("k", StarRocksFeClient.parseType("int"), false));
        Map<Long, PartitionKey> keys = StarRocksStatsUtils.buildCanonicalKeys(snapshot, partitionColumns);

        Assertions.assertEquals(2, keys.size());
        Assertions.assertTrue(keys.get(1L).compareTo(keys.get(2L)) < 0);
    }

    private static StarRocksRemoteTableStats.RangeBound rangeBound(String value) {
        StarRocksRemoteTableStats.RangeBound bound = new StarRocksRemoteTableStats.RangeBound();
        bound.values = Collections.singletonList(value);
        return bound;
    }
}
