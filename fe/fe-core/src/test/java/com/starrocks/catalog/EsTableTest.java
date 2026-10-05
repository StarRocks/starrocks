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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.elasticsearch.EsMetaStateTracker;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.connector.elasticsearch.SearchContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.type.IntegerType;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsTableTest {

    /**
     * Stands in for the three HTTP phases: each sync "finds" the field mappings in {@code mappings}, keyed by
     * column, and a fresh EsTablePartitions.
     */
    private static void mockSyncFinding(Map<String, String> mappings) {
        new MockUp<EsMetaStateTracker>() {
            @Mock
            public SearchContext run(Invocation invocation) {
                SearchContext context = ((EsMetaStateTracker) invocation.getInvokedInstance()).searchContext();
                context.fetchFieldsContext().putAll(mappings);
                context.docValueFieldsContext().putAll(mappings);
                return context;
            }
        };
        new MockUp<SearchContext>() {
            @Mock
            public EsTablePartitions tablePartitions() {
                return new EsTablePartitions();
            }
        };
    }

    private static EsTable newEsTable() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(EsTable.KEY_HOSTS, "http://127.0.0.1:8200");
        props.put(EsTable.KEY_INDEX, "doe");
        props.put(EsTable.KEY_TYPE, "doc");
        props.put(EsTable.KEY_VERSION, "6.5.3");
        return new EsTable(1L, "doe", List.of(new Column("k1", IntegerType.BIGINT),
                new Column("k2", IntegerType.BIGINT)), props, null);
    }

    /**
     * The background sync writes the table without any lock, and a query reads it without one. So a sync never
     * touches what a query may already hold: it publishes a new snapshot, and the mappings of the previous one
     * stay exactly as they were -- including a field the index no longer has, which used to linger in the
     * shared map forever.
     */
    @Test
    public void testSyncPublishesANewSnapshotAndLeavesTheOldOneAlone() throws Exception {
        EsTable table = newEsTable();
        Assertions.assertFalse(table.isMetaLockTarget());
        Assertions.assertNull(table.getEsTablePartitions());
        Assertions.assertEquals(Map.of(), table.getMetaSnapshot().getFieldsContext());

        mockSyncFinding(ImmutableMap.of("k1", "k1.keyword"));
        table.syncTableMetaData(null);
        EsTable.MetaSnapshot first = table.getMetaSnapshot();
        Assertions.assertNotNull(first.getPartitions());
        Assertions.assertEquals(Map.of("k1", "k1.keyword"), first.getFieldsContext());

        mockSyncFinding(ImmutableMap.of("k2", "k2.keyword"));
        table.syncTableMetaData(null);
        EsTable.MetaSnapshot second = table.getMetaSnapshot();
        Assertions.assertNotSame(first, second);
        Assertions.assertNotSame(first.getPartitions(), second.getPartitions());
        Assertions.assertEquals(Map.of("k1", "k1.keyword"), first.getFieldsContext());
        Assertions.assertEquals(Map.of("k1", "k1.keyword"), first.getDocValueContext());
        Assertions.assertEquals(Map.of("k2", "k2.keyword"), second.getFieldsContext());
        Assertions.assertEquals(Map.of("k2", "k2.keyword"), second.getDocValueContext());
    }

    /**
     * A failed sync leaves the table without shard routing and reports why, in the same swap; the mappings of
     * the last good sync are kept. The next good sync clears the failure.
     */
    @Test
    public void testFailedSyncIsPublishedInOneSwap() throws Exception {
        EsTable table = newEsTable();
        mockSyncFinding(ImmutableMap.of("k1", "k1.keyword"));
        table.syncTableMetaData(null);
        EsTable.MetaSnapshot good = table.getMetaSnapshot();

        RuntimeException failure = new RuntimeException("es is down");
        table.markMetaDataSyncFailed(failure);
        EsTable.MetaSnapshot failed = table.getMetaSnapshot();
        Assertions.assertNull(failed.getPartitions());
        Assertions.assertSame(failure, failed.getSyncException());
        Assertions.assertEquals(good.getFieldsContext(), failed.getFieldsContext());
        Assertions.assertNotNull(good.getPartitions());
        Assertions.assertNull(good.getSyncException());

        table.syncTableMetaData(null);
        Assertions.assertNotNull(table.getEsTablePartitions());
        Assertions.assertNull(table.getMetaSnapshot().getSyncException());
    }

    /**
     * A scan takes the snapshot once, when it is built, and carries that one to the physical operator: a sync
     * in between does not reach it, so the shard routing it prunes and the mappings it sends come from one sync.
     */
    @Test
    public void testScanOperatorsKeepTheSnapshotTheyWereBuiltWith() throws Exception {
        EsTable table = newEsTable();
        mockSyncFinding(ImmutableMap.of("k1", "k1.keyword"));
        table.syncTableMetaData(null);
        EsTable.MetaSnapshot planned = table.getMetaSnapshot();

        LogicalEsScanOperator logical = new LogicalEsScanOperator(table, new HashMap<>(), new HashMap<>(), -1,
                null, null);
        mockSyncFinding(ImmutableMap.of("k2", "k2.keyword"));
        table.syncTableMetaData(null);
        Assertions.assertNotSame(planned, table.getMetaSnapshot());

        Assertions.assertSame(planned, logical.getMetaSnapshot());
        Assertions.assertSame(planned.getPartitions(), logical.getEsTablePartitions());
        LogicalEsScanOperator rebuilt = new LogicalEsScanOperator.Builder().withOperator(logical).build();
        Assertions.assertSame(planned, rebuilt.getMetaSnapshot());
        Assertions.assertSame(planned, new PhysicalEsScanOperator(logical).getMetaSnapshot());
    }
}
