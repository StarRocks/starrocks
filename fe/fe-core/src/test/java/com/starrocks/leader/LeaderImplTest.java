// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.leader;

import com.google.common.collect.Sets;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.starrocks.catalog.Replica.ReplicaState.NORMAL;

public class LeaderImplTest {

    private long dbId;
    private String dbName;
    private String tableName;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long replicaId;
    private long backendId;

    private final LeaderImpl leader = new LeaderImpl();

    @Before
    public void setUp() {
        dbId = 1L;
        dbName = "database0";
        tableName = "table0";
        tableId = 10L;
        partitionId = 11L;
        indexId = 12L;
        tabletId = 13L;
        replicaId = 14L;
        backendId = 15L;
    }

    @Test
    public void testFindRelatedReplica(@Mocked OlapTable olapTable, @Mocked LakeTable lakeTable,
                                       @Mocked Partition partition, @Mocked MaterializedIndex index
                                       ) throws Exception {

        // olap table
        new Expectations() {
            {
                partition.getIndex(indexId);
                result = index;
                index.getTablet(tabletId);
                result = new LocalTablet(tabletId);
            }
        };
        
        Assert.assertNull(Deencapsulation.invoke(leader, "findRelatedReplica",
                olapTable, partition, backendId, tabletId, indexId));
        // lake table
        new MockUp<LakeTablet>() {
            @Mock
            public Set<Long> getBackendIds() {
                return Sets.newHashSet();
            }
        };

        new Expectations() {
            {
                partition.getIndex(indexId);
                result = index;
                index.getTablet(tabletId);
                result = new LakeTablet(tabletId);
            }
        };

        Assert.assertEquals(new Replica(tabletId, backendId, -1, NORMAL), Deencapsulation.invoke(leader, "findRelatedReplica",
                olapTable, partition, backendId, tabletId, indexId));
    }
}
