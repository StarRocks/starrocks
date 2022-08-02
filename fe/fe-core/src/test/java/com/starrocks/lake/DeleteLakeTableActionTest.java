package com.starrocks.lake;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.DdlException;
import com.starrocks.lake.proto.DropTableRequest;
import com.starrocks.rpc.LakeServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DeleteLakeTableActionTest {
    @Test
    public void testEmptyTable(@Mocked LakeTable table, @Mocked OlapTable olapTable) {
        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.emptyList();
        }};
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertTrue(action.execute());
    }

    @Test
    public void testEmptyPartition(@Mocked LakeTable table, @Mocked OlapTable olapTable, @Mocked Partition partition) {
        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(partition);
        }};
        new Expectations() {{
            partition.getMaterializedIndices((MaterializedIndex.IndexExtState) any);
            minTimes = 1;
            maxTimes = 1;
            result = Collections.emptyList();
        }};
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertTrue(action.execute());
    }

    @Test
    public void testEmptyIndex(@Mocked LakeTable table, @Mocked OlapTable olapTable, @Mocked Partition partition,
                               @Mocked MaterializedIndex index) {
        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(partition);
        }};
        new Expectations() {{
            partition.getMaterializedIndices((MaterializedIndex.IndexExtState) any);
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(index);
        }};
        new Expectations() {{
            index.getTablets();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.emptyList();
        }};
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertTrue(action.execute());
    }

    @Test
    public void testNoBackend(@Mocked LakeTable table, @Mocked OlapTable olapTable, @Mocked Partition partition,
                              @Mocked MaterializedIndex index) {
        LakeTablet tablet = new LakeTablet(10) {
            @Override
            public long getDataSize(boolean singleReplica) {
                return 0;
            }

            @Override
            public long getRowCount(long version) {
                return 0;
            }

            @Override
            public Set<Long> getBackendIds() {
                return null;
            }

            @Override
            public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas, long visibleVersion,
                                             long localBeId, int schemaHash) {
            }
        };

        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(partition);
        }};
        new Expectations() {{
            partition.getMaterializedIndices((MaterializedIndex.IndexExtState) any);
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(index);
        }};
        new Expectations() {{
            index.getTablets();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(tablet);
        }};
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return null;
            }
        };
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertFalse(action.execute());
    }

    @Test
    public void testNoBackend2(@Mocked LakeTable table, @Mocked OlapTable olapTable, @Mocked Partition partition,
                               @Mocked MaterializedIndex index,
                               @Mocked SystemInfoService systemInfoService) {
        LakeTablet tablet = new LakeTablet(10) {
            @Override
            public long getDataSize(boolean singleReplica) {
                return 0;
            }

            @Override
            public long getRowCount(long version) {
                return 0;
            }

            @Override
            public Set<Long> getBackendIds() {
                return null;
            }

            @Override
            public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas, long visibleVersion,
                                             long localBeId, int schemaHash) {
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 10L;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };
        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(partition);
        }};
        new Expectations() {{
            partition.getMaterializedIndices((MaterializedIndex.IndexExtState) any);
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(index);
        }};
        new Expectations() {{
            index.getTablets();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(tablet);
        }};
        new Expectations() {{
            systemInfoService.getBackend(anyLong);
            minTimes = 1;
            maxTimes = 1;
            result = null;
        }};
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertFalse(action.execute());
    }

    @Test
    public void testRPCFailed(@Mocked LakeTable table, @Mocked OlapTable olapTable, @Mocked Partition partition,
                              @Mocked MaterializedIndex index,
                              @Mocked SystemInfoService systemInfoService,
                              @Mocked LakeServiceClient lakeServiceClient) throws RpcException {
        Backend backend = new Backend(1000, "127.0.0.1", 8000);
        LakeTablet tablet = new LakeTablet(10) {
            @Override
            public long getDataSize(boolean singleReplica) {
                return 0;
            }

            @Override
            public long getRowCount(long version) {
                return 0;
            }

            @Override
            public Set<Long> getBackendIds() {
                return null;
            }

            @Override
            public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas, long visibleVersion,
                                             long localBeId, int schemaHash) {
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 10L;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };
        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(partition);
        }};
        new Expectations() {{
            partition.getMaterializedIndices((MaterializedIndex.IndexExtState) any);
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(index);
        }};
        new Expectations() {{
            index.getTablets();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(tablet);
        }};
        new Expectations() {{
            systemInfoService.getBackend(anyLong);
            minTimes = 1;
            maxTimes = 1;
            result = backend;
        }};
        new Expectations() {{
            lakeServiceClient.dropTable((DropTableRequest) any);
            minTimes = 1;
            maxTimes = 1;
            result = new RpcException("127.0.0.1", "mocked exception");
        }};
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertFalse(action.execute());
    }

    @Test
    public void testDeleteShardFailed(@Mocked LakeTable table, @Mocked OlapTable olapTable, @Mocked Partition partition,
                                      @Mocked MaterializedIndex index,
                                      @Mocked SystemInfoService systemInfoService,
                                      @Mocked LakeServiceClient lakeServiceClient,
                                      @Mocked StarOSAgent starOSAgent) throws RpcException, DdlException {
        Backend backend = new Backend(1000, "127.0.0.1", 8000);
        LakeTablet tablet = new LakeTablet(10) {
            @Override
            public long getDataSize(boolean singleReplica) {
                return 0;
            }

            @Override
            public long getRowCount(long version) {
                return 0;
            }

            @Override
            public Set<Long> getBackendIds() {
                return null;
            }

            @Override
            public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas, long visibleVersion,
                                             long localBeId, int schemaHash) {
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 10L;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }
        };
        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(partition);
        }};
        new Expectations() {{
            partition.getMaterializedIndices((MaterializedIndex.IndexExtState) any);
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(index);
        }};
        new Expectations() {{
            index.getTablets();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(tablet);
        }};
        new Expectations() {{
            systemInfoService.getBackend(anyLong);
            minTimes = 1;
            maxTimes = 1;
            result = backend;
        }};
        new Expectations() {{
            lakeServiceClient.dropTable((DropTableRequest) any);
            minTimes = 1;
            maxTimes = 1;
            result = any;
        }};
        new Expectations() {{
            starOSAgent.deleteShards((Set<Long>) any);
            minTimes = 1;
            maxTimes = 1;
            result = new DdlException("mocked ddl exception");
        }};
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertFalse(action.execute());
    }

    @Test
    public void testSuccess(@Mocked LakeTable table, @Mocked OlapTable olapTable, @Mocked Partition partition,
                            @Mocked MaterializedIndex index,
                            @Mocked SystemInfoService systemInfoService,
                            @Mocked LakeServiceClient lakeServiceClient,
                            @Mocked StarOSAgent starOSAgent) throws RpcException, DdlException {
        Backend backend = new Backend(1000, "127.0.0.1", 8000);
        LakeTablet tablet = new LakeTablet(10) {
            @Override
            public long getDataSize(boolean singleReplica) {
                return 0;
            }

            @Override
            public long getRowCount(long version) {
                return 0;
            }

            @Override
            public Set<Long> getBackendIds() {
                return null;
            }

            @Override
            public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas, long visibleVersion,
                                             long localBeId, int schemaHash) {
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 10L;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }
        };
        new Expectations() {{
            olapTable.getPartitions();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(partition);
        }};
        new Expectations() {{
            partition.getMaterializedIndices((MaterializedIndex.IndexExtState) any);
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(index);
        }};
        new Expectations() {{
            index.getTablets();
            minTimes = 1;
            maxTimes = 1;
            result = Collections.singletonList(tablet);
        }};
        new Expectations() {{
            systemInfoService.getBackend(anyLong);
            minTimes = 1;
            maxTimes = 1;
            result = backend;
        }};
        new Expectations() {{
            lakeServiceClient.dropTable((DropTableRequest) any);
            minTimes = 1;
            maxTimes = 1;
            result = any;
        }};
        new Expectations() {{
            starOSAgent.deleteShards((Set<Long>) any);
            minTimes = 1;
            maxTimes = 1;
            result = any;
        }};
        DeleteLakeTableAction action = new DeleteLakeTableAction(table);
        Assert.assertTrue(action.execute());
    }
}
