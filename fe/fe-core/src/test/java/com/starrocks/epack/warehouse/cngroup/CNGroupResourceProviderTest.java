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

package com.starrocks.epack.warehouse.cngroup;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.ErrorReportException;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseTestBase;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.CRAcquireStrategy;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CNGroupResourceProviderTest extends WarehouseTestBase {
    private final CNGroupResourceProvider provider = CNGroupResourceProvider.INSTANCE;

    public static final int LOW_WATERMARK_RUNNING_QUERY_COUNT = (int) GlobalVariable.getCngroupLowWatermarkRunningQueryCount();
    public static final int LOW_WATERMARK_CPU_USED_PERMILLE = (int) GlobalVariable.getCngroupLowWatermarkCPUUsedPermille();

    @Test
    public void testRoundRobinGenerator() {
        AtomicLong generator = new AtomicLong(RandomUtils.nextInt());
        Map<Integer, Integer> counter = Maps.newHashMap();
        int size = 10;
        int start = Math.floorMod(generator.getAndIncrement(), size);
        int idx = 0;
        for (int i = 0; i < size; i++) {
            idx = Math.floorMod(start + i, size);
            counter.put(idx, counter.getOrDefault(idx, 0) + 1);
        }
        System.out.println(counter);
        assertThat(counter).hasSize(size);
        for (int i = 0; i < size; i++) {
            assertThat(counter.get(i)).isEqualTo(1);
        }
    }

    @Test
    public void testAcquireResourceBasic() {
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Optional<ComputeResource> cnResource = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
        assertThat(cnResource).isPresent();
        CNGroupResource cnGroupResource = (CNGroupResource) cnResource.get();
        assertThat(cnGroupResource.getWarehouseId()).isEqualTo(0L);
        assertThat(cnGroupResource.getWorkerGroupId()).isEqualTo(0L);
    }

    @Test
    public void testAcquireResourceWithNullWarehouse() {
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        ErrorReportException e = assertThrows(ErrorReportException.class, () -> {
            provider.acquireComputeResource(null, cnAcquireContext);
        });
        assertThat(e.getMessage()).contains("not exist");
    }

    @Test
    public void testAcquireResourceWithEmptyWarehouse() {
        new MockUp<LocalWarehouse>() {
            @Mock
            public List<Long> getWorkerGroupIds() {
                return List.of();
            }
        };
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Optional<ComputeResource> cnResource = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
        assertThat(cnResource).isEmpty();
    }

    private ComputeResource acquireDefaultWarehouseResource() {
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext acquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Optional<ComputeResource> result = provider.acquireComputeResource(defaultWarehouse, acquireContext);
        assertThat(result.isPresent()).isTrue();
        return result.get();
    }

    @Test
    public void testProviderIsResourceAvailableGood() {
        ComputeResource computeResource = acquireDefaultWarehouseResource();
        assertThat(provider.isResourceAvailable(computeResource)).isTrue();
    }

    @Test
    public void testProviderIsResourceAvailableBad() {
        ComputeResource computeResource = CNGroupResource.of(1, 0);
        assertThat(provider.isResourceAvailable(computeResource)).isFalse();
    }

    @Test
    public void testProviderGetAllComputeNodeIds() {
        ComputeResource computeResource = acquireDefaultWarehouseResource();
        List<Long> result = provider.getAllComputeNodeIds(computeResource);
        assertThat(result).isEqualTo(List.of(10001L));
    }

    @Test
    public void testProviderGetAllComputeNodeIdsBad() {
        ComputeResource computeResource = CNGroupResource.of(1, 0);
        ErrorReportException e = assertThrows(ErrorReportException.class, () -> {
            provider.getAllComputeNodeIds(computeResource);
        });
        assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
    }

    @Test
    public void testAcquireResourceByLocalFirst() {
        new MockUp<LocalWarehouse>() {
            @Mock
            public List<Long> getWorkerGroupIds() {
                return List.of(0L, 1L, 2L);
            }
        };
        new MockUp<CNGroupResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource cnResource) {
                return true;
            }
        };
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        ComputeResource prevComputeResource = CNGroupResource.of(0L, 1L);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                CRAcquireStrategy.LOCAL_FIRST, prevComputeResource);
        for (int i = 0; i < 10; i++) {
            Optional<ComputeResource> cnResource = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
            assertThat(cnResource).isPresent();
            CNGroupResource cnGroupResource = (CNGroupResource) cnResource.get();
            assertThat(cnGroupResource.getWarehouseId()).isEqualTo(0L);
            assertThat(cnGroupResource.getWorkerGroupId()).isEqualTo(1L);
        }
    }

    @Test
    public void testAcquireResourceByRandom() {
        new MockUp<LocalWarehouse>() {
            @Mock
            public List<Long> getWorkerGroupIds() {
                return List.of(0L, 1L, 2L, 3L, 4L);
            }
        };
        new MockUp<CNGroupResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource cnResource) {
                return true;
            }
        };
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                CRAcquireStrategy.RANDOM);
        Map<Long, Long> workerGroupIdToCount = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Optional<ComputeResource> cnResource = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
            assertThat(cnResource).isPresent();

            CNGroupResource cnGroupResource = (CNGroupResource) cnResource.get();
            assertThat(cnGroupResource.getWarehouseId()).isEqualTo(0L);

            long workerGroupId = cnGroupResource.getWorkerGroupId();
            workerGroupIdToCount.put(workerGroupId,
                    workerGroupIdToCount.getOrDefault(workerGroupId, 0L) + 1);
        }
        assertThat(workerGroupIdToCount).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(workerGroupIdToCount.get((long) i)).isEqualTo(2L);
        }
    }

    private List<ComputeNode> mockComputeNodes(int count,
                                               int initialRunningQueries,
                                               int initialCpuUsedPermille) {
        List<ComputeNode> computeNodes = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            ComputeNode c1 = new ComputeNode(10001L + i, "192.168.0." + i, 9050);
            c1.updateResourceUsage(initialRunningQueries + i,
                    100, initialCpuUsedPermille + i);
            computeNodes.add(c1);
        }
        return computeNodes;
    }

    @Test
    public void testAcquireResourceByStandardWithoutLowWatermarks() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        new MockUp<LocalWarehouse>() {
            @Mock
            public List<Long> getWorkerGroupIds() {
                return List.of(0L, 1L, 2L);
            }
        };
        new MockUp<CNGroupResourceUsage>() {
            @Mock
            public boolean isResourceUsageFresh() {
                return true;
            }
            @Mock
            public boolean isUnderLowWatermark() {
                return false;
            }
        };
        new MockUp<CNGroupResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource cnResource) {
                return true;
            }
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource cnResource) {
                final long workerGroupId = cnResource.getWorkerGroupId();
                if (workerGroupId == 0) {
                    return mockComputeNodes(2, 0, 0);
                } else if (workerGroupId == 1) {
                    return mockComputeNodes(3, 0, 0);
                } else if (workerGroupId == 2) {
                    return mockComputeNodes(5, 0, 0);
                } else {
                    return List.of();
                }
            }
        };
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Map<Long, Long> workerGroupIdToCount = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Optional<ComputeResource> cnResource = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
            assertThat(cnResource).isPresent();

            CNGroupResource cnGroupResource = (CNGroupResource) cnResource.get();
            assertThat(cnGroupResource.getWarehouseId()).isEqualTo(0L);

            long workerGroupId = cnGroupResource.getWorkerGroupId();
            workerGroupIdToCount.put(workerGroupId,
                    workerGroupIdToCount.getOrDefault(workerGroupId, 0L) + 1);
        }
        assertThat(workerGroupIdToCount).hasSize(1);
        assertThat(workerGroupIdToCount.get(0L)).isEqualTo(10L); // Only worker group 2 has enough resources
    }

    @Test
    public void testAcquireResourceByStandardWithLowWatermark() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        new MockUp<LocalWarehouse>() {
            @Mock
            public List<Long> getWorkerGroupIds() {
                return List.of(0L, 1L, 2L);
            }
        };
        new MockUp<CNGroupResourceUsage>() {
            @Mock
            public boolean isResourceUsageFresh() {
                return true;
            }
        };
        new MockUp<CNGroupResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource cnResource) {
                return true;
            }
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource cnResource) {
                final long workerGroupId = cnResource.getWorkerGroupId();
                if (workerGroupId == 0) {
                    return mockComputeNodes(2, LOW_WATERMARK_RUNNING_QUERY_COUNT + 1, 0);
                } else if (workerGroupId == 1) {
                    return mockComputeNodes(3, 0, 0);
                } else if (workerGroupId == 2) {
                    return mockComputeNodes(5, LOW_WATERMARK_RUNNING_QUERY_COUNT + 1, 0);
                } else {
                    return List.of();
                }
            }
        };
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Map<Long, Long> workerGroupIdToCount = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Optional<ComputeResource> cnResource = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
            assertThat(cnResource).isPresent();

            CNGroupResource cnGroupResource = (CNGroupResource) cnResource.get();
            assertThat(cnGroupResource.getWarehouseId()).isEqualTo(0L);

            long workerGroupId = cnGroupResource.getWorkerGroupId();
            workerGroupIdToCount.put(workerGroupId,
                    workerGroupIdToCount.getOrDefault(workerGroupId, 0L) + 1);
        }
        assertThat(workerGroupIdToCount).hasSize(1);
        assertThat(workerGroupIdToCount.get(1L)).isEqualTo(10L); // Only worker group 2 has enough resources
    }

    @Test
    public void testAcquireResourceByStandardNonLeader() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        new MockUp<LocalWarehouse>() {
            @Mock
            public List<Long> getWorkerGroupIds() {
                return List.of(0L, 1L, 2L);
            }
        };
        new MockUp<CNGroupResourceUsage>() {
            @Mock
            public boolean isResourceUsageFresh() {
                return true;
            }
        };
        new MockUp<CNGroupResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource cnResource) {
                return true;
            }
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource cnResource) {
                final long workerGroupId = cnResource.getWorkerGroupId();
                if (workerGroupId == 0) {
                    return mockComputeNodes(2, LOW_WATERMARK_RUNNING_QUERY_COUNT + 10, 0);
                } else if (workerGroupId == 1) {
                    return mockComputeNodes(3, 0, 0);
                } else if (workerGroupId == 2) {
                    return mockComputeNodes(5, LOW_WATERMARK_RUNNING_QUERY_COUNT + 20, 0);
                } else {
                    return List.of();
                }
            }
        };
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Map<Long, Long> workerGroupIdToCount = new HashMap<>();
        for (int i = 0; i < 9; i++) {
            Optional<ComputeResource> cnResource = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
            assertThat(cnResource).isPresent();

            CNGroupResource cnGroupResource = (CNGroupResource) cnResource.get();
            assertThat(cnGroupResource.getWarehouseId()).isEqualTo(0L);

            long workerGroupId = cnGroupResource.getWorkerGroupId();
            workerGroupIdToCount.put(workerGroupId,
                    workerGroupIdToCount.getOrDefault(workerGroupId, 0L) + 1);
        }
        System.out.println(workerGroupIdToCount);
        assertThat(workerGroupIdToCount).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(workerGroupIdToCount.get((long) i)).isEqualTo(3L);
        }
    }
}
