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
import com.starrocks.qe.GlobalVariable;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class CNGroupResourceUsageTest {
    public static final int LOW_WATERMARK_RUNNING_QUERY_COUNT = (int) GlobalVariable.getCngroupLowWatermarkRunningQueryCount();
    public static final int LOW_WATERMARK_CPU_USED_PERMILLE = (int) GlobalVariable.getCngroupLowWatermarkCPUUsedPermille();

    @Test
    public void testBasic() {
        CNGroupResource resource1 = CNGroupResource.of(1, 1);
        CNGroupResourceUsage usage1 = new CNGroupResourceUsage(
                resource1, 5, 10, 50.0, 5);

        assertThat(usage1.getCnGroupResource()).isEqualTo(resource1);
        assertThat(usage1.getAliveComputeNodeCount()).isEqualTo(5);
        assertThat(usage1.getMaxRunningQueries()).isEqualTo(10);
        assertThat(usage1.getAvgCpuUsedPermille()).isEqualTo(50.0);
        assertThat(usage1.getFreshComputeNodeCount()).isEqualTo(5);

        assertThat(usage1.toString()).contains(
                        "aliveComputeNodeCount=5, freshComputeNodeCount=5, avgCpuUsedPermille=50.0, maxRunningQueries=10}");
    }

    @Test
    public void testCompareTo() {
        CNGroupResource resource1 = CNGroupResource.of(1, 1);
        CNGroupResource resource2 = CNGroupResource.of(1, 2);
        {
            CNGroupResourceUsage usage1 = new CNGroupResourceUsage(
                    resource1, 5, 10, 50.0, 5);
            CNGroupResourceUsage usage2 = new CNGroupResourceUsage(
                    resource2, 5, 10, 50.0, 5);
            assertThat(usage1.compareTo(usage2)).isEqualTo(0);
        }
        {
            CNGroupResourceUsage usage1 = new CNGroupResourceUsage(
                    resource1, 5, 10, 50.0, 5);
            CNGroupResourceUsage usage2 = new CNGroupResourceUsage(
                    resource2, 5, 20, 50.0, 5);
            assertThat(usage1.compareTo(usage2)).isLessThan(0);
        }
        {
            CNGroupResourceUsage usage1 = new CNGroupResourceUsage(
                    resource1, 5, 10, 500.0, 5);
            CNGroupResourceUsage usage2 = new CNGroupResourceUsage(
                    resource2, 10, 10, 500.0, 5);
            assertThat(usage1.compareTo(usage2)).isLessThan(0);
        }
        {
            CNGroupResourceUsage usage1 = new CNGroupResourceUsage(
                    resource1, 5, 10, 50.0, 5);
            CNGroupResourceUsage usage2 = new CNGroupResourceUsage(
                    resource2, 5, 10, 100.0, 5);
            assertThat(usage1.compareTo(usage2)).isLessThan(0);
        }
        {
            CNGroupResourceUsage usage1 = new CNGroupResourceUsage(
                    resource1, 5, 10, 50.0, 5);
            CNGroupResourceUsage usage2 = new CNGroupResourceUsage(
                    resource2, 5, 10, 50.0, 10);
            assertThat(usage1.compareTo(usage2)).isEqualTo(0);
        }
    }

    @Test
    public void testFindBestByUsage1() {
        {
            Optional<ComputeResource> result = CNGroupResourceUsage.findBestByUsage(null);
            assertThat(result.isEmpty());
        }
        {
            Optional<ComputeResource> result = CNGroupResourceUsage.findBestByUsage(List.of());
            assertThat(result.isEmpty());
        }
    }

    @Test
    public void testFindBestByUsage2() {
        Optional<ComputeResource> result = CNGroupResourceUsage.findBestByUsage(
                List.of(
                        new CNGroupResourceUsage(CNGroupResource.of(1, 1), 5, 10, 500.0, 5),
                        new CNGroupResourceUsage(CNGroupResource.of(1, 2), 10, 20, 300.0, 10),
                        new CNGroupResourceUsage(CNGroupResource.of(1, 3), 15, 15, 400.0, 15)
                )
        );
        assertThat(result.isPresent());
        assertThat(result.get()).isInstanceOf(CNGroupResource.class);
        assertThat(result.get().getWarehouseId()).isEqualTo(1);
        assertThat(result.get().getWorkerGroupId()).isEqualTo(2);
    }

    @Test
    public void testFindBestByUsage3() {
        Optional<ComputeResource> result = CNGroupResourceUsage.findBestByUsage(
                List.of(
                        new CNGroupResourceUsage(CNGroupResource.of(1, 1), 10, 10, 50.0, 5),
                        new CNGroupResourceUsage(CNGroupResource.of(1, 2), 10, 20, 50.0, 10),
                        new CNGroupResourceUsage(CNGroupResource.of(1, 3), 10, 15, 50.0, 15)
                )
        );
        assertThat(result.isPresent());
        assertThat(result.get()).isInstanceOf(CNGroupResource.class);
        assertThat(result.get().getWarehouseId()).isEqualTo(1);
        assertThat(result.get().getWorkerGroupId()).isEqualTo(1);
    }

    private List<ComputeNode> mockComputeNodes(int count,
                                               int initialRunningQueries,
                                               int initialCpuUsedPermille) {
        List<ComputeNode> computeNodes = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
            c1.updateResourceUsage(initialRunningQueries + i,
                    100, initialCpuUsedPermille + i);
            computeNodes.add(c1);
        }
        return computeNodes;
    }

    @Test
    public void testBasicWithComputeNode() {
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        List<ComputeNode> computeNodes1 = mockComputeNodes(5, 10, 50);
        CNGroupResourceUsage usage1 = CNGroupResourceUsage.of(CNGroupResource.of(1, 1), computeNodes1);
        assertThat(usage1.getCnGroupResource()).isEqualTo(CNGroupResource.of(1, 1));
        assertThat(usage1.getAliveComputeNodeCount()).isEqualTo(5);
        assertThat(usage1.getMaxRunningQueries()).isEqualTo(10 + 4);
        assertThat(usage1.getAvgCpuUsedPermille()).isEqualTo(50 + 2.0);
    }

    @Test
    public void testFindBestByUsageWithComputeNodes1() {
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        CNGroupResourceUsage usage1 = CNGroupResourceUsage.of(CNGroupResource.of(1, 1),
                mockComputeNodes(5, 10, 500));
        CNGroupResourceUsage usage2 = CNGroupResourceUsage.of(CNGroupResource.of(1, 2),
                mockComputeNodes(10, 20, 300));
        Optional<ComputeResource> result = CNGroupResourceUsage.findBestByUsage(
                List.of(
                        usage1,
                        usage2
                )
        );
        assertThat(result.isPresent());
        assertThat(result.get()).isInstanceOf(CNGroupResource.class);
        assertThat(result.get().getWarehouseId()).isEqualTo(1);
        assertThat(result.get().getWorkerGroupId()).isEqualTo(2);
    }

    @Test
    public void testFindBestByUsageWithComputeNodes2() {
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        CNGroupResourceUsage usage1 = CNGroupResourceUsage.of(CNGroupResource.of(1, 1),
                mockComputeNodes(10, 10, 30));
        CNGroupResourceUsage usage2 = CNGroupResourceUsage.of(CNGroupResource.of(1, 2),
                mockComputeNodes(10, 20, 50));
        Optional<ComputeResource> result = CNGroupResourceUsage.findBestByUsage(
                List.of(
                        usage1,
                        usage2
                )
        );
        assertThat(result.isPresent());
        assertThat(result.get()).isInstanceOf(CNGroupResource.class);
        assertThat(result.get().getWarehouseId()).isEqualTo(1);
        assertThat(result.get().getWorkerGroupId()).isEqualTo(1);
    }

    private List<ComputeNode> mockNonRefreshComputeNodes(int count) {
        List<ComputeNode> computeNodes = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
            computeNodes.add(c1);

            assertThat(c1.isResourceUsageFresh()).isFalse();
        }
        return computeNodes;
    }

    @Test
    public void testIsResourceUsageFresh() {
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        {
            CNGroupResourceUsage usage1 = CNGroupResourceUsage.of(CNGroupResource.of(1, 1),
                    mockComputeNodes(10, 10, 50));
            assertThat(usage1.isResourceUsageFresh()).isTrue();
        }

        {
            List<ComputeNode> computeNodes = mockComputeNodes(10, 10, 50);
            computeNodes.addAll(mockNonRefreshComputeNodes(5));
            CNGroupResourceUsage usage2 = CNGroupResourceUsage.of(CNGroupResource.of(1, 2), computeNodes);
            assertThat(usage2.isResourceUsageFresh()).isTrue();

            computeNodes.addAll(mockNonRefreshComputeNodes(5));
            usage2 = CNGroupResourceUsage.of(CNGroupResource.of(1, 2), computeNodes);
            assertThat(usage2.isResourceUsageFresh()).isTrue();

            computeNodes.addAll(mockNonRefreshComputeNodes(5));
            usage2 = CNGroupResourceUsage.of(CNGroupResource.of(1, 2), computeNodes);
            assertThat(usage2.isResourceUsageFresh()).isFalse();
        }
    }

    @Test
    public void testIsUnderLowWatermark() {
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAvailable() {
                return true;
            }
        };
        {
            CNGroupResourceUsage usage1 = CNGroupResourceUsage.of(CNGroupResource.of(1, 1),
                    mockComputeNodes(2, 2, 50));
            assertThat(usage1.isUnderLowWatermark()).isTrue();
        }
        {
            CNGroupResourceUsage usage1 = CNGroupResourceUsage.of(CNGroupResource.of(1, 1),
                    mockComputeNodes(2, (int) LOW_WATERMARK_RUNNING_QUERY_COUNT + 1, 50));
            assertThat(usage1.isUnderLowWatermark()).isFalse();
        }
        {
            CNGroupResourceUsage usage1 = CNGroupResourceUsage.of(CNGroupResource.of(1, 1),
                    mockComputeNodes(2, (int) LOW_WATERMARK_RUNNING_QUERY_COUNT - 3,
                            (int) LOW_WATERMARK_CPU_USED_PERMILLE + 1));
            assertThat(usage1.isUnderLowWatermark()).isFalse();
        }
    }
}
