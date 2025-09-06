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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test class for SystemHandler.canForceDrop method
 */
public class SystemHandlerCanForceDropTest {

    private SystemHandler systemHandler;

    @BeforeEach
    public void setUp() throws Exception {
        systemHandler = new SystemHandler();
    }

    @Test
    public void testCanForceDropEmptyTablets() {
        // Test case: empty tablet list should return true
        List<Long> emptyTabletIds = new ArrayList<>();
        boolean result = systemHandler.canForceDrop(emptyTabletIds);
        Assertions.assertTrue(result, "Empty tablet list should return true");
    }

    @Test
    public void testCanForceDropSharedDataMode() {
        // Test case: shared data mode should return false
        List<Long> tabletIds = Lists.newArrayList(1L, 2L, 3L);
        
        // Mock RunMode to return shared data mode
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return true;
            }
        };
        
        boolean result = systemHandler.canForceDrop(tabletIds);
        Assertions.assertFalse(result, "Shared data mode should return false");
    }

    @Test
    public void testCanForceDropRecycleBinInterval() throws Exception {
        // Test case: within recycle bin check interval should return false
        List<Long> tabletIds = Lists.newArrayList(1L, 2L, 3L);
        
        // Mock RunMode to return shared nothing mode
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return false;
            }
        };
        
        // First call to set lastRecycleBinCheckTime
        systemHandler.canForceDrop(tabletIds);
        
        // Second call within interval should return false
        boolean result = systemHandler.canForceDrop(tabletIds);
        Assertions.assertFalse(result, "Within recycle bin check interval should return false");
    }

    @Test
    public void testCanForceDropNoAvailableBackends() throws Exception {
        // Test case: no available backends should return false
        List<Long> tabletIds = Lists.newArrayList(1L, 2L, 3L);
        
        // Mock RunMode to return shared nothing mode
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return false;
            }
        };
        
        // Mock SystemInfoService to return empty available backends
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                return new ArrayList<>();
            }
        };
        
        boolean result = systemHandler.canForceDrop(tabletIds);
        Assertions.assertFalse(result, "No available backends should return false");
    }

    @Test
    public void testCanForceDropTabletNotInRecycleBin() throws Exception {
        // Test case: tablet not in recycle bin should return false
        List<Long> tabletIds = Lists.newArrayList(1L, 2L, 3L);
        
        // Mock RunMode to return shared nothing mode
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return false;
            }
        };
        
        // Mock SystemInfoService to return available backends
        List<Backend> availableBackends = Lists.newArrayList(
            new Backend(1L, "host1", 1000),
            new Backend(2L, "host2", 1000)
        );
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                return availableBackends;
            }
            
            @Mock
            public List<Backend> getRetainedBackends() {
                return Lists.newArrayList(availableBackends.get(0));
            }
        };
        
        // Mock CatalogRecycleBin to return false for isTabletInRecycleBin
        new MockUp<CatalogRecycleBin>() {
            @Mock
            public boolean isTabletInRecycleBin(TabletMeta tabletMeta) {
                return false;
            }
        };
        
        // Mock TabletInvertedIndex
        mockTabletInvertedIndex();
        
        boolean result = systemHandler.canForceDrop(tabletIds);
        Assertions.assertFalse(result, "Tablet not in recycle bin should return false");
    }

    @Test
    public void testCanForceDropInsufficientReplicas() throws Exception {
        // Test case: insufficient replicas should return false
        List<Long> tabletIds = Lists.newArrayList(1L, 2L, 3L);
        
        // Mock RunMode to return shared nothing mode
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return false;
            }
        };
        
        // Mock SystemInfoService to return available backends
        List<Backend> availableBackends = Lists.newArrayList(
            new Backend(1L, "host1", 1000),
            new Backend(2L, "host2", 1000)
        );
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                return availableBackends;
            }
            
            @Mock
            public List<Backend> getRetainedBackends() {
                return availableBackends;
            }
        };
        
        // Mock CatalogRecycleBin to return true for isTabletInRecycleBin
        new MockUp<CatalogRecycleBin>() {
            @Mock
            public boolean isTabletInRecycleBin(TabletMeta tabletMeta) {
                return true;
            }
        };
        
        // Mock TabletInvertedIndex with insufficient replicas
        mockTabletInvertedIndexWithInsufficientReplicas();
        
        boolean result = systemHandler.canForceDrop(tabletIds);
        Assertions.assertFalse(result, "Insufficient replicas should return false");
    }

    @Test
    public void testCanForceDropNoNormalReplica() throws Exception {
        // Test case: no normal replica on retained backends should return false
        List<Long> tabletIds = Lists.newArrayList(1L, 2L, 3L);
        
        // Mock RunMode to return shared nothing mode
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return false;
            }
        };
        
        // Mock SystemInfoService to return available backends
        List<Backend> availableBackends = Lists.newArrayList(
            new Backend(1L, "host1", 1000),
            new Backend(2L, "host2", 1000)
        );
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                return availableBackends;
            }
            
            @Mock
            public List<Backend> getRetainedBackends() {
                return Lists.newArrayList(availableBackends.get(0));
            }
        };
        
        // Mock CatalogRecycleBin to return true for isTabletInRecycleBin
        new MockUp<CatalogRecycleBin>() {
            @Mock
            public boolean isTabletInRecycleBin(TabletMeta tabletMeta) {
                return true;
            }
        };
        
        // Mock TabletInvertedIndex with no normal replica on retained backends
        mockTabletInvertedIndexWithNoNormalReplica();
        
        boolean result = systemHandler.canForceDrop(tabletIds);
        Assertions.assertFalse(result, "No normal replica on retained backends should return false");
    }

    @Test
    public void testCanForceDropSuccess() throws Exception {
        // Test case: all conditions met should return true
        List<Long> tabletIds = Lists.newArrayList(1L, 2L, 3L);
        
        // Mock RunMode to return shared nothing mode
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return false;
            }
        };
        
        // Mock SystemInfoService to return available backends
        List<Backend> availableBackends = Lists.newArrayList(
            new Backend(1L, "host1", 1000),
            new Backend(2L, "host2", 1000)
        );
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                return availableBackends;
            }
            
            @Mock
            public List<Backend> getRetainedBackends() {
                return Lists.newArrayList(availableBackends.get(0));
            }
        };
        
        // Mock CatalogRecycleBin to return true for isTabletInRecycleBin
        new MockUp<CatalogRecycleBin>() {
            @Mock
            public boolean isTabletInRecycleBin(TabletMeta tabletMeta) {
                return true;
            }
        };
        
        // Mock TabletInvertedIndex with sufficient replicas and normal replica on retained backends
        mockTabletInvertedIndexWithSuccess();
        
        boolean result = systemHandler.canForceDrop(tabletIds);
        Assertions.assertTrue(result, "All conditions met should return true");
    }

    private void mockTabletInvertedIndex() {
        new MockUp<TabletInvertedIndex>() {
            @Mock
            public TabletMeta getTabletMeta(long tabletId) {
                return new TabletMeta(1L, 1L, 1L, 1L, null, false);
            }
            
            @Mock
            public Map<Long, Replica> getReplicas(long tabletId) {
                Map<Long, Replica> replicas = new HashMap<>();
                replicas.put(1L, new Replica(1L, 1L, 1L, 1, 0L, 0L, Replica.ReplicaState.NORMAL, -1L, 1L));
                return replicas;
            }
        };
    }

    private void mockTabletInvertedIndexWithInsufficientReplicas() {
        new MockUp<TabletInvertedIndex>() {
            @Mock
            public TabletMeta getTabletMeta(long tabletId) {
                return new TabletMeta(1L, 1L, 1L, 1L, null, false);
            }
            
            @Mock
            public Map<Long, Replica> getReplicas(long tabletId) {
                Map<Long, Replica> replicas = new HashMap<>();
                replicas.put(1L, new Replica(1L, 1L, 1L, 1, 0L, 0L, Replica.ReplicaState.NORMAL, -1L, 1L));
                return replicas;
            }
        };
    }

    private void mockTabletInvertedIndexWithNoNormalReplica() {
        new MockUp<TabletInvertedIndex>() {
            @Mock
            public TabletMeta getTabletMeta(long tabletId) {
                return new TabletMeta(1L, 1L, 1L, 1L, null, false);
            }
            
            @Mock
            public Map<Long, Replica> getReplicas(long tabletId) {
                Map<Long, Replica> replicas = new HashMap<>();
                // Create replica on non-retained backend (backendId = 2)
                replicas.put(1L, new Replica(1L, 2L, 1L, 1, 0L, 0L, Replica.ReplicaState.NORMAL, -1L, 1L));
                return replicas;
            }
        };
    }

    private void mockTabletInvertedIndexWithSuccess() {
        new MockUp<TabletInvertedIndex>() {
            @Mock
            public TabletMeta getTabletMeta(long tabletId) {
                return new TabletMeta(1L, 1L, 1L, 1L, null, false);
            }
            
            @Mock
            public Map<Long, Replica> getReplicas(long tabletId) {
                Map<Long, Replica> replicas = new HashMap<>();
                // Create replica on retained backend (backendId = 1)
                replicas.put(1L, new Replica(1L, 1L, 1L, 1, 0L, 0L, Replica.ReplicaState.NORMAL, -1L, 1L));
                replicas.put(2L, new Replica(2L, 2L, 1L, 1, 0L, 0L, Replica.ReplicaState.NORMAL, -1L, 1L));
                return replicas;
            }
        };
    }
}
