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

package com.starrocks.clone;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletHealthStatus;
import com.starrocks.catalog.Replica;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TabletCheckerTest {

    @BeforeAll
    public static void beforeClass() {
        systemInfoService = Mockito.mock(SystemInfoService.class);
    }

    private Backend mockBackend(String locationLabel) {
        Backend mockBackend = Mockito.mock(Backend.class);
        Map<String, String> location = new HashMap<>();
        if (locationLabel.isEmpty()) {
            mockBackend.setLocation(null);
            Mockito.when(mockBackend.getLocation()).thenReturn(null);
        } else {
            String[] locKV = locationLabel.split(":");
            location.put(locKV[0].trim(), locKV[1].trim());
            Mockito.when(mockBackend.getLocation()).thenReturn(location);
        }

        return mockBackend;
    }

    private SystemInfoService mockSystemInfoService(String... locationLabel) {
        SystemInfoService mockSystemInfoService = Mockito.mock(SystemInfoService.class);
        List<Backend> backends = new ArrayList<>();
        for (String location : locationLabel) {
            backends.add(mockBackend(location));
        }
        Mockito.when(mockSystemInfoService.getBackends()).thenReturn(backends);

        return mockSystemInfoService;
    }

    @Test
    public void testCollectBackendWithLocation() {

        systemInfoService = mockSystemInfoService("", "", "");
        Multimap<String, String> backendLocations = TabletChecker.collectDistinctBackendLocations(systemInfoService);
        Assertions.assertEquals(0, backendLocations.size());

        systemInfoService = mockSystemInfoService(
                "rack:rack1",
                "rack:rack1",
                "rack:rack1",
                "");
        backendLocations = TabletChecker.collectDistinctBackendLocations(systemInfoService);
        Assertions.assertEquals(1, backendLocations.size());

        systemInfoService = mockSystemInfoService(
                "rack:rack1",
                "rack:rack1",
                "rack:rack1",
                "rack:rack2",
                "rack:rack3",
                "region:region1",
                "region:region2");
        backendLocations = TabletChecker.collectDistinctBackendLocations(systemInfoService);
        Assertions.assertEquals(5, backendLocations.size());
    }

    @Test
    public void testShouldEnsureReplicaHA() {
        systemInfoService = mockSystemInfoService(
                "rack:rack1",
                "rack:rack1",
                "rack:rack1",
                "rack:rack2",
                "rack:rack2",
                "rack:rack3",
                "region:region1",
                "region:region2");

        Multimap<String, String> requiredLocation = ArrayListMultimap.create();
        Assertions.assertFalse(TabletChecker.shouldEnsureReplicaHA(1, null, systemInfoService));
        Assertions.assertFalse(TabletChecker.shouldEnsureReplicaHA(2, requiredLocation, systemInfoService));

        requiredLocation.put("rack", "rack2");
        Assertions.assertTrue(TabletChecker.shouldEnsureReplicaHA(1, requiredLocation, systemInfoService));
        Assertions.assertFalse(TabletChecker.shouldEnsureReplicaHA(2, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("rack", "rack2");
        requiredLocation.put("rack", "rack3");
        Assertions.assertTrue(TabletChecker.shouldEnsureReplicaHA(2, requiredLocation, systemInfoService));
        Assertions.assertFalse(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("rack", "*");
        Assertions.assertTrue(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("rack", "rack3");
        requiredLocation.put("region", "*");
        Assertions.assertTrue(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("*", "");
        Assertions.assertTrue(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();
    }

    @Test
    public void testIsReplicaStateAbnormal() throws Exception {
        Method method = TabletChecker.class.getDeclaredMethod(
                "isReplicaStateAbnormal", Replica.class, Backend.class, Set.class);
        method.setAccessible(true);

        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("host1");

        // Normal replica is not abnormal
        Replica normalReplica = new Replica(1L, 10001L, 0, Replica.ReplicaState.NORMAL);
        Set<String> hosts = new HashSet<>();
        Assertions.assertFalse((Boolean) method.invoke(null, normalReplica, backend, hosts));

        // Error-state replica is abnormal
        Replica errorReplica = new Replica(2L, 10002L, 0, Replica.ReplicaState.NORMAL);
        errorReplica.setIsErrorState(true);
        hosts = new HashSet<>();
        Assertions.assertTrue((Boolean) method.invoke(null, errorReplica, backend, hosts));

        // Bad replica is abnormal
        Replica badReplica = new Replica(3L, 10003L, 0, Replica.ReplicaState.NORMAL);
        badReplica.setBad(true);
        hosts = new HashSet<>();
        Assertions.assertTrue((Boolean) method.invoke(null, badReplica, backend, hosts));

        // CLONE state replica is abnormal
        Replica cloneReplica = new Replica(4L, 10004L, 0, Replica.ReplicaState.CLONE);
        hosts = new HashSet<>();
        Assertions.assertTrue((Boolean) method.invoke(null, cloneReplica, backend, hosts));

        // DECOMMISSION state replica is abnormal
        Replica decommissionReplica = new Replica(5L, 10005L, 0, Replica.ReplicaState.DECOMMISSION);
        hosts = new HashSet<>();
        Assertions.assertTrue((Boolean) method.invoke(null, decommissionReplica, backend, hosts));
    }

    @Test
    public void testColocateTabletHealthStatusDetectsErrorStateReplica() {
        long visibleVersion = 10L;
        // Replica on backend 20001 is in error state; the error-state check must run before
        // the disk-decommission check so we do not depend on SystemInfoService being populated.
        Replica errorReplica = new Replica(10001L, 20001L, Replica.ReplicaState.NORMAL, visibleVersion, -1);
        errorReplica.setIsErrorState(true);
        Replica healthyReplica1 = new Replica(10002L, 20002L, Replica.ReplicaState.NORMAL, visibleVersion, -1);
        Replica healthyReplica2 = new Replica(10003L, 20003L, Replica.ReplicaState.NORMAL, visibleVersion, -1);

        LocalTablet tablet = new LocalTablet(10004L,
                new ArrayList<>(Arrays.asList(errorReplica, healthyReplica1, healthyReplica2)));
        Set<Long> backendsSet = Sets.newHashSet(20001L, 20002L, 20003L);

        Assertions.assertEquals(TabletHealthStatus.COLOCATE_REDUNDANT,
                TabletChecker.getColocateTabletHealthStatus(tablet, visibleVersion, 3, backendsSet));
    }

    private static SystemInfoService systemInfoService;
}
