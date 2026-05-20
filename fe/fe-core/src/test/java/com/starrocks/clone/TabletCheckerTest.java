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
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
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

    private static SystemInfoService systemInfoService;

    private Backend aliveBackend(long id) {
        Backend be = new Backend(id, "127.0.0.1", 9050);
        be.setAlive(true);
        return be;
    }

    private Backend deadBackend(long id) {
        // Defaults: isAlive=false, status non-OK (so isAvailable=false),
        // lastUpdateMs=0 (well past any tolerate window).
        return new Backend(id, "127.0.0.1", 9050);
    }

    /**
     * Defensive aliveness helper used by callers (e.g. ColocateTableBalancer.matchGroups) when
     * tablet_sched_disable_colocate_balance is on and bucketSeq may retain dead BE references.
     */
    @Test
    public void testHasEnoughAliveBackendsInBucketSeq() {
        long be1 = 10001L;
        long be2 = 10002L;
        long be3 = 10003L;
        Set<Long> backendsSet = Sets.newHashSet(be1, be2, be3);

        SystemInfoService infoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(infoService.getBackend(be1)).thenReturn(aliveBackend(be1));
        Mockito.when(infoService.getBackend(be2)).thenReturn(aliveBackend(be2));
        Mockito.when(infoService.getBackend(be3)).thenReturn(aliveBackend(be3));

        Assertions.assertTrue(
                TabletChecker.hasEnoughAliveBackendsInBucketSeq(backendsSet, 3, infoService),
                "all alive — bucket seq satisfies replication");

        // Mark be2 dead; survivors no longer cover replicationNum=3.
        Mockito.when(infoService.getBackend(be2)).thenReturn(deadBackend(be2));
        Assertions.assertFalse(
                TabletChecker.hasEnoughAliveBackendsInBucketSeq(backendsSet, 3, infoService),
                "one dead — should fall below the replication threshold");

        // With a relaxed threshold the survivors are still enough.
        Assertions.assertTrue(
                TabletChecker.hasEnoughAliveBackendsInBucketSeq(backendsSet, 2, infoService),
                "two survivors cover replicationNum=2");
    }
}
