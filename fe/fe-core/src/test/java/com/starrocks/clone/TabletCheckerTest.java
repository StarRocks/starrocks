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
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TabletCheckerTest {

    @BeforeClass
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
        Assert.assertEquals(0, backendLocations.size());

        systemInfoService = mockSystemInfoService(
                "rack:rack1",
                "rack:rack1",
                "rack:rack1",
                "");
        backendLocations = TabletChecker.collectDistinctBackendLocations(systemInfoService);
        Assert.assertEquals(1, backendLocations.size());

        systemInfoService = mockSystemInfoService(
                "rack:rack1",
                "rack:rack1",
                "rack:rack1",
                "rack:rack2",
                "rack:rack3",
                "region:region1",
                "region:region2");
        backendLocations = TabletChecker.collectDistinctBackendLocations(systemInfoService);
        Assert.assertEquals(5, backendLocations.size());
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
        Assert.assertFalse(TabletChecker.shouldEnsureReplicaHA(1, null, systemInfoService));
        Assert.assertFalse(TabletChecker.shouldEnsureReplicaHA(2, requiredLocation, systemInfoService));

        requiredLocation.put("rack", "rack2");
        Assert.assertTrue(TabletChecker.shouldEnsureReplicaHA(1, requiredLocation, systemInfoService));
        Assert.assertFalse(TabletChecker.shouldEnsureReplicaHA(2, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("rack", "rack2");
        requiredLocation.put("rack", "rack3");
        Assert.assertTrue(TabletChecker.shouldEnsureReplicaHA(2, requiredLocation, systemInfoService));
        Assert.assertFalse(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("rack", "*");
        Assert.assertTrue(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("rack", "rack3");
        requiredLocation.put("region", "*");
        Assert.assertTrue(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();

        requiredLocation.put("*", "");
        Assert.assertTrue(TabletChecker.shouldEnsureReplicaHA(3, requiredLocation, systemInfoService));
        requiredLocation.clear();
    }

    private static SystemInfoService systemInfoService;
}
