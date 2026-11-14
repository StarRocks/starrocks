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

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableMap;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class LocationRestrictedWorkerProviderTest {

    private static ImmutableMap<Long, ComputeNode> nodes(int n) {
        Map<Long, ComputeNode> map = new HashMap<>();
        for (long i = 0; i < n; i++) {
            ComputeNode node = new ComputeNode(i, "192.168.1." + i, 9030);
            node.setAlive(true);
            map.put(i, node);
        }
        return ImmutableMap.copyOf(map);
    }

    private static DefaultWorkerProvider baseProvider(ImmutableMap<Long, ComputeNode> map) {
        return new DefaultWorkerProvider(map, ImmutableMap.of(), map, ImmutableMap.of(), false,
                WarehouseManager.DEFAULT_RESOURCE);
    }

    @Test
    void testMaybeWrap() {
        ImmutableMap<Long, ComputeNode> map = nodes(2);
        DefaultWorkerProvider base = baseProvider(map);
        Assertions.assertSame(base, LocationRestrictedWorkerProvider.maybeWrap(base, null));
        Assertions.assertSame(base,
                LocationRestrictedWorkerProvider.maybeWrap(base,
                        ConnectorLocationWorkerRestrictor.Restriction.unrestricted()));

        WorkerProvider wrapped = LocationRestrictedWorkerProvider.maybeWrap(base,
                ConnectorLocationWorkerRestrictor.Restriction.of(Set.of(0L), Set.of(0L, 1L)));
        Assertions.assertTrue(wrapped instanceof LocationRestrictedWorkerProvider);
        Assertions.assertEquals(2, wrapped.getAllWorkers().size());
        Assertions.assertEquals(1, wrapped.getPreferredConnectorWorkers().size());
    }

    @Test
    void testSelectAndFilterApis() throws Exception {
        ImmutableMap<Long, ComputeNode> map = nodes(3);
        DefaultWorkerProvider base = baseProvider(map);
        LocationRestrictedWorkerProvider restricted = new LocationRestrictedWorkerProvider(base, Set.of(0L, 1L),
                Set.of(0L));

        Assertions.assertFalse(restricted.isPreferComputeNode());
        Assertions.assertEquals(WarehouseManager.DEFAULT_RESOURCE, restricted.getComputeResource());
        Assertions.assertTrue(restricted.isDataNodeAvailable(0L));
        Assertions.assertFalse(restricted.isDataNodeAvailable(2L));
        Assertions.assertNull(restricted.getWorkerById(2L));
        Assertions.assertNotNull(restricted.getWorkerById(0L));

        restricted.selectWorker(0L);
        Assertions.assertTrue(restricted.isWorkerSelected(0L));
        Assertions.assertTrue(restricted.getSelectedWorkerIds().contains(0L));

        Assertions.assertThrows(NonRecoverableException.class, () -> restricted.selectWorker(2L));
        Assertions.assertThrows(NonRecoverableException.class, () -> restricted.selectWorker(99L));

        List<Long> available = restricted.getAllAvailableNodes();
        Assertions.assertEquals(Set.of(0L, 1L), Set.copyOf(available));

        // No CNs in this provider; filtered list stays empty.
        Assertions.assertTrue(restricted.selectAllComputeNodes().isEmpty());
    }

    @Test
    void testPreferredSoftFallbackWhenPreferredInvisible() {
        ImmutableMap<Long, ComputeNode> map = nodes(2);
        DefaultWorkerProvider base = baseProvider(map);
        // Preferred id 9 is not visible; soft-fallback returns allowedWorkers.
        LocationRestrictedWorkerProvider restricted = new LocationRestrictedWorkerProvider(base, Set.of(0L, 1L),
                Set.of(9L));
        Collection<ComputeNode> preferred = restricted.getPreferredConnectorWorkers();
        Assertions.assertEquals(2, preferred.size());
        Assertions.assertEquals(Set.of(0L, 1L),
                preferred.stream().map(ComputeNode::getId).collect(Collectors.toSet()));
    }

    @Test
    void testBackupWorkerClippedToAllowed() {
        ImmutableMap<Long, ComputeNode> map = nodes(3);
        DefaultWorkerProvider base = baseProvider(map);
        LocationRestrictedWorkerProvider restricted = new LocationRestrictedWorkerProvider(base, Set.of(0L, 1L),
                Set.of(0L));

        Assertions.assertEquals(base.allowUsingBackupNode(), restricted.allowUsingBackupNode());
        // Shared-nothing DefaultWorkerProvider returns -1 for backup; still clipped by
        // allowed set.
        Assertions.assertEquals(-1, restricted.selectBackupWorker(0L));
        Assertions.assertEquals(-1, restricted.selectBackupWorker(0L, 42L));
    }

    @Test
    void testReportDelegates() {
        ImmutableMap<Long, ComputeNode> map = nodes(1);
        DefaultWorkerProvider base = baseProvider(map);
        LocationRestrictedWorkerProvider restricted = new LocationRestrictedWorkerProvider(base, Set.of(0L),
                Set.of(0L));
        Assertions.assertThrows(NonRecoverableException.class, restricted::reportDataNodeNotFoundException);
        Assertions.assertThrows(NonRecoverableException.class,
                () -> restricted.reportWorkerNotFoundException("prefix:"));
    }

    @Test
    void testSelectNextWorkerEmptyAllowed() {
        ImmutableMap<Long, ComputeNode> map = nodes(1);
        DefaultWorkerProvider base = baseProvider(map);
        // Allowed id missing from delegate → empty allowedWorkers.
        LocationRestrictedWorkerProvider restricted = new LocationRestrictedWorkerProvider(base, Set.of(99L),
                Set.of(99L));
        Assertions.assertTrue(restricted.getAllWorkers().isEmpty());
        Assertions.assertThrows(NonRecoverableException.class, restricted::selectNextWorker);
    }

    @Test
    void testSupplementAllowedFromGetAllWorkers() {
        ComputeNode onlyInAllWorkers = new ComputeNode(7L, "192.168.1.7", 9030);
        onlyInAllWorkers.setAlive(true);
        WorkerProvider delegate = new WorkerProvider() {
            @Override
            public long selectNextWorker() throws NonRecoverableException {
                return 7L;
            }

            @Override
            public void selectWorker(long workerId) throws NonRecoverableException {
                // Intentionally empty: explicit select is not exercised by this stub.
            }

            @Override
            public List<Long> selectAllComputeNodes() {
                return List.of(7L);
            }

            @Override
            public boolean isPreferComputeNode() {
                return true;
            }

            @Override
            public Collection<ComputeNode> getAllWorkers() {
                return List.of(onlyInAllWorkers);
            }

            @Override
            public ComputeNode getWorkerById(long workerId) {
                // Miss on id lookup; only visible via getAllWorkers.
                return null;
            }

            @Override
            public boolean isDataNodeAvailable(long dataNodeId) {
                return dataNodeId == 7L;
            }

            @Override
            public void reportDataNodeNotFoundException() throws NonRecoverableException {
                // Intentionally empty: reporting is not exercised by this stub.
            }

            @Override
            public void reportWorkerNotFoundException(String errorMessagePrefix) throws NonRecoverableException {
                // Intentionally empty: reporting is not exercised by this stub.
            }

            @Override
            public boolean isWorkerSelected(long workerId) {
                return false;
            }

            @Override
            public List<Long> getSelectedWorkerIds() {
                return List.of();
            }

            @Override
            public List<Long> getAllAvailableNodes() {
                return List.of(7L);
            }

            @Override
            public void selectWorkerUnchecked(long workerId) {
                // Intentionally empty: this stub only exercises getWorkerById/getAllWorkers
                // wiring; selection tracking is not under test here.
            }

            @Override
            public boolean allowUsingBackupNode() {
                return true;
            }

            @Override
            public long selectBackupWorker(long workerId) {
                return workerId == 7L ? 7L : 8L;
            }

            @Override
            public long selectBackupWorker(long workerId, long affinityKey) {
                return selectBackupWorker(workerId);
            }

            @Override
            public ComputeResource getComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        LocationRestrictedWorkerProvider restricted = new LocationRestrictedWorkerProvider(delegate, Set.of(7L),
                Set.of(7L));
        Assertions.assertEquals(1, restricted.getAllWorkers().size());
        Assertions.assertEquals(7L, restricted.getAllWorkers().iterator().next().getId());
        // Wrapper never prefers CNs, so selectAllComputeNodes must not register anyone.
        Assertions.assertTrue(restricted.selectAllComputeNodes().isEmpty());
        Assertions.assertEquals(7L, restricted.selectBackupWorker(7L));
        Assertions.assertEquals(-1, restricted.selectBackupWorker(8L));
        Assertions.assertEquals(7L, restricted.selectBackupWorker(7L, 1L));
        Assertions.assertEquals(-1, restricted.selectBackupWorker(8L, 1L));
    }

    @Test
    void testSelectAllComputeNodesDoesNotRegisterWhenPreferDisabled() {
        ImmutableMap<Long, ComputeNode> map = nodes(3);
        // prefer_compute_node=true on the delegate + ALL_NODES-style pool. RemoteFragment still
        // calls selectAllComputeNodes even though the wrapper reports prefer=false and discards
        // the result — registering allowed replica BEs would still cancel on unused-BE death.
        DefaultWorkerProvider base = new DefaultWorkerProvider(
                map, map, map, map, true, WarehouseManager.DEFAULT_RESOURCE);
        LocationRestrictedWorkerProvider restricted =
                new LocationRestrictedWorkerProvider(base, Set.of(0L, 1L), Set.of(0L));

        Assertions.assertTrue(restricted.selectAllComputeNodes().isEmpty());
        Assertions.assertFalse(restricted.isWorkerSelected(0L));
        Assertions.assertFalse(restricted.isWorkerSelected(1L));
        Assertions.assertFalse(restricted.isWorkerSelected(2L));
        Assertions.assertFalse(restricted.isPreferComputeNode());
        Assertions.assertTrue(base.isPreferComputeNode());
    }

    @Test
    void testGetAllAvailableNodesUsesAllowedWorkersWhenPreferComputeNode() {
        // BEs 0,1 and a separate CN 10. prefer_compute_node=true → delegate available list is
        // CN-only; filtering that against labelled BEs would yield []. Adaptive scheduling needs
        // the wrapper's allowedWorkers (L∪N) instead.
        ImmutableMap<Long, ComputeNode> backends = nodes(2);
        ComputeNode cn = new ComputeNode(10L, "127.0.0.1", 9060);
        cn.setAlive(true);
        ImmutableMap<Long, ComputeNode> computeNodes = ImmutableMap.of(10L, cn);
        DefaultWorkerProvider base = new DefaultWorkerProvider(
                backends, computeNodes, backends, computeNodes, true, WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(List.of(10L), base.getAllAvailableNodes());

        LocationRestrictedWorkerProvider restricted =
                new LocationRestrictedWorkerProvider(base, Set.of(0L, 1L), Set.of(0L));
        Assertions.assertEquals(Set.of(0L, 1L), Set.copyOf(restricted.getAllAvailableNodes()));
        Assertions.assertFalse(restricted.isPreferComputeNode());
    }
}
