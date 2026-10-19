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

package com.starrocks.transaction;

import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.task.PublishVersionTask.PublishedRowCount;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

public class PublishedIndexRowCountTest {
    private static final long VERSION = 11L;
    private static final long BE1 = 1L;
    private static final long BE2 = 2L;

    private MaterializedIndex index;
    private Replica r11;
    private Replica r12;
    private Replica r21;
    private Replica r22;
    // backend id -> tablet id -> what that backend reported on publish
    private Map<Long, Map<Long, PublishedRowCount>> reported;

    private static Replica replica(long id, long backendId) {
        return new Replica(id, backendId, VERSION, 0, 0L, 0L, ReplicaState.NORMAL, -1, VERSION);
    }

    private void report(long backendId, long tabletId, long version, long rowCount) {
        reported.computeIfAbsent(backendId, k -> new HashMap<>())
                .put(tabletId, new PublishedRowCount(version, rowCount));
    }

    private OptionalLong sum(Set<Long> errorReplicaIds) {
        return DatabaseTransactionMgr.publishedIndexRowCount(index, VERSION, errorReplicaIds, reported);
    }

    @BeforeEach
    public void setUp() {
        index = new MaterializedIndex(1L);
        LocalTablet t1 = new LocalTablet(101L);
        LocalTablet t2 = new LocalTablet(102L);
        r11 = replica(1L, BE1);
        r12 = replica(2L, BE2);
        r21 = replica(3L, BE1);
        r22 = replica(4L, BE2);
        t1.addReplica(r11, false);
        t1.addReplica(r12, false);
        t2.addReplica(r21, false);
        t2.addReplica(r22, false);
        index.addTablet(t1, null, false);
        index.addTablet(t2, null, false);
        reported = new HashMap<>();
    }

    @Test
    public void testSumsCountsReportedForTheVersion() {
        report(BE1, 101L, VERSION, 100L);
        report(BE1, 102L, VERSION, 50L);
        Assertions.assertEquals(OptionalLong.of(150L), sum(Set.of()));
    }

    @Test
    public void testEmptyUnlessEveryTabletReportsTheExactVersion() {
        report(BE1, 101L, VERSION, 100L);
        // the BE was already a version ahead, or behind: not a count for VERSION
        report(BE1, 102L, VERSION + 1, 60L);
        report(BE2, 102L, VERSION - 1, 40L);
        Assertions.assertEquals(OptionalLong.empty(), sum(Set.of()));
    }

    @Test
    public void testIgnoresReplicaCountsSetSinceThePublish() {
        // e.g. a tablet stat round rewrote the replicas' counts between the publish and the finish
        r11.updateStat(0L, 7L, 1L, VERSION);
        r21.updateStat(0L, 7L, 1L, VERSION);
        report(BE1, 101L, VERSION, 100L);
        report(BE1, 102L, VERSION, 50L);
        Assertions.assertEquals(OptionalLong.of(150L), sum(Set.of()));
    }

    @Test
    public void testEmptyIfReplicasDisagree() {
        report(BE1, 101L, VERSION, 100L);
        report(BE2, 101L, VERSION, 100L);
        report(BE1, 102L, VERSION, 50L);
        Assertions.assertEquals(OptionalLong.of(150L), sum(Set.of()));

        // copies of the same version must hold the same rows; one that doesn't is not trusted for the index
        report(BE2, 102L, VERSION, 70L);
        Assertions.assertEquals(OptionalLong.empty(), sum(Set.of()));
    }

    @Test
    public void testSkipsUnhealthyReplicas() {
        report(BE1, 101L, VERSION, 100L);
        report(BE1, 102L, VERSION, 50L);
        Assertions.assertEquals(OptionalLong.empty(), sum(Set.of(r21.getId())));

        report(BE2, 102L, VERSION, 55L);
        r22.setBad(true);
        Assertions.assertEquals(OptionalLong.empty(), sum(Set.of(r21.getId())));

        r22.setBad(false);
        Assertions.assertEquals(OptionalLong.of(155L), sum(Set.of(r21.getId())));

        r22.updateLastFailedVersion(VERSION + 1);
        Assertions.assertEquals(OptionalLong.empty(), sum(Set.of(r21.getId())));
    }
}
