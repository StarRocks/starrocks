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

package com.starrocks.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

public class VacuumStateTest {

    @Test
    public void testInitialStateNotInFlight() {
        VacuumState s = new VacuumState();
        Assertions.assertFalse(s.isInFlight());
        Assertions.assertEquals(0, s.getToDeleteLow());
        Assertions.assertEquals(0, s.getToDeleteHigh());
        Assertions.assertEquals(0, s.getNextProposeStartVersion());
        Assertions.assertEquals(0, s.getPassStartVersion());
        Assertions.assertEquals(0, s.getMinRetainedVersion());
        Assertions.assertTrue(s.getPassStartIndexIds().isEmpty());
    }

    @Test
    public void testFreshRoundCapturesFloorAndIndexIds() {
        VacuumState s = new VacuumState();
        Set<Long> ids = new HashSet<>();
        ids.add(100L);
        ids.add(200L);
        // Fresh round: captureFloor=true, proposes [10, 20), resume cursor 20, retain floor 10.
        s.advance(10, 20, 20, true, 10, ids);
        Assertions.assertEquals(10, s.getToDeleteLow());
        Assertions.assertEquals(20, s.getToDeleteHigh());
        Assertions.assertEquals(20, s.getNextProposeStartVersion());
        Assertions.assertEquals(10, s.getPassStartVersion());
        Assertions.assertEquals(ids, s.getPassStartIndexIds());
        Assertions.assertTrue(s.isInFlight());
    }

    @Test
    public void testResumeRoundKeepsFreshFloorAndIndexIds() {
        VacuumState s = new VacuumState();
        Set<Long> ids = new HashSet<>();
        ids.add(1L);
        s.advance(10, 20, 20, true, 10, ids);           // fresh: floor=10
        // Resume round: captureFloor=false; the floor/index-ids the fresh round established must stick even
        // though this call passes different values for them.
        s.advance(20, 30, 30, false, 999, new HashSet<>());
        Assertions.assertEquals(20, s.getToDeleteLow());
        Assertions.assertEquals(30, s.getToDeleteHigh());
        Assertions.assertEquals(30, s.getNextProposeStartVersion());
        Assertions.assertEquals(10, s.getPassStartVersion());
        Assertions.assertEquals(ids, s.getPassStartIndexIds());
    }

    @Test
    public void testCaptureFloorIgnoredWhenPassStartNonPositive() {
        VacuumState s = new VacuumState();
        // captureFloor=true but passStartVersion=0 (an empty proposal): the floor must not move off 0.
        s.advance(0, 0, 0, true, 0, new HashSet<>());
        Assertions.assertEquals(0, s.getPassStartVersion());
    }

    @Test
    public void testIsInFlightByProposedRange() {
        VacuumState s = new VacuumState();
        // Non-empty range still to commit -> the pass is in flight.
        s.advance(5, 8, 0, false, 0, null);
        Assertions.assertTrue(s.isInFlight());
    }

    @Test
    public void testIsInFlightByResumeCursor() {
        VacuumState s = new VacuumState();
        // Empty range but a non-zero resume cursor (mid-walk) -> in flight.
        s.advance(0, 0, 42, false, 0, null);
        Assertions.assertTrue(s.isInFlight());
    }

    @Test
    public void testEmptyProposalNotInFlight() {
        VacuumState s = new VacuumState();
        // Empty range + zero cursor (chain bottom / nothing to reclaim) -> not in flight.
        s.advance(0, 0, 0, true, 0, null);
        Assertions.assertFalse(s.isInFlight());
    }

    @Test
    public void testResetClearsInFlightButKeepsMinRetained() {
        VacuumState s = new VacuumState();
        s.setMinRetainedVersion(77);
        Set<Long> ids = new HashSet<>();
        ids.add(3L);
        s.advance(10, 20, 20, true, 10, ids);
        Assertions.assertTrue(s.isInFlight());

        s.reset();
        Assertions.assertFalse(s.isInFlight());
        Assertions.assertEquals(0, s.getToDeleteLow());
        Assertions.assertEquals(0, s.getToDeleteHigh());
        Assertions.assertEquals(0, s.getNextProposeStartVersion());
        Assertions.assertEquals(0, s.getPassStartVersion());
        Assertions.assertTrue(s.getPassStartIndexIds().isEmpty());
        // minRetainedVersion is the walk floor carried across passes -- must survive reset().
        Assertions.assertEquals(77, s.getMinRetainedVersion());
    }

    @Test
    public void testPassStartIndexIdsDefensiveCopy() {
        VacuumState s = new VacuumState();
        Set<Long> ids = new HashSet<>();
        ids.add(1L);
        s.advance(10, 20, 20, true, 10, ids);
        // Mutating the source set after advance() must not leak into the captured generation.
        ids.add(999L);
        Assertions.assertFalse(s.getPassStartIndexIds().contains(999L));
        Assertions.assertEquals(1, s.getPassStartIndexIds().size());
    }

    @Test
    public void testCaptureFloorWithNullIndexIdsYieldsEmptySet() {
        VacuumState s = new VacuumState();
        s.advance(10, 20, 20, true, 10, null);
        Assertions.assertNotNull(s.getPassStartIndexIds());
        Assertions.assertTrue(s.getPassStartIndexIds().isEmpty());
    }
}
