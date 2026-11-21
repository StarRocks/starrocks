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

package com.starrocks.persist.image;

import com.google.gson.JsonObject;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.VacuumState;
import com.starrocks.lake.vacuum.AutovacuumDaemon;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.PhysicalPartitionPB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VacuumStateImageV3Test {
    private PhysicalPartition source() {
        PhysicalPartition partition = new PhysicalPartition(101, 100,
                new MaterializedIndex(7, 7, MaterializedIndex.IndexState.NORMAL,
                        PhysicalPartition.INVALID_SHARD_GROUP_ID));
        partition.updateVisibleVersion(100, 1);
        partition.setNextVersion(101);
        partition.getVacuumState().advance(10, 20, 30, true, 50, Set.of(7L, 8L));
        partition.getVacuumState().setMinRetainedVersion(8);
        partition.setLastSuccVacuumVersion(12);
        return partition;
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 3, 12})
    void retainedFloorRecoversWatermarkLikeV2(long watermark) throws Exception {
        PhysicalPartition source = source();
        source.setLastSuccVacuumVersion(watermark);
        PhysicalPartition v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), PhysicalPartition.class);
        PhysicalPartition v3 = PhysicalPartition.fromProto(PhysicalPartitionPB.parseFrom(source.toProto().toByteArray()));
        assertEquals(Math.max(8, watermark), v2.getLastSuccVacuumVersion());
        assertEquals(v2.getLastSuccVacuumVersion(), v3.getLastSuccVacuumVersion());
        assertTrue(new AutovacuumDaemon().shouldVacuum(v3));
        assertEquals(Set.of(7L, 8L), v3.getVacuumState().getPassStartIndexIds());
    }

    @Test
    void oldPayloadWithoutWatermarkUsesPersistedFloor() {
        PhysicalPartition source = source();
        JsonObject json = GsonUtils.GSON.toJsonTree(source).getAsJsonObject();
        json.remove("lastSuccVacuumVersion");
        PhysicalPartition v2 = GsonUtils.GSON.fromJson(json, PhysicalPartition.class);
        PhysicalPartition v3 = PhysicalPartition.fromProto(source.toProto().toBuilder().clearLastSuccVacuumVersion().build());
        assertEquals(8, v2.getLastSuccVacuumVersion());
        assertEquals(8, v3.getLastSuccVacuumVersion());
        assertTrue(v3.getVacuumState().isInFlight());
        assertEquals(30, v3.getVacuumState().getNextProposeStartVersion());
    }

    @Test
    void oldPayloadWithoutVacuumFieldsKeepsInitialState() {
        PhysicalPartition source = source();
        JsonObject json = GsonUtils.GSON.toJsonTree(source).getAsJsonObject();
        json.remove("lastSuccVacuumVersion");
        json.remove("vacuumState");
        PhysicalPartition v2 = GsonUtils.GSON.fromJson(json, PhysicalPartition.class);
        PhysicalPartition v3 = PhysicalPartition.fromProto(source.toProto().toBuilder()
                .clearVacuumState().clearLastSuccVacuumVersion().build());
        assertEquals(0, v2.getLastSuccVacuumVersion());
        assertEquals(0, v3.getLastSuccVacuumVersion());
        GsonEquivalence.assertEquivalent(v2.getVacuumState(), v3.getVacuumState());
        assertFalse(v3.getVacuumState().isInFlight());
        assertTrue(v3.getVacuumState().getPassStartIndexIds().isEmpty());
    }

    @Test
    void completedPassRetainsFloorAndDoesNotRestartVacuum() throws Exception {
        PhysicalPartition source = source();
        source.getVacuumState().reset();
        PhysicalPartition v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), PhysicalPartition.class);
        PhysicalPartition v3 = PhysicalPartition.fromProto(PhysicalPartitionPB.parseFrom(source.toProto().toByteArray()));
        for (PhysicalPartition loaded : java.util.List.of(v2, v3)) {
            VacuumState state = loaded.getVacuumState();
            assertEquals(8, state.getMinRetainedVersion());
            assertEquals(12, loaded.getLastSuccVacuumVersion());
            assertEquals(0, state.getToDeleteLow());
            assertEquals(0, state.getToDeleteHigh());
            assertEquals(0, state.getNextProposeStartVersion());
            assertEquals(0, state.getPassStartVersion());
            assertTrue(state.getPassStartIndexIds().isEmpty());
            assertFalse(new AutovacuumDaemon().shouldVacuum(loaded));
        }
    }
}
