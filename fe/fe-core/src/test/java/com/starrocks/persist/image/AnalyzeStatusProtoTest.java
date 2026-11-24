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

import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.AnalyzeStatusPB;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatsConstants;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnalyzeStatusProtoTest {
    static AnalyzeStatus status(boolean external, long id) {
        AnalyzeStatus status = external
                ? new ExternalAnalyzeStatus(id, "hive", "db", "table", "table:uuid", List.of("c"),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE,
                        new HashMap<>(Map.of("sample", "20")), AnalyzeJobProtoTest.TIME)
                : new NativeAnalyzeStatus(id, 10, 20, List.of("c"), StatsConstants.AnalyzeType.SAMPLE,
                        StatsConstants.ScheduleType.ONCE, new HashMap<>(Map.of("sample", "20")), AnalyzeJobProtoTest.TIME);
        status.setStatus(StatsConstants.ScheduleStatus.FAILED);
        status.setEndTime(AnalyzeJobProtoTest.TIME.plusMinutes(3));
        status.setReason("collection failed");
        status.setProgress(37);
        return status;
    }

    @Test
    public void testNativeAllFields() throws Exception {
        AnalyzeStatus source = status(false, 1);
        AnalyzeStatus loaded = AnalyzeStatus.fromProto(AnalyzeStatusPB.parseFrom(
                assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeStatus.class), loaded);
        assertEquals(37, loaded.getProgress());
        assertEquals(StatsConstants.ScheduleStatus.FAILED, loaded.getStatus());
        assertEquals(source.getReason(), loaded.getReason());
    }

    @Test
    public void testExternalAllFieldsAndUuid() throws Exception {
        AnalyzeStatus source = status(true, 2);
        AnalyzeStatus loaded = AnalyzeStatus.fromProto(AnalyzeStatusPB.parseFrom(
                assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeStatus.class), loaded);
        assertEquals("table:uuid", ((ExternalAnalyzeStatus) loaded).getTableUUID());
        assertEquals("hive", loaded.getCatalogName());
    }

    @Test
    public void testAllStatusesAndScheduleTypes() {
        for (boolean external : List.of(false, true)) {
            for (StatsConstants.ScheduleStatus state : StatsConstants.ScheduleStatus.values()) {
                for (StatsConstants.ScheduleType schedule : StatsConstants.ScheduleType.values()) {
                    AnalyzeStatus source = status(external, 3);
                    source.setStatus(state);
                    setField(source, "scheduleType", schedule);
                    GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeStatus.class),
                            AnalyzeStatus.fromProto(source.toProto()));
                }
            }
        }
    }

    @Test
    public void testNullAndEmptyFields() {
        for (boolean external : List.of(false, true)) {
            AnalyzeStatus source = status(external, 4);
            for (String field : List.of("columns", "properties", "type", "scheduleType", "status", "startTime",
                    "endTime", "reason")) {
                setField(source, field, null);
            }
            GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeStatus.class),
                    AnalyzeStatus.fromProto(source.toProto()));
            setField(source, "columns", new ArrayList<>());
            source.setProperties(new HashMap<>());
            AnalyzeStatus loaded = AnalyzeStatus.fromProto(source.toProto());
            assertNotNull(loaded.getColumns());
            assertTrue(loaded.getColumns().isEmpty());
            assertTrue(loaded.getProperties().isEmpty());
        }
    }

    @Test
    public void testSecondPrecisionAndMinTime() {
        AnalyzeStatus source = status(true, 5);
        source.setEndTime(LocalDateTime.MIN);
        AnalyzeStatus loaded = AnalyzeStatus.fromProto(source.toProto());
        assertEquals(AnalyzeJobProtoTest.TIME.withNano(0), loaded.getStartTime());
        assertEquals(LocalDateTime.MIN, loaded.getEndTime());
    }

    @Test
    public void testUnknownFieldsAndIndependentProperties() {
        AnalyzeStatus source = status(false, 6);
        AnalyzeStatusPB pb = source.toProto().toBuilder().setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build();
        AnalyzeStatus loaded = AnalyzeStatus.fromProto(pb);
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeStatus.class), loaded);
        loaded.getProperties().clear();
        assertEquals(1, source.getProperties().size());
    }

    @Test
    public void testInvalidKindAndMissingPayload() {
        AnalyzeStatusPB valid = status(true, 7).toProto();
        assertThrows(SubtypeNotFoundException.class, () -> AnalyzeStatus.fromProto(valid.toBuilder().setKindValue(999).build()));
        for (AnalyzeStatusPB invalid : List.of(AnalyzeStatusPB.getDefaultInstance(),
                valid.toBuilder().clearCore().build(), valid.toBuilder().clearExternal().build(),
                valid.toBuilder().setKind(AnalyzeStatusPB.Kind.NATIVE).build())) {
            assertThrows(IllegalArgumentException.class, () -> AnalyzeStatus.fromProto(invalid));
        }
    }

    @Test
    public void testInvalidStatusAndTime() {
        AnalyzeStatusPB valid = status(false, 8).toProto();
        assertThrows(IllegalArgumentException.class, () -> AnalyzeStatus.fromProto(valid.toBuilder()
                .setCore(valid.getCore().toBuilder().setStatusValue(999)).build()));
        assertThrows(IllegalArgumentException.class, () -> AnalyzeStatus.fromProto(valid.toBuilder()
                .setCore(valid.getCore().toBuilder().setEndTime(Long.MIN_VALUE)).build()));
    }
}
