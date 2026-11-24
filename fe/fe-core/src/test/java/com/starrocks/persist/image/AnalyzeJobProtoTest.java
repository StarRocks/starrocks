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
import com.starrocks.persist.proto.AnalyzeJobPB;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnalyzeJobProtoTest {
    static final LocalDateTime TIME = LocalDateTime.of(2026, 9, 29, 1, 2, 3, 987654321);

    static AnalyzeJob job(boolean external, long id) {
        List<String> columns = new ArrayList<>(List.of("a", "b"));
        List<Type> types = List.of(IntegerType.BIGINT, new ArrayType(IntegerType.INT));
        AnalyzeJob job = external
                ? new ExternalAnalyzeJob("hive", "db", "table", columns, types, StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.SCHEDULE, new HashMap<>(Map.of("sample", "10")),
                        StatsConstants.ScheduleStatus.RUNNING, TIME)
                : new NativeAnalyzeJob(10, 20, columns, types, StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.SCHEDULE, new HashMap<>(Map.of("sample", "10")),
                        StatsConstants.ScheduleStatus.RUNNING, TIME);
        job.setId(id);
        job.setReason("last error");
        return job;
    }

    @Test
    public void testNativeAllFields() throws Exception {
        AnalyzeJob source = job(false, 1);
        AnalyzeJob loaded = AnalyzeJob.fromProto(AnalyzeJobPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeJob.class), loaded);
        assertTrue(loaded.isNative());
        assertEquals(1, loaded.getId());
        assertEquals(TIME.withNano(0), loaded.getWorkTime());
        assertEquals(source.getColumnTypes(), loaded.getColumnTypes());
    }

    @Test
    public void testExternalAllFields() throws Exception {
        AnalyzeJob source = job(true, 2);
        AnalyzeJob loaded = AnalyzeJob.fromProto(AnalyzeJobPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeJob.class), loaded);
        assertFalse(loaded.isNative());
        assertEquals("hive", loaded.getCatalogName());
        assertEquals("db", loaded.getDbName());
        assertEquals("table", loaded.getTableName());
    }

    @Test
    public void testEveryScheduleAndAnalyzeType() {
        for (boolean external : List.of(false, true)) {
            for (StatsConstants.AnalyzeType type : StatsConstants.AnalyzeType.values()) {
                for (StatsConstants.ScheduleType schedule : StatsConstants.ScheduleType.values()) {
                    for (StatsConstants.ScheduleStatus status : StatsConstants.ScheduleStatus.values()) {
                        AnalyzeJob source = job(external, -1);
                        setField(source, "type", type);
                        setField(source, "scheduleType", schedule);
                        source.setStatus(status);
                        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeJob.class),
                                AnalyzeJob.fromProto(source.toProto()));
                    }
                }
            }
        }
    }

    @Test
    public void testNullAndEmptyFields() {
        for (boolean external : List.of(false, true)) {
            AnalyzeJob source = job(external, 5);
            for (String field : List.of("columns", "columnTypes", "properties", "type", "scheduleType", "status",
                    "workTime", "reason")) {
                setField(source, field, null);
            }
            AnalyzeJob loaded = AnalyzeJob.fromProto(source.toProto());
            GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeJob.class), loaded);
            assertNull(loaded.getColumns());
            setField(source, "columns", new ArrayList<>());
            setField(source, "columnTypes", new ArrayList<>());
            setField(source, "properties", new HashMap<>());
            loaded = AnalyzeJob.fromProto(source.toProto());
            assertNotNull(loaded.getColumns());
            assertTrue(loaded.getColumns().isEmpty());
            GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeJob.class), loaded);
        }
    }

    @Test
    public void testMinAndNegativeTime() {
        for (LocalDateTime time : List.of(LocalDateTime.MIN, LocalDateTime.of(1960, 1, 2, 3, 4))) {
            AnalyzeJob source = job(false, 6);
            source.setWorkTime(time);
            assertEquals(time, AnalyzeJob.fromProto(source.toProto()).getWorkTime());
        }
    }

    @Test
    public void testNullPropertyValuesMatchV2() {
        AnalyzeJob source = job(true, 7);
        source.getProperties().put("ignored", null);
        AnalyzeJob loaded = AnalyzeJob.fromProto(source.toProto());
        assertFalse(loaded.getProperties().containsKey("ignored"));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, AnalyzeJob.class), loaded);
    }

    @Test
    public void testConvertedCollectionsAreIndependent() {
        AnalyzeJob source = job(false, 8);
        AnalyzeJob loaded = AnalyzeJob.fromProto(source.toProto());
        loaded.getColumns().clear();
        loaded.getProperties().put("new", "value");
        assertEquals(2, source.getColumns().size());
        assertFalse(source.getProperties().containsKey("new"));
    }

    @Test
    public void testUnknownFields() {
        AnalyzeJob source = job(true, 9);
        AnalyzeJobPB pb = source.toProto().toBuilder().setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build();
        GsonEquivalence.assertEquivalent(AnalyzeJob.fromProto(source.toProto()), AnalyzeJob.fromProto(pb));
    }

    @Test
    public void testInvalidKindAndMissingPayload() {
        AnalyzeJobPB valid = job(false, 10).toProto();
        assertThrows(SubtypeNotFoundException.class, () -> AnalyzeJob.fromProto(valid.toBuilder().setKindValue(999).build()));
        for (AnalyzeJobPB invalid : List.of(AnalyzeJobPB.getDefaultInstance(),
                valid.toBuilder().clearCore().build(), valid.toBuilder().clearNative().build(),
                valid.toBuilder().setKind(AnalyzeJobPB.Kind.EXTERNAL).build())) {
            assertThrows(IllegalArgumentException.class, () -> AnalyzeJob.fromProto(invalid));
        }
    }

    @Test
    public void testInvalidEnumAndTime() {
        AnalyzeJobPB valid = job(false, 11).toProto();
        assertThrows(IllegalArgumentException.class, () -> AnalyzeJob.fromProto(valid.toBuilder()
                .setCore(valid.getCore().toBuilder().setTypeValue(999)).build()));
        assertThrows(IllegalArgumentException.class, () -> AnalyzeJob.fromProto(valid.toBuilder()
                .setCore(valid.getCore().toBuilder().setWorkTime(Long.MAX_VALUE)).build()));
    }
}
