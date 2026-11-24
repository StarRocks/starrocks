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

import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.PartitionStatistics;
import com.starrocks.lake.compaction.PartitionStatistics.CompactionPriority;
import com.starrocks.lake.compaction.PartitionVersion;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.lake.compaction.ScoreSelector;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.PartitionStatisticsPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CompactionMgrImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
        existingPartitions(Set.of());
    }

    private void existingPartitions(Set<Long> dropped) {
        new MockUp<MetaUtils>() {
            @Mock
            public boolean isPhysicalPartitionExist(GlobalStateMgr global, long db, long table, long partition) {
                return !dropped.contains(partition);
            }
        };
    }

    static PartitionStatistics statistics(long partition, CompactionPriority priority) {
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(10, 20, partition));
        statistics.setCompactionVersion(new PartitionVersion(7, 1234));
        statistics.setCurrentVersion(new PartitionVersion(91, 5678));
        statistics.setNextCompactionTime(9876);
        statistics.setCompactionScore(new Quantiles(3.5, 7.25, 11.75));
        statistics.setPriority(priority);
        return statistics;
    }

    static void add(CompactionMgr manager, PartitionStatistics statistics) {
        Map<PartitionIdentifier, PartitionStatistics> map = getField(manager, "partitionStatisticsHashMap");
        map.put(statistics.getPartition(), statistics);
    }

    private CompactionMgr v2(CompactionMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        CompactionMgr result = new CompactionMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private CompactionMgr v3(CompactionMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        CompactionMgr result = new CompactionMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(230, sections.get(0).getType());
        assertEquals(0, sections.get(0).getManagerLength());
        return result;
    }

    @Test
    public void testAllStatisticsFieldsAndPriorities() throws Exception {
        for (CompactionPriority priority : CompactionPriority.values()) {
            PartitionStatistics source = statistics(30, priority);
            PartitionStatistics restored = PartitionStatistics.fromProto(
                    PartitionStatisticsPB.parseFrom(source.toProto().toByteArray()));
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(source.getPartition(), restored.getPartition());
            assertEquals(source.getPartition().hashCode(), restored.getPartition().hashCode());
            assertEquals(source.getCompactionVersion(), restored.getCompactionVersion());
            assertEquals(source.getCurrentVersion(), restored.getCurrentVersion());
            assertNotEquals(restored.getCompactionVersion(), restored.getCurrentVersion());
            assertEquals(84, restored.getDeltaVersions());
            assertEquals(9876, restored.getNextCompactionTime());
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        }
    }

    @Test
    public void testNullableScoresAndVersions() {
        PartitionStatistics source = statistics(30, CompactionPriority.DEFAULT);
        source.setCompactionVersion(null);
        source.setCurrentVersion(null);
        source.setCompactionScore(null);
        PartitionStatistics restored = PartitionStatistics.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(source, restored);
        assertNull(restored.getCompactionVersion());
        assertNull(restored.getCurrentVersion());
        assertNull(restored.getCompactionScore());
    }

    @Test
    public void testRuntimePunishFactorIsNotPersisted() {
        PartitionStatistics source = statistics(30, CompactionPriority.DEFAULT);
        setField(source, "punishFactor", 256);
        PartitionStatistics expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), PartitionStatistics.class);
        PartitionStatistics restored = PartitionStatistics.fromProto(source.toProto());
        assertEquals(0, expected.getPunishFactor());
        assertEquals(expected.getPunishFactor(), restored.getPunishFactor());
        assertEquals(256, source.getPunishFactor());
        for (Quantiles score : List.of(new Quantiles(3.5, 7.25, 11.75), new Quantiles(1, 2, 3),
                new Quantiles(1, 2, 3), new Quantiles(1, 2, 3))) {
            expected.setCompactionScoreAndAdjustPunishFactor(score, true);
            restored.setCompactionScoreAndAdjustPunishFactor(score, true);
            assertEquals(expected.getPunishFactor(), restored.getPunishFactor());
        }
        assertEquals(4, restored.getPunishFactor());
    }

    @Test
    public void testAbsentPriorityHasDefaultBehavior() {
        PartitionStatistics legacy = GsonUtils.GSON.fromJson(
                "{\"partition\":{\"dbId\":1,\"tableId\":2,\"partitionId\":3}}", PartitionStatistics.class);
        PartitionStatistics restored = PartitionStatistics.fromProto(PartitionStatisticsPB.newBuilder()
                .setPartition(legacy.getPartition().toProto()).build());
        assertEquals(legacy.getPriority(), restored.getPriority());
        assertEquals(CompactionPriority.DEFAULT, restored.getPriority());
        GsonEquivalence.assertEquivalent(legacy, restored);
    }

    @Test
    public void testDeletedPartitionsRemovedAtSaveAsInV2() throws Exception {
        existingPartitions(Set.of(31L, 33L));
        CompactionMgr source = new CompactionMgr();
        for (int i = 30; i < 35; i++) {
            add(source, statistics(i, CompactionPriority.DEFAULT));
        }
        CompactionMgr restored = v3(source);
        assertEquals(3, source.getPartitionStatsCount());
        assertEquals(partitions(source), partitions(restored));
        assertNull(restored.getStatistics(new PartitionIdentifier(10, 20, 31)));
        CompactionMgr expected = v2(source);
        for (PartitionIdentifier id : partitions(expected)) {
            GsonEquivalence.assertEquivalent(expected.getStatistics(id), restored.getStatistics(id));
        }

    }

    @Test
    public void testSelectionInputsMatchV2() throws Exception {
        CompactionMgr source = new CompactionMgr();
        for (int i = 1; i <= 6; i++) {
            PartitionStatistics s = statistics(i, CompactionPriority.values()[i % 3]);
            if (i % 3 == 0) {
                s.setNextCompactionTime(Long.MAX_VALUE);
            }
            if (i == 5) {
                s.setCompactionScore(null);
            }
            add(source, s);
        }
        CompactionMgr expected = v2(source);
        CompactionMgr restored = v3(source);
        for (Set<Long> excluded : List.of(Set.<Long>of(), Set.of(2L), Set.of(20L))) {
            ScoreSelector selector = new ScoreSelector();
            Set<PartitionIdentifier> expectedSelection = selector.select(expected.getAllStatistics(), excluded).stream()
                    .map(s -> (PartitionIdentifier) getField(s, "partition")).collect(Collectors.toSet());
            Set<PartitionIdentifier> actualSelection = selector.select(restored.getAllStatistics(), excluded).stream()
                    .map(s -> (PartitionIdentifier) getField(s, "partition")).collect(Collectors.toSet());
            assertEquals(expectedSelection, actualSelection);
        }

    }

    @Test
    public void testEmptySectionReplacesOldMap() throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, new CompactionMgr()::save);
        CompactionMgr restored = new CompactionMgr();
        add(restored, statistics(30, CompactionPriority.MANUAL_COMPACT));
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(0, restored.getPartitionStatsCount());

    }

    @Test
    public void testEachPartitionHasItsOwnEntry() throws Exception {
        CompactionMgr source = new CompactionMgr();
        for (int i = 1; i <= 1024; i++) {
            add(source, statistics(i, CompactionPriority.values()[i % 3]));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        CompactionMgr restored = new CompactionMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1024, sections.get(0).getNumEntries());
        assertEquals(1024, restored.getPartitionStatsCount());
        for (PartitionIdentifier partition : partitions(source)) {
            GsonEquivalence.assertEquivalent(source.getStatistics(partition), restored.getStatistics(partition));
        }

    }
    private static Set<PartitionIdentifier> partitions(CompactionMgr mgr) {
        return mgr.getAllStatistics().stream().map(PartitionStatistics::getPartition).collect(Collectors.toSet());
    }
}
