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

import com.google.protobuf.Message;
import com.starrocks.persist.proto.AnalyzeJobPB;
import com.starrocks.persist.proto.AnalyzeMgrPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnalyzeMgrImageV3Test {
    @TempDir
    Path dir;

    static AnalyzeMgr populated() {
        AnalyzeMgr source = new AnalyzeMgr();
        source.replayAddAnalyzeJob(AnalyzeJobProtoTest.job(false, 1));
        source.replayAddAnalyzeJob(AnalyzeJobProtoTest.job(true, 2));
        source.replayAddAnalyzeStatus(AnalyzeStatusProtoTest.status(false, 3));
        source.replayAddAnalyzeStatus(AnalyzeStatusProtoTest.status(true, 4));
        source.replayAddBasicStatsMeta(StatsMetaProtoTest.basic(20));
        source.replayAddHistogramStatsMeta(StatsMetaProtoTest.histogram());
        source.replayAddExternalBasicStatsMeta(StatsMetaProtoTest.externalBasic());
        source.replayAddExternalHistogramStatsMeta(StatsMetaProtoTest.externalHistogram());
        source.replayAddMultiColumnStatsMeta(StatsMetaProtoTest.multiColumn());
        return source;
    }

    static void equivalent(AnalyzeMgr expected, AnalyzeMgr actual) {
        StatisticsSecurityTestSupport.assertMaps(expected, actual, "analyzeJobMap", "analyzeStatusMap",
                "basicStatsMetaMap", "histogramStatsMetaMap", "externalBasicStatsMetaMap",
                "externalHistogramStatsMetaMap", "multiColumnStatsMetaMap");
    }

    @Test
    public void testSevenCollectionsMatchV2() throws Exception {
        GlobalStateMgr global = LoadImageTestSupport.global();
        AnalyzeMgr source = populated();
        AnalyzeMgr v2 = new AnalyzeMgr();
        StatisticsSecurityTestSupport.v2(source::save, v2::load);
        AnalyzeMgr loaded = new AnalyzeMgr();
        Path file = LoadImageTestSupport.save(dir, source::save);
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
        equivalent(v2, loaded);
        assertEquals(List.of(2L, 2L, 1L, 1L, 1L, 1L, 1L),
                sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertEquals(12666, loaded.getBasicStatsMetaMap().get(20L).getTotalRows());
        assertEquals(1, loaded.getMultiColumnStatsMetaMap().size());
        assertEquals(Set.of(2, 4), loaded.getMultiColumnStatsMetaMap().values().iterator().next().getColumnIds());
        assertEquals("hive.db.table.123", loaded.getExternalHistogramStatsMetaMap()
                .values().iterator().next().getTableUUID());

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testEmptySection() throws Exception {
        AnalyzeMgr source = new AnalyzeMgr();
        AnalyzeMgr loaded = new AnalyzeMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(
                LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        equivalent(source, loaded);
        assertEquals(7, sections.size());
        assertTrue(sections.stream().allMatch(section -> section.getNumEntries() == 0));

    }

    @Test
    public void testStatusDeduplicationMatchesV2() throws Exception {
        AnalyzeMgr source = populated();
        AnalyzeStatus status = source.getAnalyzeStatusMap().get(3L);
        source.getAnalyzeStatusMap().put(300L, status);
        AnalyzeMgr v2 = new AnalyzeMgr();
        StatisticsSecurityTestSupport.v2(source::save, v2::load);
        AnalyzeMgr loaded = new AnalyzeMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(
                LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        equivalent(v2, loaded);
        assertEquals(Set.of(3L, 4L), loaded.getAnalyzeStatusMap().keySet());
        assertEquals(List.of(2L, 2L, 1L, 1L, 1L, 1L, 1L),
                    sections.stream().map(IndexEntryPB::getNumEntries).toList());
    }

    @Test
    public void testLargeStatisticsCollectionIsFlat() throws Exception {
        AnalyzeMgr source = new AnalyzeMgr();
        for (int i = 0; i < 1024; i++) {
            source.replayAddBasicStatsMeta(StatsMetaProtoTest.basic(1000 + i));
        }
        AnalyzeMgr loaded = new AnalyzeMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(
                LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        assertEquals(List.of(0L, 0L, 1024L, 0L, 0L, 0L, 0L),
                sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertEquals(1024, loaded.getBasicStatsMetaMap().size());
        for (long id : List.of(1000L, 1512L, 2023L)) {
            assertEquals(12666, loaded.getBasicStatsMetaMap().get(id).getTotalRows());
            assertEquals(23, loaded.getBasicStatsMetaMap().get(id).getAnalyzedColumns().get("a").getAllPartitionSize());
        }

    }

    @Test
    public void testRecordGroupsKeepV2SaveOrder() throws Exception {
        Path file = LoadImageTestSupport.save(dir, populated()::save);
        AnalyzeMgr loaded = new AnalyzeMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
        assertEquals(List.of(160, 161, 162, 163, 164, 165, 166),
                sections.stream().map(IndexEntryPB::getType).toList());
        AnalyzeMgr baseline = new AnalyzeMgr();
        StatisticsSecurityTestSupport.v2(populated()::save, baseline::load);
        equivalent(baseline, loaded);
    }

    @Test
    public void testMalformedEntry() throws Exception {
        for (Message invalid : List.of(AnalyzeJobPB.getDefaultInstance(),
                StatsMetaProtoTest.basic(20).toProto().toBuilder()
                        .setTypeValue(999).build())) {
            Path file = StatisticsSecurityTestSupport.sections(dir,
                    AnalyzeMgrPB.getDefaultInstance(), List.of(invalid));
            ImageFormatException exception = assertThrows(ImageFormatException.class,
                    () -> LoadImageTestSupport.load(file, new AnalyzeMgr().getImageV3Loaders()));
            assertTrue(exception.getMessage().contains("ANALYZE_"));
        }
    }
}
