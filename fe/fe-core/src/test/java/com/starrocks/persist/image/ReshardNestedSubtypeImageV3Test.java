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

import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.ReshardingMaterializedIndexPB;
import com.starrocks.persist.proto.TabletReshardJobPB;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.persist.image.ReshardImageTestSupport.job;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class ReshardNestedSubtypeImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    private ReshardingMaterializedIndexPB index(TabletReshardJobPB job) {
        return job.getSplitTablet().getReshardingPhysicalPartitions().getValuesMap().values().iterator().next()
                .getReshardingIndexes().getValuesMap().values().iterator().next();
    }

    private TabletReshardJobPB replaceIndex(TabletReshardJobPB job, ReshardingMaterializedIndexPB index) {
        var split = job.getSplitTablet().toBuilder();
        var partitions = split.getReshardingPhysicalPartitions().toBuilder();
        long partitionId = partitions.getValuesMap().keySet().iterator().next();
        var partition = partitions.getValuesOrThrow(partitionId).toBuilder();
        var indexes = partition.getReshardingIndexes().toBuilder();
        long indexId = indexes.getValuesMap().keySet().iterator().next();
        indexes.putValues(indexId, index);
        partitions.putValues(partitionId, partition.setReshardingIndexes(indexes).build());
        return job.toBuilder().setSplitTablet(split.setReshardingPhysicalPartitions(partitions)).build();
    }

    private TabletReshardJobPB unknownIndexTablet(TabletReshardJobPB job) {
        var index = index(job).toBuilder();
        var materialized = index.getMaterializedIndex().toBuilder();
        materialized.setTablets(1, materialized.getTablets(1).toBuilder().setKindValue(900));
        return replaceIndex(job, index.setMaterializedIndex(materialized).build());
    }

    private TabletReshardJobPB unknownRangeVariant(TabletReshardJobPB job) {
        var tablet = index(job).getReshardingTablets().getValues(0).toBuilder();
        var split = tablet.getSplitting().toBuilder();
        var ranges = split.getNewTabletRanges().toBuilder();
        var range = ranges.getValues(1).toBuilder();
        var lower = range.getLowerBound().toBuilder();
        lower.setValues(0, lower.getValues(0).toBuilder().setKindValue(901));
        ranges.setValues(1, range.setLowerBound(lower));
        split.setNewTabletRanges(ranges);
        return ReshardImageTestSupport.withTablet(job, 0, tablet.setSplitting(split).build());
    }

    private TabletReshardJobMgr manager() {
        TabletReshardJobMgr manager = new TabletReshardJobMgr();
        when(global.getTabletReshardJobMgr()).thenReturn(manager);
        return manager;
    }

    @Test
    public void testUnknownCatalogTabletAndRangeVariantPropagateToOwningJob() {
        TabletReshardJobPB good = job(2, true, TabletReshardJob.JobState.RUNNING).toProto();
        assertThrows(SubtypeNotFoundException.class, () -> TabletReshardJob.fromProto(unknownIndexTablet(good)));
        assertThrows(SubtypeNotFoundException.class, () -> TabletReshardJob.fromProto(unknownRangeVariant(good)));
    }

    @Test
    public void testReaderSkipsCompleteJobForEitherNestedHierarchy() throws Exception {
        TabletReshardJobPB first = job(1, true, TabletReshardJob.JobState.RUNNING).toProto();
        TabletReshardJobPB target = job(2, true, TabletReshardJob.JobState.RUNNING).toProto();
        TabletReshardJobPB last = job(3, true, TabletReshardJob.JobState.RUNNING).toProto();
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            for (TabletReshardJobPB bad : List.of(unknownIndexTablet(target), unknownRangeVariant(target))) {
                Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.TABLET_RESHARD_JOB_MGR,
                        List.of(first, bad, last));
                Config.metadata_ignore_unknown_subtype = false;
                TabletReshardJobMgr strict = manager();
                assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, strict.getImageV3Loaders()));
                assertEquals(Set.of(1L), strict.getTabletReshardJobs().keySet());
                assertNull(strict.getReshardingTablet(2010, Long.MAX_VALUE));
                Config.metadata_ignore_unknown_subtype = true;
                TabletReshardJobMgr tolerant = manager();
                var sections = LoadImageTestSupport.load(path, tolerant.getImageV3Loaders());
                assertEquals(3, sections.get(0).getNumEntries());
                assertEquals(Set.of(1L, 3L), tolerant.getTabletReshardJobs().keySet());
                assertNull(tolerant.getReshardingTablet(2010, Long.MAX_VALUE));
                assertNull(tolerant.getReshardingTablet(2020, Long.MAX_VALUE));
                assertNotNull(tolerant.getReshardingTablet(1010, 101));
                assertNotNull(tolerant.getReshardingTablet(3010, 103));
                Map<Long, ?> mappings = getField(tolerant, "reshardingTabletInfos");
                assertEquals(Set.of(1010L, 1020L, 3010L, 3020L), mappings.keySet());
            }

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testUnknownNestedObjectDoesNotReplaceExistingJobWithSameId() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = true;
            TabletReshardJob existing = job(2, true, TabletReshardJob.JobState.PENDING);
            TabletReshardJobPB incoming = unknownRangeVariant(
                    job(2, true, TabletReshardJob.JobState.RUNNING).toProto());
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.TABLET_RESHARD_JOB_MGR,
                    List.of(incoming));
            TabletReshardJobMgr restored = manager();
            restored.getTabletReshardJobs().put(2L, existing);
            LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(TabletReshardJob.JobState.PENDING, getField(restored.getTabletReshardJob(2), "jobState"));
            assertTrue(((Map<?, ?>) getField(restored, "reshardingTabletInfos")).isEmpty());

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testMalformedKnownIndexStateIsNeverIgnoredAsSubtype() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = true;
            TabletReshardJobPB good = job(1, true, TabletReshardJob.JobState.RUNNING).toProto();
            var index = index(good).toBuilder();
            index.setMaterializedIndex(index.getMaterializedIndex().toBuilder().setStateValue(999));
            TabletReshardJobPB bad = replaceIndex(good, index.build());
            assertThrows(IllegalArgumentException.class, () -> TabletReshardJob.fromProto(bad));
            Path path = FinalManagersImageTestSupport.entries(tempDir,
                    SectionId.TABLET_RESHARD_JOB_MGR, List.of(bad));
            TabletReshardJobMgr restored = manager();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, restored.getImageV3Loaders()));
            assertTrue(restored.getTabletReshardJobs().isEmpty());
        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }
}
