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

import com.starrocks.catalog.Dictionary.DictionaryState;
import com.starrocks.catalog.DictionaryMgr;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.common.Pair;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionStatistics.CompactionPriority;
import com.starrocks.load.InsertOverwriteJobMgr;
import com.starrocks.load.InsertOverwriteJobState;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.mv.MVMaintenanceJob;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskThroughDictionaryImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;
    private ConnectorMgr connectors;

    @BeforeEach
    public void setUp() {
        global = TaskImageTestSupport.global();
        ResourceMgr resources = mock(ResourceMgr.class);
        when(resources.getNeedMappingCatalogResources()).thenReturn(List.of());
        when(global.getResourceMgr()).thenReturn(resources);
        LocalMetastore metastore = mock(LocalMetastore.class);
        when(global.getLocalMetastore()).thenReturn(metastore);
        MaterializedView view = mock(MaterializedView.class);
        when(view.isMaterializedView()).thenReturn(true);
        when(metastore.getTable(anyLong(), anyLong())).thenReturn(view);
        connectors = mock(ConnectorMgr.class);
        when(connectors.createConnector(any(), eq(true))).thenAnswer(invocation -> mock(CatalogConnector.class));
        new MockUp<MetaUtils>() {
            @Mock
            public boolean isPhysicalPartitionExist(GlobalStateMgr state, long dbId, long tableId, long partitionId) {
                return true;
            }
        };
    }

    private final class Managers {
        private final TaskManager tasks = new TaskManager();
        private final CatalogMgr catalogs = new CatalogMgr(connectors);
        private final InsertOverwriteJobMgr overwrite = new InsertOverwriteJobMgr();
        private final CompactionMgr compaction = new CompactionMgr();
        private final StreamLoadMgr streamLoad = new StreamLoadMgr();

        private final MaterializedViewMgr mv = new MaterializedViewMgr();
        private final GlobalFunctionMgr functions = new GlobalFunctionMgr();
        private final SharedNothingStorageVolumeMgr volumes = new SharedNothingStorageVolumeMgr();

        private final DictionaryMgr dictionaries = new DictionaryMgr();

        void save(ImageWriterV3 writer) throws java.io.IOException {
            tasks.save(writer);
            catalogs.save(writer);
            overwrite.save(writer);
            compaction.save(writer);
            streamLoad.save(writer);
            mv.save(writer);
            functions.save(writer);
            volumes.save(writer);
            dictionaries.save(writer);
        }

        List<Pair<SectionId, SectionLoader>> loaders() {
            List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>();
            loaders.addAll(tasks.getImageV3Loaders());
            loaders.addAll(catalogs.getImageV3Loaders());
            loaders.addAll(overwrite.getImageV3Loaders());
            loaders.addAll(compaction.getImageV3Loaders());
            loaders.addAll(streamLoad.getImageV3Loaders());
            loaders.addAll(mv.getImageV3Loaders());
            loaders.addAll(functions.getImageV3Loaders());
            loaders.addAll(volumes.getImageV3Loaders());
            loaders.addAll(dictionaries.getImageV3Loaders());
            return loaders;
        }
    }

    @Test
    public void testThirteenSectionsInV2ManagerOrder() throws Exception {
        Managers source = new Managers();
        source.tasks.replayCreateTask(TaskPersistenceImageV3Test.task(1));
        source.tasks.getTaskRunHistory().addHistory(TaskPersistenceImageV3Test.status(1, Constants.TaskRunState.SUCCESS));
        CatalogMgrImageV3Test.add(source.catalogs, CatalogMgrImageV3Test.catalog(2, "hive"));
        source.overwrite.registerOverwriteJob(InsertOverwriteImageV3Test.job(3, 20, InsertOverwriteJobState.OVERWRITE_RUNNING));
        CompactionMgrImageV3Test.add(source.compaction, CompactionMgrImageV3Test.statistics(30, CompactionPriority.UNSHARE));
        source.streamLoad.addLoadTask(StreamLoadPersistenceImageV3Test.task(4, StreamLoadTask.State.PREPARED));
        source.streamLoad.addLoadTask(StreamLoadPersistenceImageV3Test.multi(5, StreamLoadMultiStmtTask.State.ABORTING));
        Map<MvId, MVMaintenanceJob> jobs = getField(source.mv, "jobMap");
        jobs.put(new MvId(12, 34), MaterializedViewMgrImageV3Test.job(34));
        source.functions.replayAddFunction(GlobalFunctionMgrImageV3Test.scalar("global_fn", IntegerType.INT, -1));
        source.volumes.replayCreateStorageVolume(StorageVolumeImageV3Test.volume("volume"));
        DictionaryMgrImageV3Test.add(source.dictionaries, DictionaryMgrImageV3Test.dictionary(6, DictionaryState.COMMITTING));
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        LoadImageTestSupport.transactions(global);
        Managers restored = new Managers();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.loaders());
        assertEquals(List.of(200, 201, 210, 220, 221, 230, 240, 241, 250, 260, 270, 280, 281),
                sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(List.of(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 0L, 1L, 1L),
                sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertTrue(sections.stream().filter(section -> section.getType() != 270 && section.getType() != 280)
                .allMatch(section -> section.getManagerLength() == 0));
        GsonEquivalence.assertEquivalent(source.tasks.getTask("task-1"), restored.tasks.getTask("task-1"));
        assertEquals(Constants.TaskRunState.SUCCESS, restored.tasks.getTaskRunHistory().getTask("query-1").getState());
        GsonEquivalence.assertEquivalent(source.catalogs.getCatalogByName("hive"),
                restored.catalogs.getCatalogByName("hive"));
        GsonEquivalence.assertEquivalent(source.overwrite.getInsertOverwriteJob(3),
                restored.overwrite.getInsertOverwriteJob(3));
        assertTrue(restored.overwrite.hasRunningOverwriteJob(20));
        assertEquals(1, restored.compaction.getPartitionStatsCount());
        assertEquals(2, restored.streamLoad.getAllTasks().size());
        Map<MvId, MVMaintenanceJob> restoredJobs = getField(restored.mv, "jobMap");
        assertEquals(MVMaintenanceJob.JobState.INIT, restoredJobs.get(new MvId(12, 34)).getState());
        assertEquals(1, restored.functions.getFunctions().size());
        assertEquals("global_fn", restored.functions.getFunctions().get(0).getFunctionName().getFunction());
        GsonEquivalence.assertEquivalent(source.volumes.getStorageVolume("volume"),
                restored.volumes.getStorageVolume("volume"));
        assertEquals(DictionaryState.UNINITIALIZED,
                getField(restored.dictionaries.getDictionariesMapById().get(6L), "state"));
        assertEquals(Map.of("dictionary-6", 6L), restored.dictionaries.getDictionariesIdMapByName());
        assertSame(restored.streamLoad.getTaskByLabel("multi-5"),
                global.getGlobalTransactionMgr().getCallbackFactory().getCallback(5));

    }

    @Test
    public void testAllEmptySectionsRemainIndividuallyAddressable() throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, new Managers()::save);
        Managers restored = new Managers();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.loaders());
        assertEquals(13, sections.size());
        assertTrue(sections.stream().allMatch(section -> section.getNumEntries() == 0));
        assertEquals(13, restored.loaders().stream().map(pair -> pair.first).distinct().count());

    }

    @Test
    public void testManagerIdRangesLeaveExpansionSpace() {
        Managers managers = new Managers();
        List<Integer> ids = managers.loaders().stream().map(pair -> pair.first.getValue()).toList();
        assertEquals(List.of(200, 201, 210, 220, 221, 230, 240, 241, 250, 260, 270, 280, 281), ids);
        assertEquals(List.of(200, 210, 220, 230, 240, 250, 260, 270, 280),
                ids.stream().map(id -> id / 10 * 10).distinct().toList());
    }
}
