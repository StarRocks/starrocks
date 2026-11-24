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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.MVEpochPB;
import com.starrocks.persist.proto.MVMaintenanceJobPB;
import com.starrocks.scheduler.mv.BinlogConsumeStateVO;
import com.starrocks.scheduler.mv.BinlogConsumeStateVO.BinlogIdVO;
import com.starrocks.scheduler.mv.BinlogConsumeStateVO.BinlogLSNVO;
import com.starrocks.scheduler.mv.MVEpoch;
import com.starrocks.scheduler.mv.MVMaintenanceJob;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MaterializedViewMgrImageV3Test {
    @TempDir
    Path tempDir;
    private LocalMetastore metastore;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr global = LoadImageTestSupport.global();
        metastore = mock(LocalMetastore.class);
        when(global.getLocalMetastore()).thenReturn(metastore);
        MaterializedView view = mock(MaterializedView.class);
        when(view.isMaterializedView()).thenReturn(true);
        when(metastore.getTable(anyLong(), anyLong())).thenReturn(view);
    }

    static MVEpoch epoch() {
        MVEpoch epoch = new MVEpoch(new MvId(12, 34));
        epoch.setState(MVEpoch.EpochState.COMMITTED);
        epoch.setStartTimeMilli(123456L);
        epoch.setCommitTimeMilli(234567L);
        BinlogConsumeStateVO binlog = new BinlogConsumeStateVO();
        binlog.getBinlogMap().put(new BinlogIdVO(111), new BinlogLSNVO(9, 888));
        binlog.getBinlogMap().put(new BinlogIdVO(222), new BinlogLSNVO(10, 999));
        epoch.setBinlogState(binlog);
        return epoch;
    }

    static MVMaintenanceJob job(long id) {
        MVMaintenanceJob job = GsonUtils.GSON.fromJson(
                "{\"jobId\":" + (id + 1000) + ",\"dbId\":12,\"viewId\":" + id + ",\"state\":\"PAUSED\"}",
                MVMaintenanceJob.class);
        job.setEpoch(epoch());
        return job;
    }

    private static Map<MvId, MVMaintenanceJob> jobs(MaterializedViewMgr manager) {
        return getField(manager, "jobMap");
    }

    private MaterializedViewMgr v2(MaterializedViewMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        MaterializedViewMgr result = new MaterializedViewMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private MaterializedViewMgr v3(MaterializedViewMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        MaterializedViewMgr result = new MaterializedViewMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(250, sections.get(0).getType());
        assertEquals(0, sections.get(0).getManagerLength());
        return result;
    }

    @Test
    public void testEpochFieldsStatesAndBinlogOffsets() throws Exception {
        for (MVEpoch.EpochState state : MVEpoch.EpochState.values()) {
            MVEpoch source = epoch();
            source.setState(state);
            MVEpoch restored = MVEpoch.fromProto(MVEpochPB.parseFrom(source.toProto().toByteArray()));
            assertEquals(source, restored);
            assertEquals(source.hashCode(), restored.hashCode());
            assertNotSame(source.getBinlogState().getBinlogMap(), restored.getBinlogState().getBinlogMap());
            BinlogLSNVO offset = restored.getBinlogState().getBinlogMap().get(new BinlogIdVO(111));
            assertEquals(9, offset.getVersion());
            assertEquals(888, offset.getLsn());
            assertEquals(9, offset.toThrift().getVersion());
            assertEquals(888, offset.toThrift().getLsn());
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        }
    }

    @Test
    public void testJobUsesRuntimeStateAndIndependentIds() throws Exception {
        for (MVMaintenanceJob.JobState state : MVMaintenanceJob.JobState.values()) {
            MVMaintenanceJob source = job(34);
            setField(source, "state", new AtomicReference<>(state));
            setField(source, "serializedState", MVMaintenanceJob.JobState.FAILED);
            MVMaintenanceJob restored = MVMaintenanceJob.fromProto(
                    MVMaintenanceJobPB.parseFrom(source.toProto().toByteArray()));
            assertEquals(state, restored.getState());
            assertEquals(1034, restored.getJobId());
            assertEquals(34, restored.getViewId());
            assertEquals(12, restored.getDbId());
            assertEquals(source.getEpoch(), restored.getEpoch());
            assertEquals(source, restored);
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        }
    }

    @Test
    public void testMissingAndUnknownStatesMatchV2() {
        MVMaintenanceJob legacy = GsonUtils.GSON.fromJson("{\"state\":\"FUTURE\"}", MVMaintenanceJob.class);
        MVMaintenanceJob restored = MVMaintenanceJob.fromProto(MVMaintenanceJobPB.newBuilder().setStateValue(999).build());
        assertEquals(MVMaintenanceJob.JobState.INIT, restored.getState());
        GsonEquivalence.assertEquivalent(legacy, restored);
        assertEquals(MVMaintenanceJob.JobState.INIT,
                MVMaintenanceJob.fromProto(MVMaintenanceJobPB.getDefaultInstance()).getState());
        MVEpoch unknown = MVEpoch.fromProto(MVEpochPB.newBuilder().setStateValue(999).build());
        assertNull(unknown.getState());
        assertNull(unknown.getBinlogState());
    }

    @Test
    public void testNullableEpochAndEmptyBinlog() {
        MVMaintenanceJob source = job(34);
        source.setEpoch(null);
        assertNull(MVMaintenanceJob.fromProto(source.toProto()).getEpoch());
        MVEpoch epoch = epoch();
        epoch.setBinlogState(null);
        assertNull(MVEpoch.fromProto(epoch.toProto()).getBinlogState());
        epoch.setBinlogState(new BinlogConsumeStateVO());
        MVEpoch restored = MVEpoch.fromProto(epoch.toProto());
        assertTrue(restored.getBinlogState().getBinlogMap().isEmpty());
        restored.getBinlogState().getBinlogMap().put(new BinlogIdVO(1), new BinlogLSNVO(2, 3));
        assertTrue(epoch.getBinlogState().getBinlogMap().isEmpty());
    }

    @Test
    public void testRestoreResetsJobStateAsInV2() throws Exception {
        MaterializedViewMgr source = new MaterializedViewMgr();
        jobs(source).put(new MvId(12, 34), job(34));
        MaterializedViewMgr expected = v2(source);
        MaterializedViewMgr restored = v3(source);
        MVMaintenanceJob loaded = jobs(restored).get(new MvId(12, 34));
        assertEquals(MVMaintenanceJob.JobState.INIT, loaded.getState());
        assertEquals(jobs(expected), jobs(restored));
        assertEquals(MVEpoch.EpochState.COMMITTED, loaded.getEpoch().getState());

    }

    @Test
    public void testMissingOrWrongTargetIsSkippedAndLaterJobLoads() throws Exception {
        when(metastore.getTable(12L, 31L)).thenReturn(null);
        when(metastore.getTable(12L, 32L)).thenReturn(mock(OlapTable.class));
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            try (SectionWriter section = writer.beginSection(SectionId.MATERIALIZED_VIEW_MGR)) {
                section.writeEntry(job(31).toProto());
                section.writeEntry(job(32).toProto());
                section.writeEntry(job(33).toProto());
            }
        });
        MaterializedViewMgr restored = new MaterializedViewMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(Set.of(new MvId(12, 33)), jobs(restored).keySet());

    }

    @Test
    public void testEpochJournalReplayStillUpdatesRestoredJob() throws Exception {
        MaterializedViewMgr source = new MaterializedViewMgr();
        jobs(source).put(new MvId(12, 34), job(34));
        MaterializedViewMgr restored = v3(source);
        MVEpoch next = epoch();
        next.setCommitTimeMilli(999999L);
        restored.replayEpoch(next);
        assertEquals(next, jobs(restored).get(new MvId(12, 34)).getEpoch());

    }

    @Test
    public void testManyJobsAreSeparateEntries() throws Exception {
        MaterializedViewMgr source = new MaterializedViewMgr();
        for (int i = 1; i <= 1024; i++) {
            jobs(source).put(new MvId(12, i), job(i));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        MaterializedViewMgr restored = new MaterializedViewMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1024, sections.get(0).getNumEntries());
        assertEquals(1024, jobs(restored).size());

    }

    @Test
    public void testEmptyManager() throws Exception {
        assertTrue(jobs(v3(new MaterializedViewMgr())).isEmpty());

    }
}
