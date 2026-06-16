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

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExternalStatsReplayTest {
    private static final String TABLE_UUID = "catalog.db.table.123";

    private GlobalStateMgr state;
    private AnalyzeMgr analyzeMgr;
    private MetadataMgr metadataMgr;
    private StatisticStorage storage;
    private EditLog editLog;
    private boolean originalLazyRefresh;

    @BeforeEach
    public void setUp() {
        originalLazyRefresh = Config.enable_external_stats_lazy_refresh_on_replay;
        state = mock(GlobalStateMgr.class);
        analyzeMgr = new AnalyzeMgr();
        metadataMgr = mock(MetadataMgr.class);
        storage = mock(StatisticStorage.class);
        when(state.getAnalyzeMgr()).thenReturn(analyzeMgr);
        when(state.getMetadataMgr()).thenReturn(metadataMgr);
        when(state.getStatisticStorage()).thenReturn(storage);
        VariableMgr variableMgr = mock(VariableMgr.class);
        when(variableMgr.newSessionVariable()).thenAnswer(invocation -> new SessionVariable());
        when(state.getVariableMgr()).thenReturn(variableMgr);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return state;
            }

            @Mock
            public boolean isCheckpointThread() {
                return false;
            }
        };
        editLog = new EditLog(new LinkedBlockingQueue<>());
    }

    @AfterEach
    public void tearDown() {
        Config.enable_external_stats_lazy_refresh_on_replay = originalLazyRefresh;
    }

    @Test
    public void testBasicStatsJournalReplayAvoidsExternalMetadata() throws Exception {
        Config.enable_external_stats_lazy_refresh_on_replay = true;
        replayBasicStats(TABLE_UUID);
        verify(storage, times(2)).invalidateConnectorTableColumnStatistics(TABLE_UUID, ImmutableList.of("c1", "c2"));
        verify(metadataMgr, never()).getTable(any(ConnectContext.class), anyString(), anyString(), anyString());
    }

    @Test
    public void testHistogramJournalReplayAvoidsExternalMetadata() throws Exception {
        Config.enable_external_stats_lazy_refresh_on_replay = true;
        replayHistogramStats(TABLE_UUID);
        verify(storage, times(2)).invalidateConnectorHistogramStatistics(TABLE_UUID, ImmutableList.of("c1"));
        verify(metadataMgr, never()).getTable(any(ConnectContext.class), anyString(), anyString(), anyString());
    }

    @Test
    public void testDisabledFeatureRetainsMetadataLookup() throws Exception {
        Config.enable_external_stats_lazy_refresh_on_replay = false;
        replayBasicStats(TABLE_UUID);
        replayHistogramStats(TABLE_UUID);
        verify(metadataMgr, times(4)).getTable(any(ConnectContext.class), anyString(), anyString(), anyString());
        verify(storage, never()).invalidateConnectorTableColumnStatistics(anyString(), anyList());
        verify(storage, never()).invalidateConnectorHistogramStatistics(anyString(), anyList());
    }

    @Test
    public void testLegacyJournalsRetainMetadataLookup() throws Exception {
        Config.enable_external_stats_lazy_refresh_on_replay = true;
        replayBasicStats(null);
        replayHistogramStats(null);
        verify(metadataMgr, times(4)).getTable(any(ConnectContext.class), anyString(), anyString(), anyString());
        verify(storage, never()).invalidateConnectorTableColumnStatistics(anyString(), anyList());
        verify(storage, never()).invalidateConnectorHistogramStatistics(anyString(), anyList());
    }

    @Test
    public void testEmptyUuidRetainsMetadataLookup() throws Exception {
        Config.enable_external_stats_lazy_refresh_on_replay = true;
        replayBasicStats("");
        replayHistogramStats("");
        verify(metadataMgr, times(4)).getTable(any(ConnectContext.class), anyString(), anyString(), anyString());
    }

    @Test
    public void testUuidSurvivesSerializationAndLegacyMetadataStillLoads() {
        ExternalBasicStatsMeta basic = basicMeta(TABLE_UUID);
        Assertions.assertEquals(TABLE_UUID, basic.clone().getTableUUID());
        ExternalHistogramStatsMeta histogram = histogramMeta(TABLE_UUID);
        ExternalHistogramStatsMeta restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(histogram), ExternalHistogramStatsMeta.class);
        Assertions.assertEquals(TABLE_UUID, restored.getTableUUID());
        Assertions.assertNull(GsonUtils.GSON.fromJson(
                "{\"catalogName\":\"catalog\",\"dbName\":\"db\",\"tableName\":\"table\"}",
                ExternalBasicStatsMeta.class).getTableUUID());
        Assertions.assertNull(GsonUtils.GSON.fromJson(
                "{\"catalogName\":\"catalog\",\"dbName\":\"db\",\"tableName\":\"table\",\"column\":\"c1\"}",
                ExternalHistogramStatsMeta.class).getTableUUID());
    }

    private void replayBasicStats(String uuid) throws Exception {
        ExternalBasicStatsMeta meta = basicMeta(uuid);
        editLog.loadJournal(state, new JournalEntity(OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META, meta));
        Assertions.assertSame(meta, analyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));
        editLog.loadJournal(state, new JournalEntity(OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META, meta));
        Assertions.assertTrue(analyzeMgr.getExternalBasicStatsMetaMap().isEmpty());
    }

    private void replayHistogramStats(String uuid) throws Exception {
        ExternalHistogramStatsMeta meta = histogramMeta(uuid);
        editLog.loadJournal(state, new JournalEntity(OperationType.OP_ADD_EXTERNAL_HISTOGRAM_STATS_META, meta));
        Assertions.assertEquals(1, analyzeMgr.getExternalHistogramStatsMetaMap().size());
        editLog.loadJournal(state, new JournalEntity(OperationType.OP_REMOVE_EXTERNAL_HISTOGRAM_STATS_META, meta));
        Assertions.assertTrue(analyzeMgr.getExternalHistogramStatsMetaMap().isEmpty());
    }

    private ExternalBasicStatsMeta basicMeta(String uuid) {
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta("catalog", "db", "table", ImmutableList.of("c1", "c2"),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());
        meta.setTableUUID(uuid);
        return meta;
    }

    private ExternalHistogramStatsMeta histogramMeta(String uuid) {
        ExternalHistogramStatsMeta meta = new ExternalHistogramStatsMeta("catalog", "db", "table", "c1",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());
        meta.setTableUUID(uuid);
        return meta;
    }
}
