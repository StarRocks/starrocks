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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableColumnStats;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class AnalyzeMgrTest {
    public static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testRefreshConnectorTableBasicStatisticsCache(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");
        new Expectations() {
            {
                cachedStatisticStorage.getConnectorTableStatistics(table, ImmutableList.of("c1", "c2"));
                result = ImmutableList.of(
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 10, 0, 20, 5), 5),
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 100, 0, 200, 50), 50)
                );
                minTimes = 1;
            }
        };


        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        analyzeMgr.refreshConnectorTableBasicStatisticsCache("hive0", "partitioned_db", "t1",
                ImmutableList.of("c1", "c2"), true);

        new Expectations() {
            {
                cachedStatisticStorage.getConnectorTableStatisticsSync(table, ImmutableList.of("c1", "c2"));
                result = ImmutableList.of(
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 10, 0, 20, 5), 5),
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 100, 0, 200, 50), 50)
                );
                minTimes = 1;
            }
        };
        analyzeMgr.refreshConnectorTableBasicStatisticsCache("hive0", "partitioned_db", "t1",
                ImmutableList.of("c1", "c2"), false);

        new MockUp<MetaUtils>() {
            @Mock
            public Table getTable(String catalogName, String dbName, String tableName) {
                throw new RuntimeException("mock get table exception");
            }
        };
        analyzeMgr.refreshConnectorTableBasicStatisticsCache("hive0", "partitioned_db", "t1",
                ImmutableList.of("c1", "c2"), false);
    }

    @Test
    public void testAnalyzeMgrPersist() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");

        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(100,
                "hive0", "partitioned_db", "t1",
                table.getUUID(), ImmutableList.of("c1", "c2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        analyzeMgr.addAnalyzeStatus(analyzeStatus);
        // test persist by image
        UtFrameUtils.PseudoImage testImage = new UtFrameUtils.PseudoImage();
        analyzeMgr.save(testImage.getDataOutputStream());

        analyzeMgr.load(new SRMetaBlockReader(testImage.getDataInputStream()));
        Assert.assertEquals(1, analyzeMgr.getAnalyzeStatusMap().size());
        AnalyzeStatus analyzeStatus1 = analyzeMgr.getAnalyzeStatusMap().get(100L);
        Assert.assertEquals("hive0", analyzeStatus1.getCatalogName());
        Assert.assertEquals("partitioned_db", analyzeStatus1.getDbName());
        Assert.assertEquals("t1", analyzeStatus1.getTableName());

        // test persist by journal
        ExternalAnalyzeStatus externalAnalyzeStatus = (ExternalAnalyzeStatus)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_EXTERNAL_ANALYZE_STATUS);
        Assert.assertEquals(100L, externalAnalyzeStatus.getId());
        Assert.assertEquals("hive0", externalAnalyzeStatus.getCatalogName());
        Assert.assertEquals("partitioned_db", externalAnalyzeStatus.getDbName());
        Assert.assertEquals("t1", externalAnalyzeStatus.getTableName());
        Assert.assertEquals(StatsConstants.AnalyzeType.FULL, externalAnalyzeStatus.getType());
        Assert.assertEquals(StatsConstants.ScheduleType.ONCE, externalAnalyzeStatus.getScheduleType());

        JournalEntity journalEntity = new JournalEntity();
        journalEntity.setOpCode(OperationType.OP_ADD_EXTERNAL_ANALYZE_STATUS);
        journalEntity.setData(externalAnalyzeStatus);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assert.assertEquals(1, GlobalStateMgr.getCurrentAnalyzeMgr().getAnalyzeStatusMap().size());

        analyzeMgr.dropExternalAnalyzeStatus(table.getUUID());
        ExternalAnalyzeStatus removeExternalAnalyzeStatus = (ExternalAnalyzeStatus)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_REMOVE_EXTERNAL_ANALYZE_STATUS);
        Assert.assertEquals(100L, removeExternalAnalyzeStatus.getId());
        Assert.assertEquals("hive0", removeExternalAnalyzeStatus.getCatalogName());
        Assert.assertEquals("partitioned_db", removeExternalAnalyzeStatus.getDbName());
        Assert.assertEquals("t1", removeExternalAnalyzeStatus.getTableName());

        journalEntity.setOpCode(OperationType.OP_REMOVE_EXTERNAL_ANALYZE_STATUS);
        journalEntity.setData(removeExternalAnalyzeStatus);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assert.assertEquals(0, GlobalStateMgr.getCurrentAnalyzeMgr().getAnalyzeStatusMap().size());

        // test analyze job
        NativeAnalyzeJob nativeAnalyzeJob = new NativeAnalyzeJob(123, 1234, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        ExternalAnalyzeJob externalAnalyzeJob = new ExternalAnalyzeJob("hive0", "hive_db", "t1",
                null, StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        analyzeMgr.addAnalyzeJob(nativeAnalyzeJob);
        analyzeMgr.addAnalyzeJob(externalAnalyzeJob);

        testImage = new UtFrameUtils.PseudoImage();
        analyzeMgr.save(testImage.getDataOutputStream());
        analyzeMgr.load(new SRMetaBlockReader(testImage.getDataInputStream()));
        Assert.assertEquals(2, analyzeMgr.getAllAnalyzeJobList().size());
        NativeAnalyzeJob analyzeJob = (NativeAnalyzeJob) analyzeMgr.getAllAnalyzeJobList().get(0);
        Assert.assertEquals(123, analyzeJob.getDbId());
        Assert.assertEquals(1234, analyzeJob.getTableId());

        ExternalAnalyzeJob analyzeJob1 = (ExternalAnalyzeJob) analyzeMgr.getAllAnalyzeJobList().get(1);
        Assert.assertEquals("hive0", analyzeJob1.getCatalogName());
        Assert.assertEquals("hive_db", analyzeJob1.getDbName());
        Assert.assertEquals("t1", analyzeJob1.getTableName());

        NativeAnalyzeJob nativeAnalyzeJob1 = (NativeAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_ADD_ANALYZER_JOB);
        Assert.assertEquals(123, nativeAnalyzeJob1.getDbId());
        Assert.assertEquals(1234, nativeAnalyzeJob1.getTableId());

        journalEntity.setOpCode(OperationType.OP_ADD_ANALYZER_JOB);
        journalEntity.setData(nativeAnalyzeJob);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assert.assertEquals(1, GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList().size());

        ExternalAnalyzeJob externalAnalyzeJob1 = (ExternalAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_ADD_EXTERNAL_ANALYZER_JOB);
        Assert.assertEquals("hive0", externalAnalyzeJob1.getCatalogName());
        Assert.assertEquals("hive_db", externalAnalyzeJob1.getDbName());
        Assert.assertEquals("t1", externalAnalyzeJob1.getTableName());

        journalEntity.setOpCode(OperationType.OP_ADD_EXTERNAL_ANALYZER_JOB);
        journalEntity.setData(externalAnalyzeJob1);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assert.assertEquals(2, GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList().size());

        analyzeMgr.removeAnalyzeJob(nativeAnalyzeJob.getId());
        NativeAnalyzeJob nativeAnalyzeJob2 = (NativeAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_REMOVE_ANALYZER_JOB);
        Assert.assertEquals(123, nativeAnalyzeJob2.getDbId());
        Assert.assertEquals(1234, nativeAnalyzeJob2.getTableId());

        journalEntity.setOpCode(OperationType.OP_REMOVE_ANALYZER_JOB);
        journalEntity.setData(nativeAnalyzeJob);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);

        analyzeMgr.removeAnalyzeJob(externalAnalyzeJob.getId());
        ExternalAnalyzeJob externalAnalyzeJob2 = (ExternalAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_REMOVE_EXTERNAL_ANALYZER_JOB);
        Assert.assertEquals("hive0", externalAnalyzeJob2.getCatalogName());
        Assert.assertEquals("hive_db", externalAnalyzeJob2.getDbName());
        Assert.assertEquals("t1", externalAnalyzeJob2.getTableName());

        journalEntity.setOpCode(OperationType.OP_REMOVE_EXTERNAL_ANALYZER_JOB);
        journalEntity.setData(externalAnalyzeJob2);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assert.assertEquals(0, GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList().size());

        // test analyze basic stats
        ExternalBasicStatsMeta externalBasicStatsMeta = new ExternalBasicStatsMeta("hive0", "hive_db",
                "t1", null, StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Maps.newHashMap());
        analyzeMgr.addExternalBasicStatsMeta(externalBasicStatsMeta);

        testImage = new UtFrameUtils.PseudoImage();
        analyzeMgr.save(testImage.getDataOutputStream());
        analyzeMgr.load(new SRMetaBlockReader(testImage.getDataInputStream()));
        Assert.assertEquals(1, analyzeMgr.getExternalBasicStatsMetaMap().size());

        ExternalBasicStatsMeta replayBasicStatsMeta = (ExternalBasicStatsMeta) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META);
        Assert.assertEquals("hive0", replayBasicStatsMeta.getCatalogName());
        Assert.assertEquals("hive_db", replayBasicStatsMeta.getDbName());
        Assert.assertEquals("t1", replayBasicStatsMeta.getTableName());

        new MockUp<AnalyzeMgr>() {
            @Mock
            public void refreshConnectorTableBasicStatisticsCache(String catalogName, String dbName, String tableName,
                                                                  List<String> columns, boolean async) {}
        };
        journalEntity.setOpCode(OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META);
        journalEntity.setData(externalBasicStatsMeta);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assert.assertEquals(1, GlobalStateMgr.getCurrentAnalyzeMgr().getExternalBasicStatsMetaMap().size());

        analyzeMgr.removeExternalBasicStatsMeta(externalBasicStatsMeta.getCatalogName(),
                externalBasicStatsMeta.getDbName(), externalBasicStatsMeta.getTableName());
        ExternalBasicStatsMeta replayBasicStatsMeta1 = (ExternalBasicStatsMeta) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META);
        Assert.assertEquals("hive0", replayBasicStatsMeta1.getCatalogName());
        Assert.assertEquals("hive_db", replayBasicStatsMeta1.getDbName());
        Assert.assertEquals("t1", replayBasicStatsMeta1.getTableName());

        journalEntity.setOpCode(OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META);
        journalEntity.setData(externalBasicStatsMeta);
        EditLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assert.assertEquals(0, GlobalStateMgr.getCurrentAnalyzeMgr().getExternalBasicStatsMetaMap().size());
    }

    @Test
    public void testExternalAnalyzeStatusPersist() throws Exception {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");

        ExternalAnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(100,
                "hive0", "partitioned_db", "t1",
                table.getUUID(), ImmutableList.of("c1", "c2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        UtFrameUtils.PseudoImage testImage = new UtFrameUtils.PseudoImage();
        analyzeStatus.write(testImage.getDataOutputStream());

        ExternalAnalyzeStatus loadAnalyzeStatus = ExternalAnalyzeStatus.read(testImage.getDataInputStream());
        Assert.assertEquals("hive0", loadAnalyzeStatus.getCatalogName());
        Assert.assertEquals("partitioned_db", loadAnalyzeStatus.getDbName());
        Assert.assertEquals("t1", loadAnalyzeStatus.getTableName());
        Assert.assertEquals(StatsConstants.AnalyzeType.FULL, loadAnalyzeStatus.getType());
        Assert.assertEquals(StatsConstants.ScheduleType.ONCE, loadAnalyzeStatus.getScheduleType());
    }

    @Test
    public void testDropExternalAnalyzeStatus() {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");

        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(100,
                "hive0", "partitioned_db", "t1",
                table.getUUID(), ImmutableList.of("c1", "c2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        analyzeMgr.addAnalyzeStatus(analyzeStatus);

        analyzeMgr.dropExternalAnalyzeStatus(table.getUUID());
        Assert.assertEquals(0, analyzeMgr.getAnalyzeStatusMap().size());
    }

    @Test
    public void testUpdateLoadRowsWithTableDropped() {
        long dbId = 11111L;
        long tableId = 22222L;
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(new Database(dbId, "test"));
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(dbId, tableId,
                Lists.newArrayList("c1"), StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), new HashMap<>()));

        UUID uuid = UUID.randomUUID();
        TUniqueId requestId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        TransactionState transactionState = new TransactionState(dbId, Lists.newArrayList(tableId), 33333L, "xxx",
                requestId, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 44444L, 10000);
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(0));
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateLoadRows(transactionState);
    }
}
