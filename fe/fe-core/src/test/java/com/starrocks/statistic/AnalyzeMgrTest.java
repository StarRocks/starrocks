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
import com.starrocks.catalog.system.information.AnalyzeStatusSystemTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TAnalyzeStatusReq;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;

public class AnalyzeMgrTest {
    public static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @AfterAll
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testRefreshConnectorTableBasicStatisticsCache(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");

        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        analyzeMgr.refreshConnectorTableBasicStatisticsCache("hive0", "partitioned_db", "t1",
                ImmutableList.of("c1", "c2"), true);
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
    public void testAnalyzeMgrBasicStatsPersist() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");

        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(100,
                "hive0", "partitioned_db", "t1",
                table.getUUID(), ImmutableList.of("c1", "c2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        analyzeMgr.addAnalyzeStatus(analyzeStatus);
        // test persist by image
        UtFrameUtils.PseudoImage testImage = new UtFrameUtils.PseudoImage();
        analyzeMgr.save(testImage.getImageWriter());

        analyzeMgr.load(testImage.getMetaBlockReader());
        Assertions.assertEquals(1, analyzeMgr.getAnalyzeStatusMap().size());
        AnalyzeStatus analyzeStatus1 = analyzeMgr.getAnalyzeStatusMap().get(100L);
        Assertions.assertEquals("hive0", analyzeStatus1.getCatalogName());
        Assertions.assertEquals("partitioned_db", analyzeStatus1.getDbName());
        Assertions.assertEquals("t1", analyzeStatus1.getTableName());

        // test persist by journal
        ExternalAnalyzeStatus externalAnalyzeStatus = (ExternalAnalyzeStatus)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_EXTERNAL_ANALYZE_STATUS);
        Assertions.assertEquals(100L, externalAnalyzeStatus.getId());
        Assertions.assertEquals("hive0", externalAnalyzeStatus.getCatalogName());
        Assertions.assertEquals("partitioned_db", externalAnalyzeStatus.getDbName());
        Assertions.assertEquals("t1", externalAnalyzeStatus.getTableName());
        Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, externalAnalyzeStatus.getType());
        Assertions.assertEquals(StatsConstants.ScheduleType.ONCE, externalAnalyzeStatus.getScheduleType());

        JournalEntity journalEntity = new JournalEntity(OperationType.OP_ADD_EXTERNAL_ANALYZE_STATUS, externalAnalyzeStatus);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(1, GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeStatusMap().size());

        analyzeMgr.dropExternalAnalyzeStatus(table.getUUID());
        ExternalAnalyzeStatus removeExternalAnalyzeStatus = (ExternalAnalyzeStatus)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_REMOVE_EXTERNAL_ANALYZE_STATUS);
        Assertions.assertEquals(100L, removeExternalAnalyzeStatus.getId());
        Assertions.assertEquals("hive0", removeExternalAnalyzeStatus.getCatalogName());
        Assertions.assertEquals("partitioned_db", removeExternalAnalyzeStatus.getDbName());
        Assertions.assertEquals("t1", removeExternalAnalyzeStatus.getTableName());

        journalEntity = new JournalEntity(OperationType.OP_REMOVE_EXTERNAL_ANALYZE_STATUS, removeExternalAnalyzeStatus);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(0, GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeStatusMap().size());

        // test analyze job
        NativeAnalyzeJob nativeAnalyzeJob = new NativeAnalyzeJob(123, 1234, null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        ExternalAnalyzeJob externalAnalyzeJob = new ExternalAnalyzeJob("hive0", "hive_db", "t1",
                null, null, StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        analyzeMgr.addAnalyzeJob(nativeAnalyzeJob);
        analyzeMgr.addAnalyzeJob(externalAnalyzeJob);

        testImage = new UtFrameUtils.PseudoImage();
        analyzeMgr.save(testImage.getImageWriter());
        analyzeMgr.load(testImage.getMetaBlockReader());
        Assertions.assertEquals(2, analyzeMgr.getAllAnalyzeJobList().size());
        NativeAnalyzeJob analyzeJob = (NativeAnalyzeJob) analyzeMgr.getAllAnalyzeJobList().get(0);
        Assertions.assertEquals(123, analyzeJob.getDbId());
        Assertions.assertEquals(1234, analyzeJob.getTableId());

        ExternalAnalyzeJob analyzeJob1 = (ExternalAnalyzeJob) analyzeMgr.getAllAnalyzeJobList().get(1);
        Assertions.assertEquals("hive0", analyzeJob1.getCatalogName());
        Assertions.assertEquals("hive_db", analyzeJob1.getDbName());
        Assertions.assertEquals("t1", analyzeJob1.getTableName());

        NativeAnalyzeJob nativeAnalyzeJob1 = (NativeAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_ADD_ANALYZER_JOB);
        Assertions.assertEquals(123, nativeAnalyzeJob1.getDbId());
        Assertions.assertEquals(1234, nativeAnalyzeJob1.getTableId());

        journalEntity = new JournalEntity(OperationType.OP_ADD_ANALYZER_JOB, nativeAnalyzeJob);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(1, GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllAnalyzeJobList().size());

        ExternalAnalyzeJob externalAnalyzeJob1 = (ExternalAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_ADD_EXTERNAL_ANALYZER_JOB);
        Assertions.assertEquals("hive0", externalAnalyzeJob1.getCatalogName());
        Assertions.assertEquals("hive_db", externalAnalyzeJob1.getDbName());
        Assertions.assertEquals("t1", externalAnalyzeJob1.getTableName());

        journalEntity = new JournalEntity(OperationType.OP_ADD_EXTERNAL_ANALYZER_JOB, externalAnalyzeJob1);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(2, GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllAnalyzeJobList().size());

        analyzeMgr.removeAnalyzeJob(nativeAnalyzeJob.getId());
        NativeAnalyzeJob nativeAnalyzeJob2 = (NativeAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_REMOVE_ANALYZER_JOB);
        Assertions.assertEquals(123, nativeAnalyzeJob2.getDbId());
        Assertions.assertEquals(1234, nativeAnalyzeJob2.getTableId());

        journalEntity = new JournalEntity(OperationType.OP_REMOVE_ANALYZER_JOB, nativeAnalyzeJob);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);

        analyzeMgr.removeAnalyzeJob(externalAnalyzeJob.getId());
        ExternalAnalyzeJob externalAnalyzeJob2 = (ExternalAnalyzeJob) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_REMOVE_EXTERNAL_ANALYZER_JOB);
        Assertions.assertEquals("hive0", externalAnalyzeJob2.getCatalogName());
        Assertions.assertEquals("hive_db", externalAnalyzeJob2.getDbName());
        Assertions.assertEquals("t1", externalAnalyzeJob2.getTableName());

        journalEntity = new JournalEntity(OperationType.OP_REMOVE_EXTERNAL_ANALYZER_JOB, externalAnalyzeJob2);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(0, GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllAnalyzeJobList().size());

        // test analyze basic stats
        ExternalBasicStatsMeta externalBasicStatsMeta = new ExternalBasicStatsMeta("hive0", "hive_db",
                "t1", null, StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Maps.newHashMap());
        analyzeMgr.addExternalBasicStatsMeta(externalBasicStatsMeta);

        testImage = new UtFrameUtils.PseudoImage();
        analyzeMgr.save(testImage.getImageWriter());
        analyzeMgr.load(testImage.getMetaBlockReader());
        Assertions.assertEquals(1, analyzeMgr.getExternalBasicStatsMetaMap().size());

        ExternalBasicStatsMeta replayBasicStatsMeta = (ExternalBasicStatsMeta) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META);
        Assertions.assertEquals("hive0", replayBasicStatsMeta.getCatalogName());
        Assertions.assertEquals("hive_db", replayBasicStatsMeta.getDbName());
        Assertions.assertEquals("t1", replayBasicStatsMeta.getTableName());

        new MockUp<AnalyzeMgr>() {
            @Mock
            public void refreshConnectorTableBasicStatisticsCache(String catalogName, String dbName, String tableName,
                                                                  List<String> columns, boolean async) {
            }
        };
        journalEntity = new JournalEntity(OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META, externalBasicStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(1, GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalBasicStatsMetaMap().size());

        analyzeMgr.removeExternalBasicStatsMeta(externalBasicStatsMeta.getCatalogName(),
                externalBasicStatsMeta.getDbName(), externalBasicStatsMeta.getTableName());
        ExternalBasicStatsMeta replayBasicStatsMeta1 = (ExternalBasicStatsMeta) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META);
        Assertions.assertEquals("hive0", replayBasicStatsMeta1.getCatalogName());
        Assertions.assertEquals("hive_db", replayBasicStatsMeta1.getDbName());
        Assertions.assertEquals("t1", replayBasicStatsMeta1.getTableName());

        journalEntity = new JournalEntity(OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META, externalBasicStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(0, GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalBasicStatsMetaMap().size());
    }

    @Test
    public void testAnalyzeMgrHistogramStatsPersist() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");

        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        ExternalHistogramStatsMeta externalHistogramStatsMeta = new ExternalHistogramStatsMeta("hive0", "hive_db",
                "t1", "c1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Maps.newHashMap());
        analyzeMgr.addExternalHistogramStatsMeta(externalHistogramStatsMeta);
        // test persist by image
        UtFrameUtils.PseudoImage testImage = new UtFrameUtils.PseudoImage();
        analyzeMgr.save(testImage.getImageWriter());
        analyzeMgr.load(testImage.getMetaBlockReader());
        Assertions.assertEquals(1, analyzeMgr.getExternalHistogramStatsMetaMap().size());
        // test replay json to histogram stats
        ExternalHistogramStatsMeta replayHistogramStatsMeta =
                (ExternalHistogramStatsMeta) UtFrameUtils.PseudoJournalReplayer.
                        replayNextJournal(OperationType.OP_ADD_EXTERNAL_HISTOGRAM_STATS_META);
        Assertions.assertEquals("hive0", replayHistogramStatsMeta.getCatalogName());
        Assertions.assertEquals("hive_db", replayHistogramStatsMeta.getDbName());
        Assertions.assertEquals("t1", replayHistogramStatsMeta.getTableName());
        // test replay journal
        JournalEntity journalEntity = new JournalEntity(OperationType.OP_ADD_EXTERNAL_HISTOGRAM_STATS_META,
                externalHistogramStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(1,
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalHistogramStatsMetaMap().size());

        // test remove histogram stats
        new MockUp<StatisticExecutor>() {
            @Mock
            public void dropExternalHistogram(ConnectContext statsConnectCtx, String tableUUID,
                                              List<String> columnNames) {
                return;
            }
        };
        analyzeMgr.dropExternalHistogramStatsMetaAndData(connectContext, new TableName(
                externalHistogramStatsMeta.getCatalogName(), externalHistogramStatsMeta.getDbName(),
                externalHistogramStatsMeta.getTableName()), table, ImmutableList.of("c1"));
        replayHistogramStatsMeta = (ExternalHistogramStatsMeta) UtFrameUtils.PseudoJournalReplayer.
                replayNextJournal(OperationType.OP_REMOVE_EXTERNAL_HISTOGRAM_STATS_META);
        Assertions.assertEquals("hive0", replayHistogramStatsMeta.getCatalogName());
        Assertions.assertEquals("hive_db", replayHistogramStatsMeta.getDbName());
        Assertions.assertEquals("t1", replayHistogramStatsMeta.getTableName());
        Assertions.assertEquals("c1", replayHistogramStatsMeta.getColumn());

        journalEntity = new JournalEntity(OperationType.OP_REMOVE_EXTERNAL_HISTOGRAM_STATS_META, externalHistogramStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);
        Assertions.assertEquals(0,
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalHistogramStatsMetaMap().size());
    }

    @Test
    public void testDropExternalAnalyzeStatus() {
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");

        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(100,
                "hive0", "partitioned_db", "t1",
                table.getUUID(), ImmutableList.of("c1", "c2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        analyzeMgr.addAnalyzeStatus(analyzeStatus);

        analyzeMgr.dropExternalAnalyzeStatus(table.getUUID());
        Assertions.assertEquals(0, analyzeMgr.getAnalyzeStatusMap().size());
    }

    @Test
    public void testUpdateLoadRowsWithTableDropped() {
        long dbId = 11111L;
        long tableId = 22222L;
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(new Database(dbId, "test"));
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(dbId, tableId,
                Lists.newArrayList("c1"), StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), new HashMap<>()));

        TUniqueId requestId = UUIDUtil.genTUniqueId();
        TransactionState transactionState = new TransactionState(dbId, Lists.newArrayList(tableId), 33333L, "xxx",
                requestId, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 44444L, 10000);
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(0));
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateLoadRows(transactionState);
    }

    @Test
    public void testQuery() throws Exception {
        final String dbName = "db_analyze_status";
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert
                .withDatabase(dbName)
                .useDatabase(dbName)
                .withTable("create table t1 (c1 int, c2 int) properties('replication_num'='1')");
        UtFrameUtils.mockDML();
        starRocksAssert.getCtx().executeSql("insert into t1 values (1,1)");
        starRocksAssert.getCtx().executeSql("analyze table t1 with sync mode");

        // Add the analyze status but drop the table
        Database db = starRocksAssert.getDb(dbName);
        Table table = starRocksAssert.getTable(dbName, "t1");
        AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(100,
                db.getId(), table.getId(),
                ImmutableList.of("c1", "c2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        starRocksAssert.dropDatabase(dbName).withDatabase(dbName);

        TAnalyzeStatusReq request = new TAnalyzeStatusReq();
        AnalyzeStatusSystemTable.query(request);
    }

}
