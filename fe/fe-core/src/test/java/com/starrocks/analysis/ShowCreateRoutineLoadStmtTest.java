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


package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.ShowCreateRoutineLoadStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ShowCreateRoutineLoadStmtTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        // create connect context
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");
        ShowCreateRoutineLoadStmt stmt = new ShowCreateRoutineLoadStmt(new LabelName("testDb", "testJob"));
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals("testJob", stmt.getName());
        Assertions.assertEquals("testDb", stmt.getDbFullName());
        Assertions.assertEquals(2, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("Job", new ShowResultMetaFactory().getMetadata(stmt).getColumn(0).getName());
        Assertions.assertEquals("Create Job", new ShowResultMetaFactory().getMetadata(stmt).getColumn(1).getName());
    }

    @Test
    public void testBackquote() throws SecurityException, IllegalArgumentException {
        String sql = "SHOW CREATE ROUTINE LOAD testDb.rl_test";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());

        ShowCreateRoutineLoadStmt stmt = (ShowCreateRoutineLoadStmt) stmts.get(0);
        Assertions.assertEquals("testDb", stmt.getDbFullName());
        Assertions.assertEquals("rl_test", stmt.getName());
    }

    @Test
    public void testNoDb() {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb2");
        ShowCreateRoutineLoadStmt stmt = new ShowCreateRoutineLoadStmt(new LabelName(null, "testJob2"));
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals("testJob2", stmt.getName());
        Assertions.assertEquals("testDb2", stmt.getDbFullName());
    }

    // ===== The generated DDL must not contain a spurious comma before the first load-desc
    // clause (COLUMNS / PARTITION / WHERE) when no COLUMNS TERMINATED BY precedes it, otherwise
    // SHOW CREATE ROUTINE LOAD output is non-runnable. =====

    // Build a KafkaRoutineLoadJob whose data-source getters are stubbed so the test only
    // exercises the load-desc clause assembly in visitShowCreateRoutineLoadStatement.
    private KafkaRoutineLoadJob baseJob() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        new Expectations(job) {
            {
                job.getDataSourceTypeName();
                minTimes = 0;
                result = "KAFKA";

                job.jobPropertiesToSql();
                minTimes = 0;
                result = "(\n\"desired_concurrent_number\" = \"1\"\n)\n";

                job.dataSourcePropertiesToSql();
                minTimes = 0;
                result = "(\n\"kafka_broker_list\" = \"localhost:9092\",\n\"kafka_topic\" = \"topic\"\n)";
            }
        };
        return job;
    }

    // Resolve the job and its db/table through the shared ctx's real GlobalStateMgr. The mocks
    // live inside the calling test method and expire when it returns.
    private String buildDDL(RoutineLoadJob job) {
        Database db = new Database(10000L, "testDb");
        OlapTable table = new OlapTable();
        table.setName("testTbl");
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return db;
            }

            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return table;
            }
        };
        new MockUp<RoutineLoadMgr>() {
            @Mock
            public List<RoutineLoadJob> getJob(String dbFullName, String jobName, boolean includeHistory) {
                return Lists.newArrayList(job);
            }
        };
        ShowCreateRoutineLoadStmt stmt = new ShowCreateRoutineLoadStmt(new LabelName("testDb", "testJob"));
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        return rs.getResultRows().get(0).get(1);
    }

    @Test
    public void testShowCreateRoutineLoadClauseSeparators() {
        // COLUMNS is the first load-desc clause (no COLUMNS TERMINATED BY): the table name must
        // not be followed by a comma.
        KafkaRoutineLoadJob columnsOnly = baseJob();
        Deencapsulation.setField(columnsOnly, "columnDescs",
                Lists.newArrayList(new ImportColumnDesc("col1"), new ImportColumnDesc("col2")));
        String columnsDdl = buildDDL(columnsOnly);
        Assertions.assertFalse(columnsDdl.contains("testTbl,"), columnsDdl);
        Assertions.assertTrue(columnsDdl.contains("on testTbl\nCOLUMNS (col1, col2)"), columnsDdl);

        // COLUMNS TERMINATED BY precedes COLUMNS(...): the comma separator is required.
        KafkaRoutineLoadJob columnsAfterSeparator = baseJob();
        Deencapsulation.setField(columnsAfterSeparator, "columnSeparator", new ColumnSeparator(","));
        Deencapsulation.setField(columnsAfterSeparator, "columnDescs",
                Lists.newArrayList(new ImportColumnDesc("col1")));
        String separatorDdl = buildDDL(columnsAfterSeparator);
        Assertions.assertTrue(separatorDdl.contains(",\nCOLUMNS (col1)"), separatorDdl);

        // PARTITION is the first clause: no comma after the table name.
        KafkaRoutineLoadJob partitionOnly = baseJob();
        Deencapsulation.setField(partitionOnly, "partitions",
                new PartitionNames(false, Lists.newArrayList("p1", "p2")));
        String partitionDdl = buildDDL(partitionOnly);
        Assertions.assertFalse(partitionDdl.contains("testTbl,"), partitionDdl);
        Assertions.assertTrue(partitionDdl.contains("on testTbl\nPARTITIONS (p1, p2)"), partitionDdl);

        // WHERE is the only clause: no comma after the table name.
        KafkaRoutineLoadJob whereOnly = baseJob();
        Deencapsulation.setField(whereOnly, "whereExpr", SqlParser.parseSqlToExpr("k1 > 100", 0));
        String whereDdl = buildDDL(whereOnly);
        Assertions.assertFalse(whereDdl.contains("testTbl,"), whereDdl);
        Assertions.assertTrue(whereDdl.contains("on testTbl\nWHERE "), whereDdl);
    }

    // Like baseJob(), but with a real broker list / topic and the real
    // KafkaRoutineLoadJob.dataSourcePropertiesToSql(), so the FROM KAFKA clause is exercised too.
    private KafkaRoutineLoadJob kafkaJob() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10000L, 20000L, "localhost:9092", "topic");
        new Expectations(job) {
            {
                job.jobPropertiesToSql();
                minTimes = 0;
                result = "(\n\"desired_concurrent_number\" = \"1\"\n)\n";
            }
        };
        return job;
    }

    // ===== A job created without kafka_partitions must not print them back. currentKafkaPartitions is
    // runtime state: always empty on a follower FE, where the scheduler never runs (so the DDL carried
    // "kafka_partitions"="", which CREATE ROUTINE LOAD rejects), and every discovered partition on the
    // leader (so re-creating from the DDL would pin the job to them). =====
    @Test
    public void testShowCreateRoutineLoadOmitsUnspecifiedKafkaPartitions() throws Exception {
        KafkaRoutineLoadJob job = kafkaJob();
        // follower FE: nothing discovered yet
        String followerDdl = buildDDL(job);
        // leader FE: the scheduler filled in the topic's partitions
        Deencapsulation.setField(job, "currentKafkaPartitions", Lists.newArrayList(0, 1, 2));
        String leaderDdl = buildDDL(job);

        Assertions.assertEquals(followerDdl, leaderDdl);
        Assertions.assertTrue(leaderDdl.endsWith(
                "FROM KAFKA\n(\n\"kafka_broker_list\"=\"localhost:9092\",\n\"kafka_topic\"=\"topic\"\n);"), leaderDdl);

        // The DDL passes the CREATE ROUTINE LOAD data-source analysis and still pins no partition.
        CreateRoutineLoadStmt stmt = (CreateRoutineLoadStmt) SqlParser.parse(leaderDdl, ctx.getSessionVariable()).get(0);
        stmt.checkDataSourceProperties();
        Assertions.assertEquals("localhost:9092", stmt.getKafkaBrokerList());
        Assertions.assertEquals("topic", stmt.getKafkaTopic());
        Assertions.assertTrue(stmt.getKafkaPartitionOffsets().isEmpty());

        // What a follower FE used to print is rejected by that same analysis.
        String oldFollowerDdl = leaderDdl.replace("\"kafka_topic\"=\"topic\"\n",
                "\"kafka_topic\"=\"topic\",\n\"kafka_partitions\"=\"\"\n");
        CreateRoutineLoadStmt oldStmt =
                (CreateRoutineLoadStmt) SqlParser.parse(oldFollowerDdl, ctx.getSessionVariable()).get(0);
        Assertions.assertThrows(AnalysisException.class, oldStmt::checkDataSourceProperties);
    }

    @Test
    public void testShowCreateRoutineLoadKeepsSpecifiedKafkaPartitions() throws Exception {
        KafkaRoutineLoadJob job = kafkaJob();
        Deencapsulation.setField(job, "customeKafkaPartitionOffsets",
                Lists.newArrayList(Pair.create(1, 100L), Pair.create(0, KafkaProgress.OFFSET_BEGINNING_VAL)));
        Deencapsulation.setField(job, "customKafkaPartitions", Lists.newArrayList(1, 0));
        Deencapsulation.setField(job, "currentKafkaPartitions", Lists.newArrayList(1, 0));

        String ddl = buildDDL(job);
        Assertions.assertTrue(ddl.endsWith("FROM KAFKA\n(\n\"kafka_broker_list\"=\"localhost:9092\",\n"
                + "\"kafka_topic\"=\"topic\",\n\"kafka_partitions\"=\"0,1\",\n"
                + "\"kafka_offsets\"=\"OFFSET_BEGINNING,100\"\n);"), ddl);

        // The DDL parses back to the same partitions and offsets.
        CreateRoutineLoadStmt stmt = (CreateRoutineLoadStmt) SqlParser.parse(ddl, ctx.getSessionVariable()).get(0);
        stmt.checkDataSourceProperties();
        Assertions.assertEquals(
                Lists.newArrayList(Pair.create(0, KafkaProgress.OFFSET_BEGINNING_VAL), Pair.create(1, 100L)),
                stmt.getKafkaPartitionOffsets());
    }

    // ALTER ROUTINE LOAD enabled kafka_partition_discovery on a job created without kafka_partitions: the
    // property alone is rejected by CREATE ROUTINE LOAD, so the DDL leaves it out and stays runnable.
    @Test
    public void testShowCreateRoutineLoadDropsPartitionDiscoveryWithoutPartitions() throws Exception {
        KafkaRoutineLoadJob job = kafkaJob();
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITION_DISCOVERY, "true");
        Deencapsulation.setField(job, "customProperties", customProperties);
        Deencapsulation.setField(job, "currentKafkaPartitions", Lists.newArrayList(0, 1, 2));

        String ddl = buildDDL(job);
        Assertions.assertFalse(ddl.contains("kafka_partition_discovery"), ddl);
        Assertions.assertFalse(ddl.contains("kafka_partitions"), ddl);
        CreateRoutineLoadStmt stmt = (CreateRoutineLoadStmt) SqlParser.parse(ddl, ctx.getSessionVariable()).get(0);
        stmt.checkDataSourceProperties();
        Assertions.assertTrue(stmt.getKafkaPartitionOffsets().isEmpty());
        Assertions.assertFalse(stmt.isKafkaPartitionDiscovery());
    }
}
