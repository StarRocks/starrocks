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

package com.starrocks.catalog;

import com.staros.proto.PlacementPolicy;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.collections.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ColocateTableIndexTest {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndexTest.class);

    @BeforeEach
    public void setUp() {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void teardown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * [
     * [10002.10006, 10002_group1, 10004, 10016, 4, 1, int(11), true],
     * [10026.10030, 10026_group2, 10028, 4, 1, int(11), true]
     * ]
     * ->
     * {
     * 'group1': [10002.10006, 10002_group1, 10004, 10016, 4, 1, int(11), true],
     * 'group2': [10026.10030, 10026_group2, 10028, 4, 1, int(11), true]
     * }
     */
    private Map<String, List<String>> groupByName(List<List<String>> lists) {
        Map<String, List<String>> ret = new HashedMap();
        for (List<String> list : lists) {
            ret.put(list.get(1).split("_")[1], list);
        }
        return ret;
    }

    @Test
    public void testDropTable() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        // create table1_1->group1
        String sql = "CREATE TABLE db1.table1_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        List<List<String>> infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        // group1->table1To1
        Assertions.assertEquals(1, infos.size());
        Map<String, List<String>> map = groupByName(infos);
        Table table1To1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1").getTable("table1_1");
        Assertions.assertEquals(String.format("%d", table1To1.getId()), map.get("group1").get(2));
        LOG.info("after create db1.table1_1: {}", infos);

        // create table1_2->group1
        sql = "CREATE TABLE db1.table1_2 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        // group1 -> table1To1, table1To2
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        Assertions.assertEquals(1, infos.size());
        map = groupByName(infos);
        Table table1To2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1").getTable("table1_2");
        Assertions.assertEquals(String.format("%d, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        LOG.info("after create db1.table1_2: {}", infos);

        // create db2
        createDbStmtStr = "create database db2;";
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group2\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        // group1 -> table1_1, table1_2
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        Assertions.assertEquals(2, infos.size());
        map = groupByName(infos);
        Assertions.assertEquals(String.format("%d, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Table table2To1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2").getTable("table2_1");
        Assertions.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after create db2.table2_1: {}", infos);

        // drop db1.table1_1
        sql = "DROP TABLE db1.table1_1;";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals(String.format("%d*, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after drop db1.table1_1: {}", infos);

        // drop db1.table1_2
        sql = "DROP TABLE db1.table1_2;";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));

        Assertions.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        Assertions.assertEquals(String.format("table2_1", table2To1.getId()), map.get("group2").get(3));

        LOG.info("after drop db1.table1_2: {}", infos);

        // drop db2
        sql = "DROP DATABASE db2;";
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .dropDb(connectContext, dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l*
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals(String.format("%d*", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after drop db2: {}", infos);

        // create & drop db2 again
        createDbStmtStr = "create database db2;";
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_3 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group3\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Table table2To3 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2").getTable("table2_3");
        sql = "DROP DATABASE db2;";
        dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .dropDb(connectContext, dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        LOG.info("after create & drop db2: {}", infos);
        Assertions.assertEquals(3, infos.size());
        Assertions.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals("[deleted], [deleted]", map.get("group1").get(3));
        Assertions.assertEquals(String.format("%d*", table2To1.getId()), map.get("group2").get(2));
        Assertions.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));
        Assertions.assertEquals(String.format("%d*", table2To3.getId()), map.get("group3").get(2));
        Assertions.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));
    }

    @Test
    public void testCleanUp() throws Exception {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create goodDb
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database goodDb;", connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database goodDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("goodDb");
        // create goodtable
        String sql = "CREATE TABLE " +
                "goodDb.goodTable (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"goodGroup\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(goodDb.getFullName(), "goodTable");
        ColocateTableIndex.GroupId goodGroup = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId());

        // create a bad db
        long badDbId = 4000;
        table.id = 4001;
        table.name = "goodTableOfBadDb";
        colocateTableIndex.addTableToGroup(
                badDbId, table, "badGroupOfBadDb", new ColocateTableIndex.GroupId(badDbId, 4002), false);
        // create a bad table in good db
        table.id = 4003;
        table.name = "badTable";
        colocateTableIndex.addTableToGroup(
                goodDb.getId(), table, "badGroupOfBadTable", new ColocateTableIndex.GroupId(goodDb.getId(), 4004), false);

        Map<String, List<String>> map = groupByName(GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos());
        Assertions.assertTrue(map.containsKey("goodGroup"));
        Assertions.assertTrue(map.containsKey("badGroupOfBadDb"));
        Assertions.assertTrue(map.containsKey("badGroupOfBadTable"));

        colocateTableIndex.cleanupInvalidDbOrTable(GlobalStateMgr.getCurrentState());
        map = groupByName(GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos());

        Assertions.assertTrue(map.containsKey("goodGroup"));
        Assertions.assertFalse(map.containsKey("badGroupOfBadDb"));
        Assertions.assertFalse(map.containsKey("badGroupOfBadTable"));
    }

    @Test
    public void testLakeTableColocation(@Mocked LakeTable olapTable, @Mocked StarOSAgent starOSAgent) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        long dbId = 100;
        long dbId2 = 101;
        long tableId = 200;
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new HashDistributionInfo();
            }
        };

        new MockUp<LakeTable>() {
            @Mock
            public List<Long> getShardGroupIds() {
                return new ArrayList<>();
            }
        };

        colocateTableIndex.addTableToGroup(
                dbId, (OlapTable) olapTable, "lakeGroup", new ColocateTableIndex.GroupId(dbId, 10000), false /* isReplay */);
        Assertions.assertTrue(colocateTableIndex.isMetaGroupColocateTable(tableId));
        colocateTableIndex.addTableToGroup(
                dbId2, (OlapTable) olapTable, "lakeGroup", new ColocateTableIndex.GroupId(dbId2, 10000),
                false /* isReplay */);
        Assertions.assertTrue(colocateTableIndex.isMetaGroupColocateTable(tableId));

        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(new ColocateTableIndex.GroupId(dbId, 10000)));
        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(new ColocateTableIndex.GroupId(dbId, 10001)));

        colocateTableIndex.removeTable(tableId, null, false /* isReplay */);
        Assertions.assertFalse(colocateTableIndex.isMetaGroupColocateTable(tableId));
    }

    // Regression test: when addTableToGroup is called with a non-null assignedGroupId
    // for a brand new group (not in groupName2Id), it should call createMetaGroup,
    // not updateMetaGroup. The bug was introduced by the refactoring that splits
    // modifyTableColocate into prepare + apply: prepareModifyTableColocate always
    // generates assignedGroupId via getNextId(), but addTableToGroup misinterprets
    // non-null assignedGroupId as "the group already exists" and calls updateMetaGroup
    // (join) instead of createMetaGroup (create), causing StarOS NOT_EXIST error.
    @Test
    public void testLakeTableNewGroupShouldCallCreateMetaGroup(
            @Mocked LakeTable olapTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        AtomicBoolean createMetaGroupCalled = new AtomicBoolean(false);
        AtomicBoolean updateMetaGroupCalled = new AtomicBoolean(false);

        new MockUp<StarOSAgent>() {
            @Mock
            public void createMetaGroup(long metaGroupId, List<Long> shardGroupIds) {
                createMetaGroupCalled.set(true);
            }

            @Mock
            public void updateMetaGroup(long metaGroupId, List<Long> shardGroupIds, boolean isJoin) {
                updateMetaGroupCalled.set(true);
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        long dbId = 100;
        long tableId = 200;
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new HashDistributionInfo();
            }
        };

        new MockUp<LakeTable>() {
            @Mock
            public List<Long> getShardGroupIds() {
                return Arrays.asList(5001L, 5002L, 5003L);
            }
        };

        // Simulate the modifyTableColocate (ALTER TABLE SET colocate_with) path on sr11073:
        // prepareModifyTableColocate generates a new assignedGroupId via getNextId(),
        // then passes it to addTableToGroup. The group "newGroup" does NOT exist yet.
        ColocateTableIndex.GroupId assignedGroupId = new ColocateTableIndex.GroupId(dbId, 99999);
        colocateTableIndex.addTableToGroup(
                dbId, (OlapTable) olapTable, "newGroup", assignedGroupId, false /* isReplay */);

        // For a brand new group, createMetaGroup should be called, NOT updateMetaGroup
        Assertions.assertTrue(createMetaGroupCalled.get(),
                "createMetaGroup should be called for a brand new colocate group");
        Assertions.assertFalse(updateMetaGroupCalled.get(),
                "updateMetaGroup should NOT be called for a brand new colocate group, " +
                "but it was called due to groupAlreadyExist being incorrectly true " +
                "when assignedGroupId is non-null");
    }

    @Test
    public void testSaveLoadJsonFormatImage() throws Exception {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create goodDb
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils
                .parseStmtWithNewParser("create database db_image;", connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db_image");
        // create goodtable
        String sql = "CREATE TABLE " +
                "db_image.tbl1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"goodGroup\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        colocateTableIndex.saveColocateTableIndexV2(image.getImageWriter());

        ColocateTableIndex followerIndex = new ColocateTableIndex();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        followerIndex.loadColocateTableIndexV2(reader);
        reader.close();
        Assertions.assertEquals(colocateTableIndex.getAllGroupIds(), followerIndex.getAllGroupIds());
        Assertions.assertEquals(colocateTableIndex.getGroup(table.getId()), followerIndex.getGroup(table.getId()));

        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testLoadLegacyImageWithoutColocateRangeMgr(@Mocked SRMetaBlockReader reader) throws Exception {
        ColocateTableIndex legacyIndex = new ColocateTableIndex();
        Deencapsulation.setField(legacyIndex, "colocateRangeMgr", null);

        new Expectations() {
            {
                reader.readJson(ColocateTableIndex.class);
                result = legacyIndex;
            }
        };

        ColocateTableIndex restoredIndex = new ColocateTableIndex();
        restoredIndex.loadColocateTableIndexV2(reader);
        Assertions.assertNotNull(restoredIndex.getColocateRangeMgr());
    }

    @Test
    public void testRangeColocateNotSupportedInSharedNothing() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnableRangeDistribution(true);

        String createDbStmtStr = "create database db_range_colocate;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(
                createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        // Range distribution colocate is only supported in shared-data mode.
        // In shared-nothing mode (non-lake table), it should fail.
        String sql = "CREATE TABLE db_range_colocate.t1 (k1 int, k2 int, k3 varchar(32), v1 int)\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PROPERTIES(\"colocate_with\"=\"rg1:k1,k2\", \"replication_num\" = \"1\");\n";
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertThrows(Exception.class, () -> StarRocksAssert.utCreateTableWithRetry(stmt));
    }

    @Test
    public void testRangeColocateInSharedData(@Mocked LakeTable lakeTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new MockUp<StarOSAgent>() {
            @Mock
            public void createMetaGroup(long metaGroupId, List<Long> shardGroupIds) {
            }

            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                return 9999L;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        long dbId = 100;
        long tableId = 200;
        // Mock a lake table with range distribution and colocate group
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new RangeDistributionInfo();
            }

            @Mock
            public String getColocateGroup() {
                return "rg1:k1,k2";
            }

            @Mock
            public short getDefaultReplicationNum() {
                return 1;
            }
        };

        new MockUp<LakeTable>() {
            @Mock
            public List<Long> getShardGroupIds() {
                return Arrays.asList(5001L);
            }
        };

        // Mock MetaUtils.getRangeColocateColumns to return 2 columns
        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                return Arrays.asList(
                        new Column("k1", com.starrocks.type.IntegerType.INT),
                        new Column("k2", com.starrocks.type.IntegerType.INT));
            }
        };

        // Add lake range colocate table should succeed
        colocateTableIndex.addTableToGroup(
                dbId, (OlapTable) lakeTable, "rg1", null, false /* isReplay */);

        // Verify group was created with RANGE type
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
        Assertions.assertNotNull(groupId);
        ColocateGroupSchema schema = colocateTableIndex.getGroupSchema(groupId);
        Assertions.assertNotNull(schema);
        Assertions.assertTrue(schema.isRangeColocate());
        Assertions.assertEquals(2, schema.getColocateColumnCount());

        // Verify ColocateRangeMgr is initialized
        ColocateRangeMgr rangeMgr = colocateTableIndex.getColocateRangeMgr();
        Assertions.assertTrue(rangeMgr.containsColocateGroup(groupId.grpId));
        List<ColocateRange> ranges = rangeMgr.getColocateRanges(groupId.grpId);
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertTrue(ranges.get(0).getRange().isAll());
    }

    @Test
    public void testRangeColocatePropertyMustMatchExistingGroupSchema(
            @Mocked LakeTable firstTable, @Mocked LakeTable secondTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();
        DistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(1L, (short) 1);

        new Expectations() {
            {
                firstTable.getId();
                result = 200L;
                minTimes = 0;
                secondTable.getId();
                result = 201L;
                minTimes = 0;
                firstTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                secondTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                firstTable.getDefaultDistributionInfo();
                result = rangeDistributionInfo;
                minTimes = 0;
                secondTable.getDefaultDistributionInfo();
                result = rangeDistributionInfo;
                minTimes = 0;
                firstTable.getDefaultReplicationNum();
                result = (short) 1;
                minTimes = 0;
                secondTable.getDefaultReplicationNum();
                result = (short) 1;
                minTimes = 0;
                firstTable.getPartitionInfo();
                result = partitionInfo;
                minTimes = 0;
                secondTable.getPartitionInfo();
                result = partitionInfo;
                minTimes = 0;
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                if (colocateColumnNames != null && colocateColumnNames.size() == 1) {
                    return Arrays.asList(new Column("k1", com.starrocks.type.IntegerType.INT));
                }
                return Arrays.asList(
                        new Column("k1", com.starrocks.type.IntegerType.INT),
                        new Column("k2", com.starrocks.type.IntegerType.INT));
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                return 9999L;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        colocateTableIndex.addTableToGroup(100L, firstTable, "rg1:k1", null, false);
        Assertions.assertThrows(DdlException.class,
                () -> colocateTableIndex.addTableToGroup(100L, secondTable, "rg1:k1,k2", null, false));
    }

    @Test
    public void testRangeCrossDbSchemaCheckUsesRequestedColocateColumns(
            @Mocked LakeTable existingTable, @Mocked LakeTable newTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();
        DistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(1L, (short) 1);

        new Expectations() {
            {
                existingTable.getId();
                result = 300L;
                minTimes = 0;
                newTable.getId();
                result = 301L;
                minTimes = 0;
                existingTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                newTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                existingTable.getDefaultDistributionInfo();
                result = rangeDistributionInfo;
                minTimes = 0;
                newTable.getDefaultDistributionInfo();
                result = rangeDistributionInfo;
                minTimes = 0;
                existingTable.getDefaultReplicationNum();
                result = (short) 1;
                minTimes = 0;
                newTable.getDefaultReplicationNum();
                result = (short) 1;
                minTimes = 0;
                existingTable.getPartitionInfo();
                result = partitionInfo;
                minTimes = 0;
                newTable.getPartitionInfo();
                result = partitionInfo;
                minTimes = 0;
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                if (colocateColumnNames != null && colocateColumnNames.size() == 1) {
                    return Arrays.asList(new Column("k1", com.starrocks.type.IntegerType.INT));
                }
                return Arrays.asList(
                        new Column("k1", com.starrocks.type.IntegerType.INT),
                        new Column("k2", com.starrocks.type.IntegerType.INT));
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                return 9999L;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        colocateTableIndex.addTableToGroup(100L, existingTable, "rg1:k1", null, false);
        Assertions.assertThrows(DdlException.class,
                () -> colocateTableIndex.checkColocateSchemaWithGroupInOtherDb("rg1:k1,k2", 101L, newTable));
    }

    // ========== Tests for public addTableToGroup afterTabletCreation routing ==========

    @Test
    public void testAfterTabletCreationRoutingForNonLakeTable(
            @Mocked Database db, @Mocked OlapTable olapTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new Expectations() {
            {
                db.getId();
                result = 100L;
                minTimes = 0;
                olapTable.isCloudNativeTableOrMaterializedView();
                result = false;
                minTimes = 0;
                olapTable.getDefaultDistributionInfo();
                result = new HashDistributionInfo();
                minTimes = 0;
            }
        };

        // Non-lake table: afterTabletCreation=false should proceed
        Assertions.assertTrue(
                colocateTableIndex.addTableToGroup(db, olapTable, "g1", false /* afterTabletCreation */));
        // Non-lake table: afterTabletCreation=true should be skipped
        Assertions.assertFalse(
                colocateTableIndex.addTableToGroup(db, olapTable, "g1", true /* afterTabletCreation */));
    }

    @Test
    public void testAfterTabletCreationRoutingForHashColocateLakeTable(
            @Mocked Database db, @Mocked LakeTable lakeTable, @Mocked StarOSAgent starOSAgent) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new Expectations() {
            {
                db.getId();
                result = 100L;
                minTimes = 0;
                lakeTable.getId();
                result = 200L;
                minTimes = 0;
                lakeTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                lakeTable.getDefaultDistributionInfo();
                result = new HashDistributionInfo();
                minTimes = 0;
                lakeTable.getShardGroupIds();
                result = new ArrayList<>();
                minTimes = 0;
            }
        };

        // Hash colocate lake table: afterTabletCreation=false should be skipped
        Assertions.assertFalse(
                colocateTableIndex.addTableToGroup(db, lakeTable, "g1", false /* afterTabletCreation */));
        // Hash colocate lake table: afterTabletCreation=true should proceed
        Assertions.assertTrue(
                colocateTableIndex.addTableToGroup(db, lakeTable, "g1", true /* afterTabletCreation */));
    }

    @Test
    public void testAfterTabletCreationRoutingForRangeColocateLakeTable(
            @Mocked Database db, @Mocked LakeTable lakeTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                return 9999L;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                return Arrays.asList(new Column("k1", com.starrocks.type.IntegerType.INT));
            }
        };

        new Expectations() {
            {
                db.getId();
                result = 100L;
                minTimes = 0;
                lakeTable.getId();
                result = 200L;
                minTimes = 0;
                lakeTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                lakeTable.getDefaultDistributionInfo();
                result = new RangeDistributionInfo();
                minTimes = 0;
                lakeTable.getDefaultReplicationNum();
                result = (short) 1;
                minTimes = 0;
            }
        };

        // Range colocate lake table: afterTabletCreation=false should proceed
        Assertions.assertTrue(
                colocateTableIndex.addTableToGroup(db, lakeTable, "rg1:k1", false /* afterTabletCreation */));
        // Verify table was registered
        Assertions.assertNotNull(colocateTableIndex.getGroup(200L));

        // Range colocate lake table: afterTabletCreation=true should be skipped
        Assertions.assertFalse(
                colocateTableIndex.addTableToGroup(db, lakeTable, "rg1:k1", true /* afterTabletCreation */));
    }

    @Test
    public void testReplaySkipsPackShardGroupCreation(@Mocked LakeTable lakeTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        AtomicBoolean createShardGroupCalled = new AtomicBoolean(false);

        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                createShardGroupCalled.set(true);
                return 9999L;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                return Arrays.asList(new Column("k1", com.starrocks.type.IntegerType.INT));
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return 200L;
            }

            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new RangeDistributionInfo();
            }

            @Mock
            public short getDefaultReplicationNum() {
                return 1;
            }
        };

        // Replay should NOT call createShardGroup
        colocateTableIndex.addTableToGroup(
                100L, (OlapTable) lakeTable, "rg1:k1", null, true /* isReplay */);
        Assertions.assertFalse(createShardGroupCalled.get(),
                "createShardGroup(PACK) should NOT be called during replay");

        // Verify group was created but ColocateRangeMgr was NOT initialized
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(200L);
        Assertions.assertNotNull(groupId);
        Assertions.assertFalse(colocateTableIndex.getColocateRangeMgr().containsColocateGroup(groupId.grpId));
    }

    @Test
    public void testRemoveTableSkipsMetaGroupForRangeColocate(@Mocked LakeTable lakeTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        AtomicBoolean updateMetaGroupCalled = new AtomicBoolean(false);

        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                return 9999L;
            }

            @Mock
            public void updateMetaGroup(long metaGroupId, List<Long> shardGroupIds, boolean isJoin) {
                updateMetaGroupCalled.set(true);
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                return Arrays.asList(new Column("k1", com.starrocks.type.IntegerType.INT));
            }
        };

        long tableId = 200L;
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new RangeDistributionInfo();
            }

            @Mock
            public short getDefaultReplicationNum() {
                return 1;
            }
        };

        new MockUp<LakeTable>() {
            @Mock
            public List<Long> getShardGroupIds() {
                return Arrays.asList(5001L);
            }
        };

        // Add range colocate table
        colocateTableIndex.addTableToGroup(
                100L, (OlapTable) lakeTable, "rg1:k1", null, false /* isReplay */);
        Assertions.assertNotNull(colocateTableIndex.getGroup(tableId));

        // Remove table — should NOT call updateMetaGroup for range colocate
        colocateTableIndex.removeTable(tableId, (OlapTable) lakeTable, false /* isReplay */);
        Assertions.assertFalse(updateMetaGroupCalled.get(),
                "updateMetaGroup should NOT be called when removing a range colocate table");
    }

    @Test
    public void testUpdateLakeTableColocationInfoSkipsRangeColocate(@Mocked LakeTable lakeTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        AtomicBoolean updateMetaGroupCalled = new AtomicBoolean(false);

        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                return 9999L;
            }

            @Mock
            public void updateMetaGroup(long metaGroupId, List<Long> shardGroupIds, boolean isJoin) {
                updateMetaGroupCalled.set(true);
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                return Arrays.asList(new Column("k1", com.starrocks.type.IntegerType.INT));
            }
        };

        long tableId = 200L;
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new RangeDistributionInfo();
            }

            @Mock
            public short getDefaultReplicationNum() {
                return 1;
            }
        };

        new MockUp<LakeTable>() {
            @Mock
            public List<Long> getShardGroupIds() {
                return Arrays.asList(5001L);
            }
        };

        // Add range colocate table
        colocateTableIndex.addTableToGroup(
                100L, (OlapTable) lakeTable, "rg1:k1", null, false /* isReplay */);

        // updateLakeTableColocationInfo should skip range colocate
        colocateTableIndex.updateLakeTableColocationInfo(
                (OlapTable) lakeTable, true /* isJoin */, null);
        Assertions.assertFalse(updateMetaGroupCalled.get(),
                "updateMetaGroup should NOT be called for range colocate table");
    }

    @Test
    public void testConstructMetaGroupsSkipsRangeColocate(@Mocked LakeTable lakeTable) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId,
                                          long indexId, PlacementPolicy placementPolicy) {
                return 9999L;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeColocateColumns(OlapTable olapTable, List<String> colocateColumnNames) {
                return Arrays.asList(new Column("k1", com.starrocks.type.IntegerType.INT));
            }
        };

        long tableId = 200L;
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new RangeDistributionInfo();
            }

            @Mock
            public short getDefaultReplicationNum() {
                return 1;
            }
        };

        // Add range colocate table
        colocateTableIndex.addTableToGroup(
                100L, (OlapTable) lakeTable, "rg1:k1", null, false /* isReplay */);
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
        Assertions.assertNotNull(groupId);

        // Range colocate table should NOT be in metaGroups
        Assertions.assertFalse(colocateTableIndex.isMetaGroupColocateTable(tableId));

        // constructMetaGroups is private, invoked by loadColocateTableIndexV2.
        // Verify via isGroupUnstable: for non-metaGroups, it checks unstableGroups (not MetaGroup).
        // Since we didn't mark it unstable, it should return false.
        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId));

        // New in PR-1: isRangeColocateGroup distinguishes range vs hash groups.
        Assertions.assertTrue(colocateTableIndex.isRangeColocateGroup(groupId));
    }

    @Test
    public void testIsRangeColocateGroupForUnknownGroup() {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();
        ColocateTableIndex.GroupId unknown = new ColocateTableIndex.GroupId(1L, 2L);
        Assertions.assertFalse(colocateTableIndex.isRangeColocateGroup(unknown));
    }
}
