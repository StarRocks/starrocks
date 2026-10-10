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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage for {@code ALTER MATERIALIZED VIEW ... SET ("datacache.partition_duration" = ...)}, which is
 * only meaningful on a cloud-native MV (the create path gates it the same way, see
 * {@code PropertyAnalyzer#analyzeMVProperties}). Uses the lightweight no-mincluster fixture of
 * {@link AlterMVJobExecutorTest}: a real {@link LakeMaterializedView} plus
 * {@code UtFrameUtils.setUpForPersistTest()} for the pseudo edit log.
 */
public class AlterMVDataCachePartitionDurationTest {
    private static final String DB_NAME = "test_alter_mv_datacache_duration";
    private static final long DB_ID = 30001L;
    private static final long MV_ID = 30002L;
    private static final long BASE_META_ID = 30003L;
    private static final String MV_NAME = "lake_mv";

    private ConnectContext connectContext;
    private Database db;
    private MaterializedView mv;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        db = new Database(DB_ID, DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(db);

        mv = createLakeMv(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static List<Column> mvColumns() {
        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", IntegerType.INT);
        k1.setIsKey(true);
        k1.setUniqueId(0);
        columns.add(k1);
        Column v1 = new Column("v1", IntegerType.INT);
        v1.setUniqueId(1);
        columns.add(v1);
        return columns;
    }

    private static MaterializedView createLakeMv(long mvId, String mvName) {
        List<Column> columns = mvColumns();
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Collections.singletonList(columns.get(0)));
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);

        MaterializedView lakeMv = new LakeMaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        lakeMv.setIndexMeta(BASE_META_ID, mvName, columns, 0, 0, (short) columns.size(),
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        lakeMv.setBaseIndexMetaId(BASE_META_ID);
        return lakeMv;
    }

    private static MaterializedView createOlapMv(long mvId, String mvName) {
        List<Column> columns = mvColumns();
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Collections.singletonList(columns.get(0)));
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);

        MaterializedView olapMv = new MaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        olapMv.setIndexMeta(BASE_META_ID, mvName, columns, 0, 0, (short) columns.size(),
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapMv.setBaseIndexMetaId(BASE_META_ID);
        return olapMv;
    }

    private void alterPartitionDuration(String mvName, String value) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, value);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                new TableRef(QualifiedName.of(List.of(DB_NAME, mvName)), null, NodePosition.ZERO),
                new ModifyTablePropertiesClause(properties),
                NodePosition.ZERO);
        new AlterMVJobExecutor().process(stmt, connectContext);
    }

    @Test
    public void testAlterOnCloudNativeMvAndFollowerReplay() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        alterPartitionDuration(MV_NAME, "7 day");

        // Leader: the parsed field is rebuilt and the persisted string is normalized ("7 day" -> "7 days").
        assertEquals("7 days",
                TimeUtils.toHumanReadableString(mv.getTableProperty().getDataCachePartitionDuration()));
        assertEquals("7 days", mv.getTableProperty().getProperties()
                .get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION));

        // EditLog carries the same normalized string the leader holds, so the follower cannot diverge.
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
        assertNotNull(replayInfo);
        assertEquals(DB_ID, replayInfo.getDbId());
        assertEquals(MV_ID, replayInfo.getTableId());
        assertEquals("7 days",
                replayInfo.getProperties().get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION));

        // Follower replay must rebuild the parsed PeriodDuration, which only happens if
        // TableProperty#buildMvProperties calls buildDataCachePartitionDuration().
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        MaterializedView followerMv = createLakeMv(MV_ID, MV_NAME);
        followerDb.registerTableUnlocked(followerMv);

        followerMetastore.replayAlterMaterializedViewProperties(replayInfo);

        MaterializedView replayed = (MaterializedView) followerDb.getTable(MV_ID);
        assertNotNull(replayed);
        assertTrue(replayed.isActive(), "replay must not inactivate the mv");
        assertNotNull(replayed.getTableProperty().getDataCachePartitionDuration(),
                "follower must rebuild the parsed datacache.partition_duration");
        assertEquals(mv.getTableProperty().getDataCachePartitionDuration(),
                replayed.getTableProperty().getDataCachePartitionDuration());
    }

    @Test
    public void testInvalidDurationRejected() {
        SemanticException ex = assertThrows(SemanticException.class,
                () -> alterPartitionDuration(MV_NAME, "abc"));
        assertTrue(ex.getMessage().contains("Cannot parse text to Duration"),
                "expected a parse-failure message, got: " + ex.getMessage());
    }

    @Test
    public void testRejectedOnNonCloudNativeMv() {
        String olapMvName = "olap_mv";
        db.registerTableUnlocked(createOlapMv(MV_ID + 1, olapMvName));

        SemanticException ex = assertThrows(SemanticException.class,
                () -> alterPartitionDuration(olapMvName, "7 day"));
        assertTrue(ex.getMessage().contains("only supported for cloud native materialized view"),
                "expected cloud-native-only message, got: " + ex.getMessage());
    }
}
