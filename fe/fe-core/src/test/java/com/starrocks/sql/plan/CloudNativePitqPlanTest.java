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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CloudNativePitqPlanTest {

    private static final String DB = "pitq_db";
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static long dbId;

    @BeforeAll
    public static void boot() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        CreateDbStmt createDb = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(
                "create database " + DB, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDb.getFullDbName());
        dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB).getId();
        connectContext.setDatabase(DB);
        starRocksAssert.withTable("create table cn (k bigint, v bigint) "
                + "duplicate key(k) distributed by hash(k) buckets 1 "
                + "properties ('replication_num' = '1')");
        // The test harness wraps each plan SQL as a CREATE VIEW to validate view
        // round-trips; FOR VERSION AS OF is rejected on views, so disable that probe.
        FeConstants.unitTestView = false;
    }

    @AfterAll
    public static void teardown() {
        FeConstants.unitTestView = true;
    }

    @Test
    public void testScopedTablePlan() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(DB).getTable("cn");
        // LocalMetastore.createTable does not stamp dbId; production callers
        // (QueryAnalyzer) populate it before the resolver runs, so seed it for
        // the bookmark registration step here.
        table.maySetDatabaseId(dbId);
        Bookmark bookmark = GlobalStateMgr.getCurrentState().getBookmarkManager()
                .create(dbId, table.getId(), BookmarkHolder.forEmptyInfo("plan-test"));

        String plan = UtFrameUtils.getFragmentPlan(connectContext,
                "select * from cn [_BOOKMARK_" + bookmark.getBookmarkId() + "_]");
        assertTrue(plan.contains("OlapScanNode"),
                "plan should include the OlapScan node:\n" + plan);
    }

    @Test
    public void testUnknownBookmark() {
        long unknownBookmarkId = 999_999_999L;
        SemanticException ex = assertThrows(SemanticException.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext,
                        "select * from cn [_BOOKMARK_" + unknownBookmarkId + "_]"));
        assertTrue(ex.getMessage().contains("bookmark " + unknownBookmarkId + " not found"),
                ex.getMessage());
    }

    @Test
    public void testHintGate() {
        // FOR VERSION AS OF on cloud-native OlapTable returns the pre-PITQ error,
        // because OlapTable.isTemporal() is false and bookmark uses a hint instead.
        StarRocksPlannerException asOfRejected = assertThrows(StarRocksPlannerException.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext,
                        "select * from cn FOR VERSION AS OF 1"));
        assertTrue(asOfRejected.getMessage().contains("Unsupported table type for temporal clauses"),
                asOfRejected.getMessage());

        // Malformed bookmark id (non-numeric) is rejected at parse time.
        SemanticException badId = assertThrows(SemanticException.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext,
                        "select * from cn [_BOOKMARK_abc_]"));
        assertTrue(badId.getMessage().contains("invalid bookmark hint format"),
                badId.getMessage());

        // Bookmark hint combined with _META_ is rejected.
        SemanticException combo = assertThrows(SemanticException.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext,
                        "select * from cn [_BOOKMARK_1_, _META_]"));
        assertTrue(combo.getMessage().contains("bookmark hint cannot combine"),
                combo.getMessage());
    }
}
