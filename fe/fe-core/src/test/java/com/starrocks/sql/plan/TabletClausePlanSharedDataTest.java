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
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The TABLET clause cases in a shared-data cluster. Here a table created without DISTRIBUTED BY is range distributed,
 * so every case also runs on such tables, and a bookmark can scope a read to the table as it was when the bookmark was
 * taken.
 */
public class TabletClausePlanSharedDataTest extends TabletClausePlanTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        startCluster(RunMode.SHARED_DATA);
        createTables();
        // Range distribution is the shared-data default. Asking for it on the session too keeps these tables range
        // distributed whatever the cluster config says.
        connectContext.getSessionVariable().setEnableRangeDistribution(true);
        createPartitionedTables("t_dup_range", "t_pk_range", "");
        List<String> rangeDistributedTables = List.of("t_dup_range", "t_pk_range");
        for (String name : rangeDistributedTables) {
            assertEquals("RANGE", table(name).getDefaultDistributionInfo().getType().name(), name);
        }
        starRocksAssert.withTable("CREATE TABLE t_bookmark (dt date NOT NULL, k bigint NOT NULL, v bigint NULL) " +
                "DUPLICATE KEY(dt, k)\n" +
                "PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2025-02-01'))\n" +
                "DISTRIBUTED BY HASH(k) BUCKETS 1");
        markLoaded(table("t_bookmark").getPartition("p1"));
        useProductionPruning();
    }

    // A read scoped to a bookmark reads the table as it was then, so its tablets are checked against that table: a
    // partition added since is not one of the partitions it reads.
    @Test
    public void testBookmarkReadChecksTabletsAgainstTheTableAsOfTheBookmark() throws Exception {
        OlapTable table = table("t_bookmark");
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB).getId();
        // CREATE TABLE leaves the database id unset; a query sets it before a bookmark is resolved.
        table.maySetDatabaseId(dbId);
        Bookmark bookmark = GlobalStateMgr.getCurrentState().getBookmarkManager()
                .create(dbId, table.getId(), BookmarkHolder.forEmptyInfo("tablet-clause"));
        starRocksAssert.alterTable("ALTER TABLE t_bookmark ADD PARTITION p2 VALUES LESS THAN ('2025-03-01')");
        markLoaded(table("t_bookmark").getPartition("p2"));

        long p1 = tabletOf("t_bookmark", "p1");
        long p2 = tabletOf("t_bookmark", "p2");
        String asOfBookmark = " [_BOOKMARK_" + bookmark.getBookmarkId() + "_]";
        assertAll(
                reads("SELECT k FROM t_bookmark TABLET (" + p2 + ")", "t_bookmark", List.of(p2)),
                reads("SELECT k FROM t_bookmark TABLET (" + p1 + ")" + asOfBookmark, "t_bookmark", List.of(p1)),
                fails("SELECT k FROM t_bookmark TABLET (" + p2 + ")" + asOfBookmark, noSuchTablet(p2, "t_bookmark")));
    }
}
