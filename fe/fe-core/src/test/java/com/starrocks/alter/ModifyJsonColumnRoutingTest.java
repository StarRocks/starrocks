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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Routing tests for MODIFY COLUMN on a shared-data table: which conversions may skip the data
 * rewrite (fast schema evolution) and which may not.
 *
 * <p>JSON -> VARCHAR must not. The stored JSON text can be longer than the target VARCHAR, and the
 * length check only runs while data is rewritten; skipping the rewrite leaves oversized values behind
 * an undersized schema, and every later compaction of that tablet fails with
 * "string length(N) > limit(M)" forever. The fast path used to be gated only on
 * {@link com.starrocks.catalog.SchemaChangeTypeCompatibility#canReuseZonemapIndex}, which returns
 * true for any source type that has no zonemap at all -- JSON among them -- so "the zonemap can be
 * reused" was silently answering "the data can stay as it is".
 */
public class ModifyJsonColumnRoutingTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    private static SchemaChangeHandler handler() {
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
    }

    private static Database db() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
    }

    /**
     * Analyze the ALTER without running the resulting job. A rewrite is required when this returns a
     * {@link LakeTableSchemaChangeJob}; the fast path returns either a
     * {@link LakeTableAsyncFastSchemaChangeJob} (v2 off) or null, having already updated the catalog
     * in place (v2 on).
     */
    private static AlterJobV2 createJob(String tableName, String alterSql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        return handler().analyzeAndCreateJob(stmt.getAlterClauseList(), db(), table(tableName));
    }

    private static void createTable(String name, String columns, boolean fastSchemaEvolutionV2) throws Exception {
        starRocksAssert.withTable("create table " + name + " (" + columns + ")\n"
                + "duplicate key(k)\n"
                + "distributed by hash(k) buckets 1\n"
                + "properties('replication_num' = '1',\n"
                + "  'fast_schema_evolution' = 'true',\n"
                + "  'cloud_native_fast_schema_evolution_v2' = '" + fastSchemaEvolutionV2 + "');");
    }

    // JSON -> VARCHAR routes to the rewrite job, which is where the length check lives.
    @Test
    public void testJsonToVarcharRoutesToRewriteJob() throws Exception {
        createTable("t_json_v1", "k int, j json", false);
        AlterJobV2 job = createJob("t_json_v1", "alter table t_json_v1 modify column j varchar(1024)");
        assertInstanceOf(LakeTableSchemaChangeJob.class, job);
    }

    // Same with fast schema evolution v2, where the fast path produces no job at all: the regression
    // shows up as a null return (catalog updated in place, data untouched and unvalidated).
    @Test
    public void testJsonToVarcharRoutesToRewriteJobWithFastSchemaEvolutionV2() throws Exception {
        createTable("t_json_v2", "k int, j json", true);
        AlterJobV2 job = createJob("t_json_v2", "alter table t_json_v2 modify column j varchar(1024)");
        assertNotNull(job, "JSON -> VARCHAR must not be answered by fast schema evolution");
        assertInstanceOf(LakeTableSchemaChangeJob.class, job);
    }

    // Widening a VARCHAR is still resolved without a rewrite.
    @Test
    public void testVarcharWidenStaysOnFastPath() throws Exception {
        createTable("t_varchar", "k int, v varchar(10)", false);
        AlterJobV2 job = createJob("t_varchar", "alter table t_varchar modify column v varchar(100)");
        assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job);
    }

    // So is widening an integer.
    @Test
    public void testIntWidenStaysOnFastPath() throws Exception {
        createTable("t_int", "k int, v int", false);
        AlterJobV2 job = createJob("t_int", "alter table t_int modify column v bigint");
        assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job);
    }

    // The reverse direction was never on the fast path (VARCHAR has a zonemap and the matrix has no
    // VARCHAR -> JSON entry) and stays off it.
    @Test
    public void testVarcharToJsonRoutesToRewriteJob() throws Exception {
        createTable("t_varchar_json", "k int, v varchar(10)", false);
        AlterJobV2 job = createJob("t_varchar_json", "alter table t_varchar_json modify column v json");
        assertInstanceOf(LakeTableSchemaChangeJob.class, job);
    }
}
