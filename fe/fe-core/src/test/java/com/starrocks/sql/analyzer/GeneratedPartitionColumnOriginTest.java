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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeneratedPartitionColumnOriginTest {
    private static final String PREFIX = "__generated_partition_column_";
    private static final String PROPERTIES =
            " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')";

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testUserPartitionColumnCannotAcquireInternalOrigin() {
        boolean original = Config.allow_system_reserved_names;
        try {
            for (boolean allowReservedNames : new boolean[] {false, true}) {
                Config.allow_system_reserved_names = allowReservedNames;
                for (String suffix : new String[] {"x", "0"}) {
                    String column = PREFIX + suffix;
                    analyzeFail("CREATE TABLE origin_user (k BIGINT, v BIGINT SUM, " + column +
                                    " BIGINT AS k + 1) AGGREGATE KEY(k) PARTITION BY (" + column + ")" + PROPERTIES,
                            "Generated Column does not support AGG table");
                }
            }
        } finally {
            Config.allow_system_reserved_names = original;
        }
    }

    @Test
    public void testUserRandomPartitionColumnRejected() {
        analyzeFail("CREATE TABLE origin_rand (k BIGINT, v BIGINT SUM, " + PREFIX +
                        "x BIGINT AS k + CAST(rand() * 2 AS BIGINT)) AGGREGATE KEY(k) PARTITION BY (" +
                        PREFIX + "x)" + PROPERTIES,
                "partition expr should be deterministic");
    }

    @Test
    public void testMaterializerIsTheOriginForExplicitAndInferredAggregateTables() {
        for (String keys : new String[] {" AGGREGATE KEY(k)", ""}) {
            CreateTableStmt stmt = (CreateTableStmt) analyzeSuccess(
                    "CREATE TABLE origin_internal (k BIGINT, v BIGINT SUM)" + keys +
                            " PARTITION BY abs(k)" + PROPERTIES);
            assertTrue(stmt.getColumnDefs().get(2).isInternalGeneratedPartitionColumn());
            Column column = stmt.getColumns().get(2);
            assertTrue(column.isInternalGeneratedPartitionColumn());
            assertFalse(column.isKey());
            assertEquals(AggregateType.REPLACE, column.getAggregationType());
        }
    }

    @Test
    public void testUserGeneratedDuplicatePartitionColumnIsNotInternal() {
        CreateTableStmt stmt = (CreateTableStmt) analyzeSuccess(
                "CREATE TABLE origin_dup (k BIGINT, v BIGINT, " + PREFIX +
                        "x BIGINT AS k + 1) DUPLICATE KEY(k) PARTITION BY (" + PREFIX + "x)" + PROPERTIES);
        assertFalse(stmt.getColumnDefs().get(2).isInternalGeneratedPartitionColumn());
        assertFalse(stmt.getColumns().get(2).isInternalGeneratedPartitionColumn());
    }

    @Test
    public void testLikeAndDdlReplayRematerializeInternalOrigin() throws Exception {
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        String tableName = "origin_like_source";
        starRocksAssert.withTable("CREATE TABLE " + tableName +
                " (k BIGINT, v BIGINT SUM) AGGREGATE KEY(k) PARTITION BY abs(k)" + PROPERTIES);
        try {
            CreateTableLikeStmt like = (CreateTableLikeStmt) analyzeSuccess(
                    "CREATE TABLE origin_like_target LIKE " + tableName);
            assertTrue(like.getCreateTableStmt().getColumns().get(2).isInternalGeneratedPartitionColumn());
            OlapTable table = (OlapTable) starRocksAssert.getTable("test", tableName);
            List<String> ddl = new ArrayList<>();
            AstToStringBuilder.getDdlStmt("test", table, ddl, null, null, false, false, false);
            String replayDdl = ddl.get(0).replace("`" + tableName + "`", "`origin_like_replay`");
            CreateTableStmt replay = (CreateTableStmt) analyzeSuccess(replayDdl);
            assertEquals("origin_like_replay", replay.getTableName());
            assertTrue(replay.getColumns().get(2).isInternalGeneratedPartitionColumn());
            assertEquals(AggregateType.REPLACE, replay.getColumns().get(2).getAggregationType());
        } finally {
            starRocksAssert.dropTable(tableName);
        }
    }
}
