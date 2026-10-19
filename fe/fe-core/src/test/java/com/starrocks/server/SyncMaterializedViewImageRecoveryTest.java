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

package com.starrocks.server;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SyncMaterializedViewDefinition;
import com.starrocks.catalog.SyncMaterializedViewRecoveryException;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the actual loadImage post-load catch policy with real registered, syntax-restored tables. */
public class SyncMaterializedViewImageRecoveryTest {
    private static final long MV_ID = 100;
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static GlobalStateMgr state;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(context);
        starRocksAssert.withDatabase("sync_image_recovery").useDatabase("sync_image_recovery");
        state = GlobalStateMgr.getCurrentState();
        db = state.getLocalMetastore().getDb("sync_image_recovery");
    }

    @AfterEach
    public void removeTables() {
        for (com.starrocks.catalog.Table table : db.getTables()) {
            db.dropTable(table.getId());
        }
    }

    @Test
    public void testInvalidAuthoritativeDefinitionFailsImagePostLoad() throws Exception {
        OlapTable recovered = recoveredTable("invalid_new", "missing + 1", true);
        assertEquals(MaterializedIndex.IndexState.NORMAL,
                recovered.getPhysicalPartitions().iterator().next().getLatestIndex(MV_ID).getState());
        // Syntax restoration succeeded; semantic recovery must escape BOTH startup catches.
        assertNotNull(recovered.getIndexMetaByMetaId(MV_ID).getSchema().get(1).getDefineExpr());
        SyncMaterializedViewRecoveryException failure =
                assertThrows(SyncMaterializedViewRecoveryException.class, state::postLoadImageWithRecoveryPolicy);
        assertTrue(failure.getMessage().contains("invalid_new"));
        assertTrue(failure.getMessage().contains("100"));
        assertTrue(failure.getMessage().contains("missing"));
    }

    @Test
    public void testValidAuthoritativeDefinitionAnalyzedBeforeImagePostLoadSucceeds() throws Exception {
        OlapTable recovered = recoveredTable("valid_new", "v + 1", true);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
        List<SlotRef> slots = new ArrayList<>();
        recovered.getIndexMetaByMetaId(MV_ID).getSchema().get(1).getDefineExpr().collect(SlotRef.class, slots);
        assertEquals(1, slots.size());
        assertEquals(IntegerType.INT, slots.get(0).getType());
        assertEquals(OlapTable.OlapTableState.NORMAL, recovered.getState());
    }

    @Test
    public void testLegacySemanticFailureContinuesImagePostLoad() throws Exception {
        OlapTable recovered = recoveredTable("invalid_legacy", "missing + 1", false);
        assertNotNull(recovered.getIndexMetaByMetaId(MV_ID).getSchema().get(1).getDefineExpr());
        OlapTable valid = recoveredTable("valid_sibling", "v + 1", true);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
        List<SlotRef> slots = new ArrayList<>();
        valid.getIndexMetaByMetaId(MV_ID).getSchema().get(1).getDefineExpr().collect(SlotRef.class, slots);
        assertEquals(IntegerType.INT, slots.get(0).getType());
        assertEquals(OlapTable.OlapTableState.NORMAL, recovered.getState());
    }

    @Test
    public void testDetachedDefinitionValidationDoesNotMarkImageFailure() throws Exception {
        OlapTable recovered = recoveredTable("invalid_detached", "missing + 1", true);
        SemanticException failure = assertThrows(SemanticException.class,
                () -> recovered.analyzeRollupIndexMeta(recovered.getIndexMetaByMetaId(MV_ID)));
        assertEquals(SemanticException.class, failure.getClass());
    }

    @Test
    public void testUnmarkedPostLoadSemanticFailureRemainsBestEffort() throws Exception {
        recoveredTable("valid_unmarked", "v + 1", true);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public void processMvRelatedMeta() {
                throw new SemanticException("legacy post-load failure");
            }
        };
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    private static OlapTable recoveredTable(String name, String expression, boolean authoritative) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + name + " (k INT, v INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTable(name);
        Column projected = new Column("mv_projected", IntegerType.INT);
        projected.setDefineExpr(SqlParser.parseSqlToExpr(expression, SqlModeHelper.MODE_DEFAULT));
        table.setIndexMeta(MV_ID, "definition_mv", List.of(new Column(table.getBaseSchema().get(0)), projected),
                0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS,
                authoritative ? null : new OriginStatementInfo("CREATE MATERIALIZED VIEW definition_mv AS SELECT k, "
                        + expression + " AS projected FROM " + name, 0));
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(MV_ID);
        if (authoritative) {
            meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(meta.getSchema(), null));
        }
        for (PhysicalPartition physical : table.getPhysicalPartitions()) {
            physical.createRollupIndex(new MaterializedIndex(MV_ID, MaterializedIndex.IndexState.NORMAL));
        }
        table.rebuildFullSchema();
        OlapTable recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        db.dropTable(table.getId());
        assertTrue(db.registerTableUnlocked(recovered));
        return recovered;
    }
}
