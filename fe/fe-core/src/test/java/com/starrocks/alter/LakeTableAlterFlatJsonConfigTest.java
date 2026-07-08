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
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Durability of the cloud-native (lake) flat_json ALTER applied by
 * {@link LakeTableAlterMetaJob#updateCatalog}.
 *
 * The config applied to a cloud-native table by the alter-meta job must be written into the
 * GSON-serialized {@code properties} map. {@code TableProperty.flatJsonConfig} carries no
 * {@code @SerializedName} and is rebuilt from {@code properties} via {@code buildFlatJsonConfig()}
 * in {@code gsonPostProcess()}; if the job only set the transient field, the config (enable /
 * factors / version) would be silently lost once the async job is GC'd and a new FE image is
 * written. These tests reconstruct a {@link TableProperty} from only the serialized properties to
 * simulate an FE restart and assert the config — including its version — is recovered.
 */
public class LakeTableAlterFlatJsonConfigTest {

    private LakeTable newLakeTable(long tableId) {
        Column c0 = new Column("c0", IntegerType.INT, true);
        DistributionInfo dist = new HashDistributionInfo(1, Collections.singletonList(c0));
        PartitionInfo partitionInfo = new RangePartitionInfo(Collections.singletonList(c0));
        LakeTable table = new LakeTable(tableId, "t_flat_json", Collections.singletonList(c0),
                KeysType.DUP_KEYS, partitionInfo, dist);
        table.setTableProperty(new TableProperty(new HashMap<>()));
        return table;
    }

    private FlatJsonConfig newConfig(boolean enable, long version) {
        FlatJsonConfig cfg = new FlatJsonConfig();
        cfg.setFlatJsonEnable(enable);
        cfg.setFlatJsonNullFactor(0.1);
        cfg.setFlatJsonSparsityFactor(0.8);
        cfg.setFlatJsonColumnMax(90);
        cfg.setVersion(version);
        return cfg;
    }

    private static FlatJsonConfig reloadFromImage(TableProperty source) throws Exception {
        // Rebuild a TableProperty from only the serialized `properties` map, the way an FE image
        // reload would, and let gsonPostProcess() reconstruct the transient flat_json config.
        TableProperty reloaded = new TableProperty(new HashMap<>(source.getProperties()));
        reloaded.gsonPostProcess();
        return reloaded.getFlatJsonConfig();
    }

    @Test
    public void testFlatJsonConfigSurvivesFeImageReload() throws Exception {
        Database db = new Database(1L, "db_flat_json");
        LakeTable table = newLakeTable(1001L);
        FlatJsonConfig cfg = newConfig(true, 1L);

        LakeTableAlterMetaJob job = new LakeTableAlterMetaJob(
                10L, db.getId(), table.getId(), table.getName(), 3600000L, cfg);
        // updateCatalog runs both on the leader at job finish and on journal replay.
        job.updateCatalog(db, table, false);

        // In-memory transient field is set.
        Assertions.assertNotNull(table.getFlatJsonConfig());
        Assertions.assertTrue(table.getFlatJsonConfig().getFlatJsonEnable());

        // Durability: the config must be in the serialized `properties` map so it survives a restart.
        Map<String, String> persisted = table.getTableProperty().getProperties();
        Assertions.assertTrue(persisted.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE),
                "flat_json config must be persisted into the table properties map to survive FE restart");
        Assertions.assertTrue(persisted.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_VERSION));

        // Simulate an FE restart and verify the config (incl. version) is recovered.
        FlatJsonConfig recovered = reloadFromImage(table.getTableProperty());
        Assertions.assertNotNull(recovered, "flat_json config must survive an FE image reload");
        Assertions.assertEquals(cfg.getVersion(), recovered.getVersion());
        Assertions.assertTrue(recovered.getFlatJsonEnable());
        Assertions.assertEquals(0.8, recovered.getFlatJsonSparsityFactor(), 1e-9);
        Assertions.assertEquals(90, recovered.getFlatJsonColumnMax());
    }

    @Test
    public void testFlatJsonConfigReplayPersistsToProperties() throws Exception {
        Database db = new Database(2L, "db_flat_json_replay");
        LakeTable table = newLakeTable(2001L);
        FlatJsonConfig cfg = newConfig(true, 3L);

        LakeTableAlterMetaJob job = new LakeTableAlterMetaJob(
                11L, db.getId(), table.getId(), table.getName(), 3600000L, cfg);
        job.updateCatalog(db, table, true);

        Map<String, String> persisted = table.getTableProperty().getProperties();
        Assertions.assertEquals("3", persisted.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_VERSION),
                "replayed flat_json config must persist its version into the properties map");

        FlatJsonConfig recovered = reloadFromImage(table.getTableProperty());
        Assertions.assertNotNull(recovered);
        Assertions.assertEquals(3L, recovered.getVersion());
    }

    /**
     * The analyzer's "factors require flat_json.enable=true" check runs without the table lock,
     * so a concurrent ALTER can disable flat_json between analysis and execution. The lake
     * branch of createAlterMetaJob must re-validate under the lock, or it would persist and
     * propagate a disabled config carrying factor changes (which toProperties() then drops on
     * image save, diverging in-memory state from replay/failover).
     */
    @Test
    public void testCreateAlterMetaJobRejectsFactorOnDisabledConfig() {
        Map<String, String> factorValues = new HashMap<>();
        factorValues.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.2");
        factorValues.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.5");
        factorValues.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");

        long tableId = 3001L;
        for (Map.Entry<String, String> factor : factorValues.entrySet()) {
            Database db = new Database(3L, "db_flat_json_revalidate");
            LakeTable table = newLakeTable(tableId++);
            // The table's config was disabled after the (unlocked) analyzer check passed.
            table.getTableProperty().setFlatJsonConfig(newConfig(false, 2L));

            Map<String, String> properties = new HashMap<>();
            properties.put(factor.getKey(), factor.getValue());
            ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);

            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> new SchemaChangeHandler().createAlterMetaJob(clause, db, table),
                    factor.getKey() + " must be rejected on a disabled config");
            Assertions.assertTrue(e.getMessage().contains("must be set after enabling flat JSON"),
                    "unexpected message: " + e.getMessage());
            // The stale config must remain untouched: no factor applied, no version bump.
            Assertions.assertEquals(2L, table.getFlatJsonConfig().getVersion());
            Assertions.assertEquals(0.1, table.getFlatJsonConfig().getFlatJsonNullFactor(), 1e-9);
        }
    }

    @Test
    public void testCreateAlterMetaJobAcceptsFactorOnEnabledConfig() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public long getNextId() {
                return 12345L;
            }
        };
        Database db = new Database(4L, "db_flat_json_enabled");
        LakeTable table = newLakeTable(4001L);
        table.getTableProperty().setFlatJsonConfig(newConfig(true, 2L));

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.2");
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);

        AlterJobV2 job = new SchemaChangeHandler().createAlterMetaJob(clause, db, table);
        Assertions.assertNotNull(job);
    }
}
