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

import com.starrocks.catalog.Table;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.paimon.PaimonScanRangeParams;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonAnnPlanTest extends PlanTestBase {
    @Test
    void bindsUnfilteredSqlAtFinalScanPlanning(@TempDir Path directory) throws Exception {
        String catalogName = "paimon_ann_planning";
        String warehouse = directory.toUri().toString();
        var paimonPath = new org.apache.paimon.fs.Path(warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(paimonPath))) {
            catalog.createDatabase("db", true);
            Identifier identifier = Identifier.create("db", "documents");
            catalog.createTable(identifier, Schema.newBuilder()
                    .column("id", DataTypes.INT().notNull())
                    .column("embedding", new ArrayType(false, DataTypes.FLOAT().notNull()))
                    .option("bucket", "-1")
                    .option("data-evolution.enabled", "true")
                    .option("row-tracking.enabled", "true")
                    .build(), false);
            var writer = catalog.getTable(identifier).newBatchWriteBuilder();
            try (var write = writer.newWrite(); var commit = writer.newCommit()) {
                for (int i = 0; i < 12; ++i) {
                    write.write(GenericRow.of(i, new GenericArray(new float[] {i, 2, 3, 4})));
                }
                commit.commit(write.prepareCommit());
            }
        }

        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("paimon", catalogName, "", Map.of(
                "type", "paimon", "paimon.catalog.type", "filesystem", "paimon.catalog.warehouse", warehouse));
        String previousMode = connectContext.getSessionVariable().getPaimonGlobalIndexMode().modeName();
        try {
            var scanParams = new AtomicReference<PaimonScanRangeParams>();
            new MockUp<MetadataMgr>() {
                @Mock
                public List<RemoteFileInfo> getRemoteFiles(Invocation invocation, Table table,
                                                          GetRemoteFilesParams params) {
                    if (params instanceof PaimonScanRangeParams finalScan) {
                        scanParams.set(finalScan);
                    }
                    return invocation.proceed(table, params);
                }
            };
            // A shape that would be eligible with an index still uses ordinary planning when none exists.
            connectContext.getSessionVariable().setPaimonGlobalIndexMode("distributed");
            String select = "SELECT id, approx_l2_distance(embedding, CAST([1,2,3,4] AS ARRAY<FLOAT>)) AS score "
                    + "FROM " + catalogName + ".db.documents ";
            String plan = getFragmentPlan(select + "ORDER BY score LIMIT 3 OFFSET 2").toLowerCase(Locale.ROOT);
            var scan = scanParams.get();
            assertNotNull(scan);
            assertNotNull(scan.getAnnQuery());
            assertEquals("embedding", scan.getAnnQuery().columnName());
            assertEquals(List.of(1F, 2F, 3F, 4F), scan.getAnnQuery().queryVector());
            assertEquals(5, scan.getAnnQuery().k());
            assertEquals(-1, scan.getLimit());
            assertTrue(scan.getFieldNames().contains("embedding"));
            assertTrue(plan.contains("approx_l2_distance") && plan.contains("top-n"), plan);

            scanParams.set(null);
            getFragmentPlan(select + "WHERE id > 3 ORDER BY score LIMIT 3");
            assertNotNull(scanParams.get());
            assertNull(scanParams.get().getAnnQuery());
        } finally {
            connectContext.getSessionVariable().setPaimonGlobalIndexMode(previousMode);
            starRocksAssert.dropCatalog(catalogName);
        }
    }
}
