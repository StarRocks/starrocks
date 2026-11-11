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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AlterMvRewriteStalenessPersistTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE t_staleness (k1 int, v1 int) DUPLICATE KEY(k1) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES('replication_num' = '1')");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_staleness " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1 REFRESH MANUAL " +
                "AS SELECT k1, sum(v1) AS sv FROM t_staleness GROUP BY k1");
    }

    @Test
    public void testImageLoadRestoresStalenessFromProperties() throws Exception {
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv_staleness SET ('mv_rewrite_staleness_second' = '600')");
        MaterializedView mv = starRocksAssert.getMv(DB_NAME, "mv_staleness");
        // what an image checkpointed by a replay that dropped the ALTER carries
        mv.setMaxMVRewriteStaleness(0);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(image.getImageWriter());

        LocalMetastore restarted = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        restarted.load(new SRMetaBlockReaderV2(image.getJsonReader()));
        MaterializedView loaded = (MaterializedView) restarted.getDb(DB_NAME).getTable("mv_staleness");
        Assertions.assertEquals(600, loaded.getMaxMVRewriteStaleness());
    }
}
