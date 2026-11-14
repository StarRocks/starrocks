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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MvRewritePredicateFallbackTest extends MVTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE predicate_fallback (id INT NULL, aux INT NULL, v BIGINT NULL) " +
                "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        executeInsertSql("INSERT INTO predicate_fallback VALUES (1,0,10),(2,2,20),(3,0,30),(NULL,2,40)");
        createAndRefreshMv("CREATE MATERIALIZED VIEW predicate_fallback_mv " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH MANUAL PROPERTIES ('replication_num'='1') AS " +
                "SELECT id, aux, v FROM predicate_fallback WHERE id=1");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        dropMv(DB_NAME, "predicate_fallback_mv");
        starRocksAssert.dropTable("predicate_fallback");
    }

    @Test
    public void testFilteredSimplePredicateHitsMv() throws Exception {
        assertFilteredMv("id=1");
    }

    @Test
    public void testFilteredCasePredicateHitsMv() throws Exception {
        assertFilteredMv("CASE WHEN id=1 THEN 'a' ELSE 'b' END='a'");
    }

    private void assertFilteredMv(String predicate) throws Exception {
        SessionVariable session = connectContext.getSessionVariable();
        String rewriteMode = session.getMaterializedViewRewriteMode();
        boolean multiStages = session.isEnableMaterializedViewMultiStagesRewrite();
        boolean unionRewrite = session.isEnableMaterializedViewUnionRewrite();
        try {
            session.setMaterializedViewRewriteMode("force");
            session.setEnableMaterializedViewMultiStagesRewrite(false);
            session.setEnableMaterializedViewUnionRewrite(false);
            String plan = getFragmentPlan("SELECT id, aux, v FROM predicate_fallback WHERE " + predicate);
            PlanTestBase.assertContains(plan, "TABLE: predicate_fallback_mv");
            PlanTestBase.assertNotContains(plan, "TABLE: predicate_fallback\n");
        } finally {
            session.setMaterializedViewRewriteMode(rewriteMode);
            session.setEnableMaterializedViewMultiStagesRewrite(multiStages);
            session.setEnableMaterializedViewUnionRewrite(unionRewrite);
        }
    }
}
