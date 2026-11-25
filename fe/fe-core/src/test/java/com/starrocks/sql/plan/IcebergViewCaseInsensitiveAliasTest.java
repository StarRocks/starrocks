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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

/**
 * Spark and Trino resolve relation aliases case-insensitively, so a view they define can refer to one
 * alias as both "t" and "T". Expanding such an Iceberg view must resolve the alias case-insensitively,
 * the same as a Hive view.
 */
public class IcebergViewCaseInsensitiveAliasTest extends ConnectorPlanTestBase {
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(newFolder(temp, "junit").toURI().toString());
        // A native view whose aliases differ only by case; valid only with case-sensitive aliases.
        starRocksAssert.withView("create view test.case_sensitive_alias_view as "
                + "select a.v1 from test.t0 a cross join test.t0 A");
    }

    @Test
    public void testMixedCaseAliasInIcebergView() throws Exception {
        String plan = getFragmentPlan("select * from iceberg0.view_db.mixed_case_alias_view");
        assertContains(plan, "IcebergScanNode");
    }

    @Test
    public void testOuterQueryAliasStaysCaseSensitive() {
        // Expanding the view must restore the session setting, so the outer query is unaffected.
        connectContext.setRelationAliasCaseInSensitive(false);
        Throwable t = Assertions.assertThrows(Throwable.class, () -> getFragmentPlan(
                "select T.id from iceberg0.view_db.mixed_case_alias_view t"));
        Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive());
        // The view itself expands fine; only the outer reference to "T" fails.
        Assertions.assertFalse(t.getMessage().contains("references invalid table"), t.getMessage());
        Assertions.assertTrue(t.getMessage().contains("cannot be resolved"), t.getMessage());
    }

    @Test
    public void testNativeViewInsideIcebergViewKeepsItsAliasRule() throws Exception {
        connectContext.setRelationAliasCaseInSensitive(false);
        String plan = getFragmentPlan("select * from iceberg0.view_db.native_view_reader");
        assertContains(plan, "CROSS JOIN");
        Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive());
        Assertions.assertNull(connectContext.getRelationAliasCaseInsensitiveOutsideConnectorView());
    }
}
