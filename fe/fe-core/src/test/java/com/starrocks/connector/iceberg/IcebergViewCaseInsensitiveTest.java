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

package com.starrocks.connector.iceberg;

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IcebergViewCaseInsensitiveTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Test
    public void testQueryIcebergViewCaseInsensitive() throws Exception {
        String sql = "select * from iceberg0.unpartitioned_db." + MockIcebergMetadata.MOCKED_CASE_INSENSITIVE_VIEW_NAME;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "t0");

        // Aliases are matched case-insensitively only while the view body is analyzed; outside of it
        // the session default (case-sensitive) is restored.
        Throwable exception = assertThrows(SemanticException.class, () -> {
            getFragmentPlan("select * from iceberg0.unpartitioned_db."
                    + MockIcebergMetadata.MOCKED_CASE_INSENSITIVE_VIEW_NAME
                    + " v1 join test.t0 T0 on v1.id = t0.v1");
        });
        assertThat(exception.getMessage(), containsString("Column '`t0`.`v1`' cannot be resolved"));
    }
}
