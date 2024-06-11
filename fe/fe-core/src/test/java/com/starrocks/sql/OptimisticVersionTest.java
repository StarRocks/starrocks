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

package com.starrocks.sql;

import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class OptimisticVersionTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterAll
    public static void afterAll() {
        PlanTestBase.afterClass();
    }

    @Test
    public void testOptimisticVersion() {
        OlapTable table = new OlapTable();

        // initialized
        assertTrue(OptimisticVersion.validateTableUpdate(table, OptimisticVersion.generate()));

        // schema change
        table.lastSchemaUpdateTime.set(OptimisticVersion.generate());
        assertTrue(OptimisticVersion.validateTableUpdate(table, OptimisticVersion.generate()));
    }
}