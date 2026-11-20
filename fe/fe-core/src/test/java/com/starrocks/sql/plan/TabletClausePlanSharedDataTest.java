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

import com.starrocks.server.RunMode;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The TABLET clause cases in a shared-data cluster. Here a table created without DISTRIBUTED BY is range distributed,
 * so every case also runs on such tables.
 */
public class TabletClausePlanSharedDataTest extends TabletClausePlanTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        startCluster(RunMode.SHARED_DATA);
        createTables();
        // Range distribution is the shared-data default. Asking for it on the session too keeps these tables range
        // distributed whatever the cluster config says.
        connectContext.getSessionVariable().setEnableRangeDistribution(true);
        createPartitionedTables("t_dup_range", "t_pk_range", "");
        List<String> rangeDistributedTables = List.of("t_dup_range", "t_pk_range");
        for (String name : rangeDistributedTables) {
            assertEquals("RANGE", table(name).getDefaultDistributionInfo().getType().name(), name);
        }
        useProductionPruning();
    }
}
