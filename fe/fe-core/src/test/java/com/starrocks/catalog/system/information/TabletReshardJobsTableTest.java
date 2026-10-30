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

package com.starrocks.catalog.system.information;

import com.starrocks.catalog.system.SystemTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TabletReshardJobsTableTest {

    @Test
    public void testRoutedToLeader() {
        // Reshard jobs live only on the leader: TabletReshardJobMgr runs there, and a job's mutable state
        // (including the reason it is stuck retrying a publish) is not journaled on every change -- only
        // state transitions are. A follower answering this scan from its own replayed copy would report a
        // job as RUNNING with an empty ERROR_MESSAGE while the leader knows why it is not progressing,
        // which defeats the point of reporting the reason at all. Same rationale as the other job-state
        // tables in QUERY_FROM_LEADER_TABLES (loads, routine_load_jobs, task_runs, ...).
        Assertions.assertTrue(SystemTable.needQueryFromLeader(TabletReshardJobsTable.NAME));
    }
}
