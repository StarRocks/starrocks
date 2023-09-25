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

package com.starrocks.qe.scheduler.plan;

import com.starrocks.qe.scheduler.SchedulerTestBase;
import org.junit.Test;

public class AggregateTest extends SchedulerTestBase {
    @Test
    public void testLocalOnePhaseAggregateNonePartition() {
        runFileUnitTest("scheduler/aggregate/agg_local_one_phase_non_partition");
    }

    @Test
    public void testLocalOnePhaseAggregatePartition() {
        runFileUnitTest("scheduler/aggregate/agg_local_one_phase_partition");
    }

    /**
     * Check whether the session variable {@code parallel_exchange_instance_num} is effective.
     *
     * @see com.starrocks.qe.SessionVariable#PARALLEL_EXCHANGE_INSTANCE_NUM
     */
    @Test
    public void testLocalOnePhaseAggregateWithExchangeInstanceParallel() {
        runFileUnitTest("scheduler/aggregate/agg_local_one_phase_non_partition_one_exchange");
    }
}
