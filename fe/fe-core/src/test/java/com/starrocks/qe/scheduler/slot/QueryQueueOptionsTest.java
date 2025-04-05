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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.common.Config;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.BackendResourceStat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryQueueOptionsTest extends SchedulerTestBase {
    private boolean prevEnableQueryQueueV2 = false;
    private boolean prevEnableQueryQueueSelect = false;

    @Before
    public void before() {
        prevEnableQueryQueueV2 = Config.enable_query_queue_v2;
        prevEnableQueryQueueSelect = GlobalVariable.isEnableQueryQueueSelect();
    }

    @After
    public void after() {
        Config.enable_query_queue_v2 = prevEnableQueryQueueV2;
        GlobalVariable.setEnableQueryQueueSelect(prevEnableQueryQueueSelect);

        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testZeroConcurrencyLevel() {
        QueryQueueOptions.V2 opts = new QueryQueueOptions.V2(0, 0, 0, 0, 0, 0);
        assertThat(opts.getTotalSlots()).isEqualTo(4);
    }

    @Test
    public void testZeroOthers() {
        QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2(2, 0, 0, 0, 0, 0));
        assertThat(opts.v2().getNumWorkers()).isOne();
        assertThat(opts.v2().getNumRowsPerSlot()).isOne();
        assertThat(opts.v2().getTotalSlots()).isEqualTo(2);
        assertThat(opts.v2().getTotalSmallSlots()).isOne();
        assertThat(opts.v2().getMemBytesPerSlot()).isEqualTo(Long.MAX_VALUE);
        assertThat(opts.v2().getCpuCostsPerSlot()).isOne();
    }

    @Test
    public void testCreateFromEnv() {
        {
            Config.enable_query_queue_v2 = false;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);

            assertThat(opts.isEnableQueryQueueV2()).isFalse();
        }

        {
            final int numCores = 16;
            final long memLimitBytes = 64L * 1024 * 1024 * 1024;
            final int numBEs = 2;
            final int concurrencyLevel = Config.query_queue_v2_concurrency_level;

            BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, numCores);
            BackendResourceStat.getInstance().setMemLimitBytesOfBe(1, memLimitBytes);
            BackendResourceStat.getInstance().setNumHardwareCoresOfBe(2, numCores);
            BackendResourceStat.getInstance().setMemLimitBytesOfBe(2, memLimitBytes);
            Config.enable_query_queue_v2 = true;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);

            assertThat(opts.isEnableQueryQueueV2()).isTrue();
            assertThat(opts.v2().getNumWorkers()).isEqualTo(numBEs);
            assertThat(opts.v2().getNumRowsPerSlot()).isEqualTo(Config.query_queue_v2_num_rows_per_slot);
            assertThat(opts.v2().getTotalSlots()).isEqualTo(concurrencyLevel * numBEs * numCores);
            assertThat(opts.v2().getMemBytesPerSlot()).isEqualTo(memLimitBytes / concurrencyLevel / numCores);
            assertThat(opts.v2().getTotalSmallSlots()).isEqualTo(numCores);
        }
    }

    @Test
    public void testCreateFromEnvAndQuery() throws Exception {
        Config.enable_query_queue_v2 = true;

        {
            GlobalVariable.setEnableQueryQueueSelect(false);
            DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(enable_query_queue=true)*/ * FROM lineitem");
            QueryQueueOptions opts = QueryQueueOptions.createFromEnvAndQuery(coordinator);
            assertThat(opts.isEnableQueryQueueV2()).isFalse();
        }

        {
            GlobalVariable.setEnableQueryQueueSelect(true);
            DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(enable_query_queue=true)*/ * FROM lineitem");
            QueryQueueOptions opts = QueryQueueOptions.createFromEnvAndQuery(coordinator);
            assertThat(opts.isEnableQueryQueueV2()).isTrue();
        }

        {
            GlobalVariable.setEnableQueryQueueSelect(true);
            DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(enable_query_queue=false)*/ * FROM lineitem");
            QueryQueueOptions opts = QueryQueueOptions.createFromEnvAndQuery(coordinator);
            assertThat(opts.isEnableQueryQueueV2()).isFalse();
        }

        {
            GlobalVariable.setEnableQueryQueueSelect(true);
            DefaultCoordinator coordinator = getScheduler(
                    "SELECT /*+SET_VAR(enable_query_queue=true)*/ * FROM information_schema.columns");
            QueryQueueOptions opts = QueryQueueOptions.createFromEnvAndQuery(coordinator);
            assertThat(opts.isEnableQueryQueueV2()).isFalse();
        }
    }

    @Test
    public void testCreateV2WithMetrics() {
        assertThat(QueryQueueOptions.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID)).isNotNull();
        assertThat(QueryQueueOptions.getQueryQueuePendingTimeoutSecond(WarehouseManager.DEFAULT_WAREHOUSE_ID))
                .isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());
        assertThat(QueryQueueOptions.getQueryQueueMaxQueuedQueries(WarehouseManager.DEFAULT_WAREHOUSE_ID))
                .isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(QueryQueueOptions.isEnableQueryQueue(WarehouseManager.DEFAULT_WAREHOUSE_ID))
                .isEqualTo(Config.enable_query_queue_v2);

        {
            Config.enable_query_queue_v2 = false;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
            assertThat(opts.isEnableQueryQueueV2()).isFalse();
            assertThat(opts.v2()).isEqualTo(new QueryQueueOptions.V2());
        }

        {
            Config.enable_query_queue_v2 = true;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
            assertThat(opts.isEnableQueryQueueV2()).isTrue();
            assertThat(opts.v2()).isNotEqualTo(new QueryQueueOptions.V2());
            QueryQueueOptions.V2 v2 = opts.v2();
            assertThat(v2.getNumWorkers()).isEqualTo(1);
            assertThat(v2.getNumRowsPerSlot()).isEqualTo(Config.query_queue_v2_num_rows_per_slot);
            assertThat(v2.getTotalSlots()).isEqualTo(4);
            assertThat(v2.getTotalSmallSlots()).isEqualTo(1);
            assertThat(v2.getCpuCostsPerSlot()).isEqualTo(Config.query_queue_v2_cpu_costs_per_slot);
        }
    }

    @Test
    public void testCorrectSlotNum() {
        assertThat(QueryQueueOptions.correctSlotNum(0)).isEqualTo(0);
        assertThat(QueryQueueOptions.correctSlotNum(1)).isEqualTo(1);
        assertThat(QueryQueueOptions.correctSlotNum(2)).isEqualTo(1);
        assertThat(QueryQueueOptions.correctSlotNum(4)).isEqualTo(1);
        assertThat(QueryQueueOptions.correctSlotNum(5)).isEqualTo(2);
    }
}
