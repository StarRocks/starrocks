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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class WarehouseQueryQueueOptionsTest {
    private boolean prevEnableQueryQueueV2 = false;
    private boolean prevEnableQueryQueueSelect = false;

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private SystemInfoService infoService;
    @Mocked
    private ConnectContext connectContext;

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
    public void testCreateV2WithMetricsInSharedData() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        long warehouseId = 10000L;
        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.getCurrentWarehouseId();
                minTimes = 0;
                result = warehouseId;

                GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseId);
                minTimes = 0;
                result = Lists.newArrayList(10003L, 10004L, 10005L);
            }
        };

        {
            Config.enable_query_queue_v2 = false;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(warehouseId);
            assertThat(opts.isEnableQueryQueueV2()).isFalse();
            assertThat(opts.v2()).isEqualTo(new QueryQueueOptions.V2());
        }

        new MockUp<BaseSlotManager>() {
            @Mock
            public boolean isEnableQueryQueueV2(long warehouseId) {
                return true;
            }
        };

        {
            Config.enable_query_queue_v2 = true;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(warehouseId);
            assertThat(opts.isEnableQueryQueueV2()).isTrue();
            assertThat(opts.v2()).isNotEqualTo(new QueryQueueOptions.V2());
            QueryQueueOptions.V2 v2 = opts.v2();
            assertThat(v2.getNumWorkers()).isEqualTo(3);
            assertThat(v2.getNumRowsPerSlot()).isEqualTo(Config.query_queue_v2_num_rows_per_slot);
            assertThat(v2.getTotalSlots()).isEqualTo(12);
            assertThat(v2.getTotalSmallSlots()).isEqualTo(1);
            assertThat(v2.getCpuCostsPerSlot()).isEqualTo(Config.query_queue_v2_cpu_costs_per_slot);
        }
    }
}
