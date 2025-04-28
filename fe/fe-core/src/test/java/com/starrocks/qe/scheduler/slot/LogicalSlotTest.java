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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.Assert.assertTrue;

public class LogicalSlotTest {

    private static LogicalSlot generateSlot(int numSlots) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0,
                0, 0);
    }

    @Test
    public void testExtraMessage() {
        LogicalSlot slot1 = generateSlot(1);
        assertTrue(slot1.getExtraMessage().isEmpty());

        ConnectContext connectContext = new ConnectContext();
        LogicalSlot.ExtraMessage extraMessage = new LogicalSlot.ExtraMessage(connectContext);
        assertTrue(extraMessage.getQuery().isEmpty());
        assertTrue(extraMessage.getMemCostBytes() == -1);
        assertTrue(extraMessage.getPlanMemCostBytes() == -1);
        assertTrue(extraMessage.getQueryStartTime() != 0);
        assertTrue(extraMessage.getQueryEndTime() != 0);
        assertTrue(extraMessage.getQueryDuration() >= 0);
        assertTrue(extraMessage.getPredictMemBytes() == 0L);
        assertTrue(extraMessage.getQueryState().equals(QueryState.MysqlStateType.OK));
        String json = GsonUtils.GSON.toJson(extraMessage);
        assertTrue(json.equals("{\"QueryState\":\"OK\",\"PlanMemCostBytes\":-1,\"MemCostBytes\":-1,\"PredictMemBytes\":0}"));
    }

    @Test
    public void testExtraMessageWithNull() {
        LogicalSlot slot1 = generateSlot(1);
        assertTrue(slot1.getExtraMessage().isEmpty());

        LogicalSlot.ExtraMessage extraMessage = new LogicalSlot.ExtraMessage(null);
        assertTrue(extraMessage.getQuery().isEmpty());
        assertTrue(extraMessage.getMemCostBytes() == 0);
        assertTrue(extraMessage.getPlanMemCostBytes() == 0);
        assertTrue(extraMessage.getQueryStartTime() == 0);
        assertTrue(extraMessage.getQueryEndTime() >= 0);
        assertTrue(extraMessage.getQueryDuration() >= 0);
        assertTrue(extraMessage.getPredictMemBytes() == 0L);
        assertTrue(extraMessage.getQueryState().equals(QueryState.MysqlStateType.OK));

        System.out.println(GsonUtils.GSON.toJson(extraMessage));
        String json = GsonUtils.GSON.toJson(extraMessage);
        assertTrue(json.equals("{\"QueryState\":\"OK\",\"PlanMemCostBytes\":0,\"MemCostBytes\":0,\"PredictMemBytes\":0}"));
    }

    @Test
    public void testConnectContextListener() {
        LogicalSlot slot1 = generateSlot(1);
        LogicalSlot.ConnectContextListener listener = new LogicalSlot.ConnectContextListener(slot1);
        ConnectContext connectContext = new ConnectContext();
        connectContext.setStartTime();

        connectContext.registerListener(listener);

        connectContext.onQueryFinished();
        Optional<LogicalSlot.ExtraMessage> extraMessage = slot1.getExtraMessage();
        assertTrue(extraMessage.isEmpty());

        Config.max_query_queue_history_slots_number = 10;
        connectContext.onQueryFinished();
        extraMessage = slot1.getExtraMessage();
        assertTrue(extraMessage.isPresent());
        assertTrue(extraMessage.get().getQueryStartTime() == connectContext.getStartTime());
        Config.max_query_queue_history_slots_number = 0;
    }
}
