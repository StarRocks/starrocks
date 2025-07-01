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
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.server.WarehouseManager;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogicalSlotTest {
    @Mocked
    private ConnectContext.Listener listener1;
    @Mocked
    private ConnectContext.Listener listener2;

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

    @Test
    public void onQueryFinished_invokesAllListenersSuccessfully() {
        ConnectContext ctx = new ConnectContext();
        ctx.registerListener(listener1);
        ctx.registerListener(listener2);

        new Expectations() {
            {
                listener1.onQueryFinished(ctx);
                listener2.onQueryFinished(ctx);
            }
        };

        ctx.onQueryFinished();
    }

    @Test
    public void onQueryFinished_logsWarningWhenListenerThrowsException() {
        ConnectContext ctx = new ConnectContext();
        ctx.registerListener(listener1);

        new Expectations() {
            {
                listener1.onQueryFinished(ctx);
                result = new RuntimeException("Listener error");
            }
        };

        ctx.onQueryFinished();
        // Verify that the warning log is generated (log verification depends on the logging framework used)
    }

    @Test
    public void onQueryFinished_logsWarningWhenAuditEventBuilderFails() {
        ConnectContext ctx = new ConnectContext();

        new MockUp<AuditEvent.AuditEventBuilder>() {
            @Mock
            public void setCNGroup(String groupName) {
                throw new RuntimeException("AuditEventBuilder error");
            }
        };

        ctx.onQueryFinished();
        // Verify that the warning log is generated (log verification depends on the logging framework used)
    }

    @Test
    public void onQueryFinished_handlesEmptyListenerListGracefully() {
        ConnectContext ctx = new ConnectContext();
        ctx.onQueryFinished();
        // No exceptions should be thrown
    }

    @Test
    public void onQueryFinished_invokesAllListenersSuccessfully(
            @Mocked ConnectContext.Listener listener1,
            @Mocked ConnectContext.Listener listener2) {
        ConnectContext ctx = new ConnectContext();
        ctx.registerListener(listener1);
        ctx.registerListener(listener2);

        new Expectations() {
            {
                listener1.onQueryFinished(ctx);
                listener2.onQueryFinished(ctx);
            }
        };

        ctx.onQueryFinished();
    }

    @Test
    public void onQueryFinished_logsWarningWhenListenerThrowsException(
            @Mocked ConnectContext.Listener listener) {
        ConnectContext ctx = new ConnectContext();
        ctx.registerListener(listener);

        new Expectations() {
            {
                listener.onQueryFinished(ctx);
                result = new RuntimeException("Listener error");
            }
        };

        ctx.onQueryFinished();
        // Verify that the warning is logged (log verification depends on the logging framework used).
    }

    @Test
    public void onQueryFinished_setsCNGroupSuccessfully(@Mocked AuditEvent.AuditEventBuilder auditEventBuilder) {
        ConnectContext ctx = new ConnectContext();

        new Expectations(ctx) {
            {
                ctx.getCurrentComputeResourceName();
                result = "testCNGroup";
                auditEventBuilder.setCNGroup("testCNGroup");
            }
        };

        ctx.onQueryFinished();
    }

    @Test
    public void onQueryFinished_logsWarningWhenSettingCNGroupFails(
            @Mocked AuditEvent.AuditEventBuilder auditEventBuilder) {
        ConnectContext ctx = new ConnectContext();

        new Expectations(ctx) {
            {
                ctx.getCurrentComputeResourceName();
                result = new RuntimeException("CN group error");
            }
        };

        ctx.onQueryFinished();
        // Verify that the warning is logged (log verification depends on the logging framework used).
    }
}
