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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TResourceLogicalSlot;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.Warehouse;
import org.apache.parquet.Strings;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A logical slot represents resources which is required by a query from the {@link SlotManager}.
 * <p> It contains multiple physical slots. A physical slot represents a part of resource from the cluster. There are a total of
 * {@link GlobalVariable#getQueryQueueConcurrencyLimit()} slots and {@link ResourceGroup#getConcurrencyLimit()} slots for a group.
 * <p> When the query queue is enabled by {@link GlobalVariable#isEnableQueryQueueSelect()},
 * {@link GlobalVariable#isEnableQueryQueueStatistic()} or {@link GlobalVariable#isEnableQueryQueueLoad()}, a query must be
 * required enough physical slots from the {@link SlotManager}.
 */
public class LogicalSlot {
    public static final long ABSENT_GROUP_ID = -1;

    private final TUniqueId slotId;
    private final String requestFeName;

    private final long warehouseId;
    private Optional<String> warehouseName = Optional.empty();

    private final long groupId;

    // est number of physical slots to require
    private final int numPhysicalSlots;

    private final long expiredPendingTimeMs;

    private final long expiredAllocatedTimeMs;

    private final long feStartTimeMs;

    /**
     * Set when creating this slot. It is just used in {@code show running queries} and not accurate enough.
     */
    private final long startTimeMs;
    private final int numFragments;
    private int pipelineDop;
    private Optional<Integer> allocatedNumPhysicalSlots = Optional.empty();
    private Optional<Double> queuedWaitSeconds = Optional.empty();
    private Optional<ExtraMessage> extraMessage = Optional.empty();

    private State state = State.CREATED;

    public LogicalSlot(TUniqueId slotId, String requestFeName,
                       long warehouseId, long groupId, int numPhysicalSlots,
                       long expiredPendingTimeMs, long expiredAllocatedTimeMs, long feStartTimeMs,
                       int numFragments, int pipelineDop) {
        this.slotId = slotId;
        this.requestFeName = requestFeName;
        this.warehouseId = warehouseId;
        this.groupId = groupId;
        this.numPhysicalSlots = numPhysicalSlots;
        this.expiredPendingTimeMs = expiredPendingTimeMs;
        this.expiredAllocatedTimeMs = expiredAllocatedTimeMs;
        this.feStartTimeMs = feStartTimeMs;
        this.startTimeMs = System.currentTimeMillis();
        this.numFragments = numFragments;
        this.pipelineDop = pipelineDop;
    }

    public State getState() {
        return state;
    }

    public void onRequire() {
        transitionState(State.CREATED, State.REQUIRING);
    }

    public void onAllocate() {
        transitionState(State.REQUIRING, State.ALLOCATED);
        if (state == State.ALLOCATED) {
            // Record the number of physical slots allocated
            allocatedNumPhysicalSlots = Optional.of(numPhysicalSlots);
            // Record the time spent in the queue
            queuedWaitSeconds = Optional.of((System.currentTimeMillis() - startTimeMs) / 1000.0);
        }
    }

    public void onCancel() {
        transitionState(state, State.CANCELLED);
    }

    public void onRetry() {
        transitionState(state, State.CREATED);
    }

    public void onRelease() {
        transitionState(State.ALLOCATED, State.RELEASED);
    }

    public TResourceLogicalSlot toThrift() {
        TResourceLogicalSlot tslot = new TResourceLogicalSlot();
        tslot.setSlot_id(slotId)
                .setRequest_fe_name(requestFeName)
                .setGroup_id(groupId)
                .setWarehouse_id(warehouseId)
                .setNum_slots(numPhysicalSlots)
                .setExpired_pending_time_ms(expiredPendingTimeMs)
                .setExpired_allocated_time_ms(expiredAllocatedTimeMs)
                .setFe_start_time_ms(feStartTimeMs)
                .setNum_fragments(numFragments)
                .setPipeline_dop(pipelineDop);

        return tslot;
    }

    public static LogicalSlot fromThrift(TResourceLogicalSlot tslot) {
        return new LogicalSlot(tslot.getSlot_id(), tslot.getRequest_fe_name(), tslot.getWarehouse_id(),
                tslot.getGroup_id(), tslot.getNum_slots(),
                tslot.getExpired_pending_time_ms(), tslot.getExpired_allocated_time_ms(), tslot.getFe_start_time_ms(),
                tslot.getNum_fragments(), tslot.getPipeline_dop());
    }

    public TUniqueId getSlotId() {
        return slotId;
    }

    public String getRequestFeName() {
        return requestFeName;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public long getGroupId() {
        return groupId;
    }

    public int getNumPhysicalSlots() {
        return numPhysicalSlots;
    }

    public long getExpiredPendingTimeMs() {
        return expiredPendingTimeMs;
    }

    public long getExpiredAllocatedTimeMs() {
        return expiredAllocatedTimeMs;
    }

    public boolean isPendingTimeout() {
        return System.currentTimeMillis() >= expiredPendingTimeMs;
    }

    public boolean isAllocatedExpired(long nowMs) {
        return nowMs >= expiredAllocatedTimeMs;
    }

    public long getFeStartTimeMs() {
        return feStartTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public int getNumDrivers() {
        return numFragments * pipelineDop;
    }

    public int getNumFragments() {
        return numFragments;
    }

    public boolean isAdaptiveDop() {
        return pipelineDop == 0;
    }

    public int getPipelineDop() {
        return pipelineDop;
    }

    public void setPipelineDop(int pipelineDop) {
        this.pipelineDop = pipelineDop;
    }

    public Optional<Integer> getAllocatedNumPhysicalSlots() {
        return allocatedNumPhysicalSlots;
    }

    public double getQueuedWaitSeconds() {
        return queuedWaitSeconds.orElse((System.currentTimeMillis() - startTimeMs) / 1000.0);
    }

    public String getWarehouseName() {
        if (warehouseName.isEmpty()) {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse = warehouseManager.getWarehouse(warehouseId);
            this.warehouseName = Optional.of(warehouse.getName());
        }
        return warehouseName.orElse("");
    }

    @Override
    public String toString() {
        return "LogicalSlot{" +
                "slotId=" + DebugUtil.printId(slotId) +
                ", requestFeName='" + requestFeName + '\'' +
                ", warehouseId=" + warehouseId +
                ", groupId=" + groupId +
                ", numPhysicalSlots=" + numPhysicalSlots +
                ", expiredPendingTimeMs=" + TimeUtils.longToTimeString(expiredPendingTimeMs) +
                ", expiredAllocatedTimeMs=" + TimeUtils.longToTimeString(expiredAllocatedTimeMs) +
                ", feStartTimeMs=" + TimeUtils.longToTimeString(feStartTimeMs) +
                ", startTimeMs=" + TimeUtils.longToTimeString(startTimeMs) +
                ", state=" + state +
                '}';
    }

    private void transitionState(State from, State to) {
        if (state == from) {
            state = to;
        }
    }

    public enum State {
        CREATED,
        REQUIRING,
        ALLOCATED,
        RELEASED,
        CANCELLED;

        boolean isTerminal() {
            return this == RELEASED || this == CANCELLED;
        }

        // Put RUNNING State at first
        private static final Map<State, Integer> SORT_ORDER = Map.of(
                ALLOCATED, 1,
                REQUIRING, 2,
                CREATED, 3,
                CANCELLED, 4,
                RELEASED, 5
        );

        public int getSortOrder() {
            return SORT_ORDER.getOrDefault(this, SORT_ORDER.size());
        }

        public String toQueryStateString() {
            switch (this) {
                case CREATED:
                    return "CREATED";
                case REQUIRING:
                    return "PENDING";
                case ALLOCATED:
                    return "RUNNING";
                case RELEASED:
                case CANCELLED:
                    return "FINISHED";
                default:
                    return "UNKNOWN";
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(slotId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LogicalSlot that = (LogicalSlot) obj;
        return Objects.equals(slotId, that.slotId);
    }

    public static class ExtraMessage {
        // to avoid too long query string, use a trimmed string
        private static final int QUERY_TRIM_LENGTH = 16;

        private final String query;
        private final long queryStartTime;
        private final long queryEndTime;
        private final long queryDuration;

        @SerializedName("QueryState")
        private final QueryState.MysqlStateType queryState;
        @SerializedName("PlanMemCostBytes")
        private final long planMemCostBytes;
        @SerializedName("MemCostBytes")
        private final long memCostBytes;
        @SerializedName("PredictMemBytes")
        private final long predictMemBytes;

        public ExtraMessage(ConnectContext connectContext) {
            this.query = getOriginalStmt(connectContext);
            this.queryStartTime = getQueryStartTime(connectContext);
            // this is the time when the query is finished
            this.queryEndTime = System.currentTimeMillis();
            this.queryDuration = getQueryDuration(connectContext);
            if (connectContext != null && connectContext.getState() != null) {
                this.queryState = connectContext.getState().getStateType();
            } else {
                this.queryState = QueryState.MysqlStateType.OK;
            }
            this.planMemCostBytes = getPlanMemCostBytes(connectContext);
            this.memCostBytes = getMemCostBytes(connectContext);
            this.predictMemBytes = getPredictMemBytes(connectContext);
        }

        private String getOriginalStmt(ConnectContext connectContext) {
            if (connectContext == null) {
                return "";
            }
            StmtExecutor executor = connectContext.getExecutor();
            if (executor == null) {
                return "";
            }
            String originalStmt = executor.getOriginStmtInString();
            // only substring when the length is greater than QUERY_TRIM_LENGTH
            if (!Strings.isNullOrEmpty(originalStmt) && originalStmt.length() > QUERY_TRIM_LENGTH) {
                originalStmt = originalStmt.trim().substring(0, QUERY_TRIM_LENGTH);
            }
            return originalStmt;
        }

        private long getQueryStartTime(ConnectContext connectContext) {
            if (connectContext == null) {
                return 0;
            }
            return connectContext.getStartTime();
        }

        private long getQueryDuration(ConnectContext connectContext) {
            if (connectContext == null) {
                return 0;
            }
            long endTime = getQueryEndTime();
            long result = endTime - connectContext.getStartTime();
            return result <= 0 ? 0 : result;
        }

        private long getPlanMemCostBytes(ConnectContext connectContext) {
            if (connectContext == null || connectContext.getAuditEventBuilder() == null) {
                return 0;
            }
            AuditEvent auditEvent = connectContext.getAuditEventBuilder().build();
            return (long) auditEvent.planMemCosts;
        }

        private long getMemCostBytes(ConnectContext connectContext) {
            if (connectContext == null || connectContext.getAuditEventBuilder() == null) {
                return 0;
            }
            AuditEvent auditEvent = connectContext.getAuditEventBuilder().build();
            return auditEvent.memCostBytes;
        }

        private long getPredictMemBytes(ConnectContext connectContext) {
            if (connectContext == null || connectContext.getAuditEventBuilder() == null) {
                return 0;
            }
            AuditEvent auditEvent = connectContext.getAuditEventBuilder().build();
            return auditEvent.predictMemBytes;
        }

        public long getPlanMemCostBytes() {
            return planMemCostBytes;
        }

        public String getQuery() {
            return query;
        }

        public long getQueryDuration() {
            return queryDuration;
        }

        public long getQueryEndTime() {
            return queryEndTime;
        }

        public long getQueryStartTime() {
            return queryStartTime;
        }

        public QueryState.MysqlStateType getQueryState() {
            return queryState;
        }

        public long getMemCostBytes() {
            return memCostBytes;
        }

        public long getPredictMemBytes() {
            return predictMemBytes;
        }
    }

    public void setExtraMessage(ExtraMessage extraMessage) {
        this.extraMessage = Optional.of(extraMessage);
    }

    public Optional<ExtraMessage> getExtraMessage() {
        return extraMessage;
    }

    public static class ConnectContextListener implements ConnectContext.Listener {
        private final LogicalSlot logicalSlot;
        public ConnectContextListener(LogicalSlot logicalSlot) {
            this.logicalSlot = logicalSlot;
        }

        @Override
        public void onQueryFinished(ConnectContext context) {
            if (Config.max_query_queue_history_slots_number > 0 && this.logicalSlot != null) {
                LogicalSlot.ExtraMessage extraMessage = new LogicalSlot.ExtraMessage(context);
                this.logicalSlot.setExtraMessage(extraMessage);
            }
        }
    }
}
