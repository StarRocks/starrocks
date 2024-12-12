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

package com.starrocks.system;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.DecommissionType;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DnsCache;
import com.starrocks.datacache.DataCacheMetrics;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResourceGroupUsage;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This class extends the primary identifier of a compute node with computing capabilities
 * and no storage capacity。
 */
public class ComputeNode implements IComputable, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ComputeNode.class);

    @SerializedName("id")
    private long id;
    @SerializedName("host")
    private String host;
    @SerializedName("version")
    private String version;

    @SerializedName("heartbeatPort")
    private int heartbeatPort; // heartbeat
    @SerializedName("bePort")
    private volatile int bePort; // be
    @SerializedName("httpPort")
    private volatile int httpPort; // web service
    @SerializedName("beRpcPort")
    private volatile int beRpcPort; // be rpc port
    @SerializedName("brpcPort")
    private volatile int brpcPort = -1;
    @SerializedName("arrowFlightPort")
    private volatile int arrowFlightPort = -1; // be arrow port

    @SerializedName("cpuCores")
    private volatile int cpuCores = 0; // Cpu cores of node
    @SerializedName("mlb")
    private volatile long memLimitBytes = 0;

    @SerializedName("lastUpdateMs")
    private volatile long lastUpdateMs;
    @SerializedName("lastStartTime")
    private volatile long lastStartTime;
    @SerializedName("isAlive")
    private AtomicBoolean isAlive;

    @SerializedName("isDecommissioned")
    private final AtomicBoolean isDecommissioned;
    @SerializedName("decommissionType")
    private volatile int decommissionType;

    // to index the state in some cluster
    @SerializedName("backendState")
    private volatile int backendState;

    @SerializedName("heartbeatErrMsg")
    private String heartbeatErrMsg = "";

    @SerializedName("lastMissingHeartbeatTime")
    private long lastMissingHeartbeatTime = -1;

    @SerializedName("heartbeatRetryTimes")
    private int heartbeatRetryTimes = 0;

    // port of starlet on BE
    @SerializedName("starletPort")
    private volatile int starletPort = 0;

    @SerializedName("lastWriteFail")
    private volatile boolean lastWriteFail = false;

    @SerializedName("workerGroupId")
    private long workerGroupId = 0;

    @SerializedName("warehouseId")
    private long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
    // Indicate there is whether storage_path or not with CN node
    // It must be true for Backend
    @SerializedName("isSetStoragePath")
    private volatile boolean isSetStoragePath = false;

    // Tracking the heartbeat status, CONNECTING/ALIVE/SHUTDOWN/DISCONNECTED
    @SerializedName("status")
    private Status status;

    private volatile DataCacheMetrics dataCacheMetrics = null;

    private volatile int numRunningQueries = 0;
    private volatile long memUsedBytes = 0;
    private volatile int cpuUsedPermille = 0;
    private volatile long lastUpdateResourceUsageMs = 0;
    private final AtomicReference<Map<Long, ResourceGroupUsage>> groupIdToUsage = new AtomicReference<>(new HashMap<>());

    /**
     * Other similar status might be confusing with this one.
     * - HeartbeatResponse.HbStatus: {OK, BAD}
     * - HeartbeatResponse.AliveStatus: {ALIVE, NOT_ALIVE}
     * - Backend.BackendState: {using, offline, free}
     * NOTE: The status will be serialized along with the ComputeNode object,
     * so be cautious changing the enum name.
     */
    public enum Status {
        CONNECTING,         // New added node, no heartbeat probing yet
        OK,                 // Heartbeat OK
        SHUTDOWN,           // Heartbeat response code indicating shutdown in progress
        DISCONNECTED,       // Heartbeat failed consecutively for `n` times
    }

    public ComputeNode() {
        this.host = "";
        this.version = "";
        this.lastUpdateMs = 0;
        this.lastStartTime = 0;
        this.isAlive = new AtomicBoolean();
        this.isDecommissioned = new AtomicBoolean(false);

        this.bePort = 0;
        this.httpPort = 0;
        this.beRpcPort = 0;
        this.arrowFlightPort = -1;

        this.backendState = Backend.BackendState.free.ordinal();

        this.decommissionType = DecommissionType.SystemDecommission.ordinal();
        this.status = Status.CONNECTING;
    }

    public ComputeNode(long id, String host, int heartbeatPort) {
        this.id = id;
        this.host = host;
        this.version = "";
        this.heartbeatPort = heartbeatPort;
        this.bePort = -1;
        this.httpPort = -1;
        this.beRpcPort = -1;
        this.arrowFlightPort = -1;
        this.lastUpdateMs = -1L;
        this.lastStartTime = -1L;

        this.isAlive = new AtomicBoolean(false);
        this.isDecommissioned = new AtomicBoolean(false);

        this.backendState = Backend.BackendState.free.ordinal();
        this.decommissionType = DecommissionType.SystemDecommission.ordinal();
        this.status = Status.CONNECTING;
    }

    public void setLastWriteFail(boolean lastWriteFail) {
        this.lastWriteFail = lastWriteFail;
    }

    public boolean getLastWriteFail() {
        return this.lastWriteFail;
    }

    public int getStarletPort() {
        return starletPort;
    }

    // for test only
    public void setStarletPort(int starletPort) {
        this.starletPort = starletPort;
    }

    public boolean isSetStoragePath() {
        return isSetStoragePath;
    }

    // for test only
    public void setIsStoragePath(boolean isSetStoragePath) {
        this.isSetStoragePath = isSetStoragePath;
    }

    public long getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    // The result will be in the IP string format.
    public String getIP() {
        return DnsCache.tryLookup(host);
    }

    public String getVersion() {
        return version;
    }

    public int getBePort() {
        return bePort;
    }

    public int getHeartbeatPort() {
        return heartbeatPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getBeRpcPort() {
        return beRpcPort;
    }

    public int getBrpcPort() {
        return brpcPort;
    }

    public int getArrowFlightPort() {
        return arrowFlightPort;
    }

    public void setArrowFlightPort(int arrowFlightPort) {
        this.arrowFlightPort = arrowFlightPort;
    }

    public TNetworkAddress getAddress() {
        return new TNetworkAddress(host, bePort);
    }

    public TNetworkAddress getBrpcAddress() {
        return new TNetworkAddress(host, brpcPort);
    }

    public TNetworkAddress getBrpcIpAddress() {
        return new TNetworkAddress(getIP(), brpcPort);
    }

    public TNetworkAddress getHttpAddress() {
        return new TNetworkAddress(host, httpPort);
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public void setWorkerGroupId(long workerGroupId) {
        this.workerGroupId = workerGroupId;
    }

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    // For TEST ONLY
    @VisibleForTesting
    public void updateOnce(int bePort, int httpPort, int beRpcPort) {
        if (this.bePort != bePort) {
            this.bePort = bePort;
        }

        if (this.httpPort != httpPort) {
            this.httpPort = httpPort;
        }

        if (this.beRpcPort != beRpcPort) {
            this.beRpcPort = beRpcPort;
        }

        long currentTime = System.currentTimeMillis();
        this.lastUpdateMs = currentTime;
        if (!isAlive()) {
            this.lastStartTime = currentTime;
            LOG.info("{} is alive,", this.toString());
        }
        setAlive(true);
        heartbeatErrMsg = "";
    }

    public boolean setDecommissioned(boolean isDecommissioned) {
        if (this.isDecommissioned.compareAndSet(!isDecommissioned, isDecommissioned)) {
            LOG.warn("{} set decommission: {}", this.toString(), isDecommissioned);
            return true;
        }
        return false;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setBackendState(Backend.BackendState state) {
        this.backendState = state.ordinal();
    }

    protected void setHeartbeatPort(int heartbeatPort) {
        this.heartbeatPort = heartbeatPort;
    }

    /** Set liveness and adjust the Internal status accordingly
     * |    Status    |  IsAlive |
     * |  CONNECTING  |   false  |
     * |     OK       |   true   |
     * |  SHUTDOWN    |   false  |
     * | DISCONNECTED |   false  |
     */
    public boolean setAlive(boolean isAlive) {
        boolean success = this.isAlive.compareAndSet(!isAlive, isAlive);
        if (success) {
            if (isAlive) {
                // force reset the status to OK under no condition
                this.status = Status.OK;
            } else {
                if (this.status == Status.OK) {
                    // force set to disconnected if target status is not alive but current status is OK
                    this.status = Status.DISCONNECTED;
                }
            }
        }
        return success;
    }

    public void setBePort(int agentPort) {
        this.bePort = agentPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public void setBeRpcPort(int beRpcPort) {
        this.beRpcPort = beRpcPort;
    }

    public void setBrpcPort(int brpcPort) {
        this.brpcPort = brpcPort;
    }

    public long getLastUpdateMs() {
        return this.lastUpdateMs;
    }

    public void setLastUpdateMs(long currentTime) {
        this.lastUpdateMs = currentTime;
    }

    public long getLastStartTime() {
        return this.lastStartTime;
    }

    public void setLastStartTime(long currentTime) {
        this.lastStartTime = currentTime;
    }

    public long getLastMissingHeartbeatTime() {
        return lastMissingHeartbeatTime;
    }

    public boolean isAlive() {
        return this.isAlive.get();
    }

    public boolean isDecommissioned() {
        return this.isDecommissioned.get();
    }

    public boolean isAvailable() {
        return this.status == Status.OK && !this.isDecommissioned.get();
    }

    public int getNumRunningQueries() {
        return numRunningQueries;
    }

    public long getMemUsedBytes() {
        return memUsedBytes;
    }

    public long getMemLimitBytes() {
        return memLimitBytes;
    }

    public double getMemUsedPct() {
        if (0 == memLimitBytes) {
            return 0;
        }
        return ((double) memUsedBytes) / memLimitBytes;
    }

    public int getCpuUsedPermille() {
        return cpuUsedPermille;
    }

    public void updateResourceUsage(int numRunningQueries, long memUsedBytes, int cpuUsedPermille) {
        this.numRunningQueries = numRunningQueries;
        // memLimitBytes is set by heartbeats instead of reports.
        this.memUsedBytes = memUsedBytes;
        this.cpuUsedPermille = cpuUsedPermille;
        this.lastUpdateResourceUsageMs = System.currentTimeMillis();
    }

    public void updateDataCacheMetrics(DataCacheMetrics dataCacheMetrics) {
        this.dataCacheMetrics = dataCacheMetrics;
    }

    public void updateResourceGroupUsage(List<Pair<ResourceGroup, TResourceGroupUsage>> groupAndUsages) {
        Map<Long, ResourceGroupUsage> newGroupIdToUsage = groupAndUsages.stream().collect(Collectors.toMap(
                groupAndUsage -> groupAndUsage.first.getId(),
                groupAndUsage -> ResourceGroupUsage.fromThrift(groupAndUsage.second, groupAndUsage.first)
        ));
        groupIdToUsage.set(newGroupIdToUsage);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static ComputeNode read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ComputeNode.class);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ComputeNode)) {
            return false;
        }

        ComputeNode computeNode = (ComputeNode) obj;

        return (id == computeNode.id) && (host.equals(computeNode.host)) && (heartbeatPort == computeNode.heartbeatPort)
                && (bePort == computeNode.bePort) && (isAlive.get() == computeNode.isAlive.get());
    }

    @Override
    public String toString() {
        return "ComputeNode [id=" + id + ", host=" + host + ", heartbeatPort=" + heartbeatPort + ", alive=" +
                isAlive.get() + ", status=" + status + "]";
    }

    public Backend.BackendState getBackendState() {
        switch (backendState) {
            case 0:
                return Backend.BackendState.using;
            case 1:
                return Backend.BackendState.offline;
            default:
                return Backend.BackendState.free;
        }
    }

    public void setDecommissionType(DecommissionType type) {
        decommissionType = type.ordinal();
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public AtomicBoolean getIsAlive() {
        return isAlive;
    }

    public void setDecommissionType(int decommissionType) {
        this.decommissionType = decommissionType;
    }

    public void setBackendState(int backendState) {
        this.backendState = backendState;
    }

    public DecommissionType getDecommissionType() {
        if (decommissionType == DecommissionType.ClusterDecommission.ordinal()) {
            return DecommissionType.ClusterDecommission;
        }
        return DecommissionType.SystemDecommission;
    }

    public int getCpuCores() {
        return cpuCores;
    }

    @VisibleForTesting
    public void setCpuCores(int cpuCores) {
        this.cpuCores = cpuCores;
    }

    @VisibleForTesting
    public void setMemLimitBytes(long memLimitBytes) {
        this.memLimitBytes = memLimitBytes;
    }

    /**
     * handle Compute node's heartbeat response.
     * return true if any port changed, or alive state is changed.
     */
    public boolean handleHbResponse(BackendHbResponse hbResponse, boolean isReplay) {
        boolean isChanged = false;
        boolean changedToShutdown = false;
        boolean becomeDead = false;
        if (hbResponse.getStatus() == HeartbeatResponse.HbStatus.OK) {
            if (this.version == null) {
                return false;
            }
            if (!this.version.equals(hbResponse.getVersion())) {
                isChanged = true;
                this.version = hbResponse.getVersion();
            }

            if (this.bePort != hbResponse.getBePort()) {
                isChanged = true;
                this.bePort = hbResponse.getBePort();
            }

            if (this.httpPort != hbResponse.getHttpPort()) {
                isChanged = true;
                this.httpPort = hbResponse.getHttpPort();
            }

            if (this.brpcPort != hbResponse.getBrpcPort()) {
                isChanged = true;
                this.brpcPort = hbResponse.getBrpcPort();
            }

            if (RunMode.isSharedDataMode() && this.starletPort != hbResponse.getStarletPort()) {
                isChanged = true;
                this.starletPort = hbResponse.getStarletPort();
            }

            if (this.arrowFlightPort != hbResponse.getArrowFlightPort()) {
                isChanged = true;
                this.arrowFlightPort = hbResponse.getArrowFlightPort();
            }

            if (RunMode.isSharedDataMode() && this.isSetStoragePath != hbResponse.isSetStoragePath()) {
                isChanged = true;
                this.isSetStoragePath = hbResponse.isSetStoragePath();
            }

            this.lastUpdateMs = hbResponse.getHbTime();
            // RebootTime will be `-1` if not set from backend.
            if (hbResponse.getRebootTime() > this.lastStartTime) {
                this.lastStartTime = hbResponse.getRebootTime();
                isChanged = true;
                // reboot time change means the BE has been restarted
                // but alive state may be not changed since the BE may be restarted in a short time
                // we need notify coordinator to cancel query
                becomeDead = true;
            }

            if (!isAlive.get()) {
                isChanged = true;
                if (hbResponse.getRebootTime() == -1) {
                    // Only update lastStartTime by hbResponse.hbTime if the RebootTime is not set from an OK-response.
                    // Just for backwards compatibility purpose in case the response is from an ancient version
                    this.lastStartTime = hbResponse.getHbTime();
                }
                LOG.info("{} is alive, last start time: {}, hbTime: {}", this.toString(), this.lastStartTime,
                        hbResponse.getHbTime());
                setAlive(true);
            }

            if (this.cpuCores != hbResponse.getCpuCores()) {
                isChanged = true;
                this.cpuCores = hbResponse.getCpuCores();

                // BackendCoreStat is a global state, checkpoint should not modify it.
                if (!GlobalStateMgr.isCheckpointThread()) {
                    BackendResourceStat.getInstance().setNumHardwareCoresOfBe(hbResponse.getBeId(), hbResponse.getCpuCores());
                }
            }

            if (this.memLimitBytes != hbResponse.getMemLimitBytes()) {
                isChanged = true;
                this.memLimitBytes = hbResponse.getMemLimitBytes();

                // BackendCoreStat is a global state, checkpoint should not modify it.
                if (!GlobalStateMgr.isCheckpointThread()) {
                    BackendResourceStat.getInstance().setMemLimitBytesOfBe(hbResponse.getBeId(), hbResponse.getMemLimitBytes());
                }
            }

            heartbeatErrMsg = "";
            this.heartbeatRetryTimes = 0;
        } else {
            boolean isShutdown = (hbResponse.getStatusCode() == TStatusCode.SHUTDOWN);
            String deadMessage = "";
            boolean needSetAlive = false;
            if (isShutdown) {
                heartbeatRetryTimes = 0;
                lastUpdateMs = hbResponse.getHbTime();
                deadMessage = "the target node is in shutting down";
                needSetAlive = true;
            } else {
                this.heartbeatRetryTimes++;
                if (this.heartbeatRetryTimes > Config.heartbeat_retry_times) {
                    deadMessage = "exceed heartbeatRetryTimes";
                    needSetAlive = true;
                    lastMissingHeartbeatTime = System.currentTimeMillis();
                }
            }

            if (needSetAlive) {
                if (isAlive.compareAndSet(true, false)) {
                    LOG.info("{} is dead due to {}", this, deadMessage);
                }
                heartbeatErrMsg = hbResponse.getMsg() == null ? "Unknown error" : hbResponse.getMsg();
                Status targetStatus = isShutdown ? Status.SHUTDOWN : Status.DISCONNECTED;
                if (status != targetStatus) {
                    status = targetStatus;
                    switch (targetStatus) {
                        case SHUTDOWN:
                            changedToShutdown = true;
                            break;
                        case DISCONNECTED:
                            becomeDead = true;
                            break;
                    }
                }
            }
            // When the master receives an error heartbeat info which status not ok, 
            // this heartbeat info also need to be synced to follower.
            // Since the failed heartbeat info also modifies fe's memory, (this.heartbeatRetryTimes++;)
            // if this heartbeat is not synchronized to the follower, 
            // that will cause the Follower and leader’s memory to be inconsistent
            isChanged = true;
        }
        if (!isReplay) {
            hbResponse.aliveStatus = isAlive.get() ?
                    HeartbeatResponse.AliveStatus.ALIVE : HeartbeatResponse.AliveStatus.NOT_ALIVE;
        } else {
            if (hbResponse.aliveStatus != null) {
                // Override the aliveStatus detected by the counter `heartbeatRetryTimes`, in two cases
                // 1. the follower has a different `heartbeatRetryTimes` value compared to the Leader, the follower
                //    must follow the leader's aliveStatus decision.
                // 2. editLog replay in leader FE's startup, where the value of `heartbeatRetryTimes` is changed.
                boolean newIsAlive = hbResponse.aliveStatus == HeartbeatResponse.AliveStatus.ALIVE;
                if (setAlive(newIsAlive)) {
                    LOG.info("{} alive status is changed to {}", this, newIsAlive);
                }
            }
        }

        if (!GlobalStateMgr.isCheckpointThread()) {
            if (changedToShutdown) {
                // only notify the resource usage changed when the node turns to SHUTDOWN status
                // Don't add it to CoordinatorMonitor, otherwise FE will proactively cancel queries
                // where the node is still trying to complete.
                GlobalStateMgr.getCurrentState().getResourceUsageMonitor().notifyBackendDead();
            }
            if (becomeDead) {
                // the node is firmly dead.
                CoordinatorMonitor.getInstance().addDeadBackend(id);
            }
        }
        return isChanged;
    }

    public Optional<DataCacheMetrics> getDataCacheMetrics() {
        return Optional.ofNullable(dataCacheMetrics);
    }

    public boolean isResourceUsageFresh() {
        if (!isAvailable()) {
            return false;
        }

        long currentMs = System.currentTimeMillis();
        // The resource usage is not fresh enough to decide whether it is overloaded.
        return currentMs - lastUpdateResourceUsageMs <= GlobalVariable.getQueryQueueResourceUsageIntervalMs();
    }

    public boolean isResourceOverloaded() {
        if (!isResourceUsageFresh()) {
            return false;
        }

        if (GlobalVariable.isQueryQueueCpuUsedPermilleLimitEffective() &&
                cpuUsedPermille >= GlobalVariable.getQueryQueueCpuUsedPermilleLimit()) {
            return true;
        }

        return GlobalVariable.isQueryQueueMemUsedPctLimitEffective() &&
                getMemUsedPct() >= GlobalVariable.getQueryQueueMemUsedPctLimit();
    }

    public Collection<ResourceGroupUsage> getResourceGroupUsages() {
        return groupIdToUsage.get().values();
    }

    public boolean isResourceGroupOverloaded(long groupId) {
        if (!isResourceUsageFresh()) {
            return false;
        }

        Map<Long, ResourceGroupUsage> currGroupIdToUsage = groupIdToUsage.get();

        if (!currGroupIdToUsage.containsKey(groupId)) {
            return false;
        }

        ResourceGroupUsage usage = currGroupIdToUsage.get(groupId);
        return usage.group.isMaxCpuCoresEffective() && usage.isCpuCoreUsagePermilleEffective() &&
                usage.cpuCoreUsagePermille >= usage.group.getMaxCpuCores() * 1000;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public void gsonPostProcess() {
        if (isAlive.get()) {
            // Upgraded from an old version where the status is not properly set.
            // reset the status according to the aliveness
            status = Status.OK;
        }
    }

    public static class ResourceGroupUsage {
        private final ResourceGroup group;
        private final int cpuCoreUsagePermille;
        private final long memUsageBytes;
        private final int numRunningQueries;

        private ResourceGroupUsage(ResourceGroup group, int cpuCoreUsagePermille, long memUsageBytes, int numRunningQueries) {
            this.group = group;
            this.cpuCoreUsagePermille = cpuCoreUsagePermille;
            this.memUsageBytes = memUsageBytes;
            this.numRunningQueries = numRunningQueries;
        }

        private static ResourceGroupUsage fromThrift(TResourceGroupUsage tUsage, ResourceGroup group) {
            return new ResourceGroupUsage(group, tUsage.getCpu_core_used_permille(), tUsage.getMem_used_bytes(),
                    tUsage.getNum_running_queries());
        }

        public boolean isCpuCoreUsagePermilleEffective() {
            return cpuCoreUsagePermille > 0;
        }

        public ResourceGroup getGroup() {
            return group;
        }

        public int getCpuCoreUsagePermille() {
            return cpuCoreUsagePermille;
        }

        public long getMemUsageBytes() {
            return memUsageBytes;
        }

        public int getNumRunningQueries() {
            return numRunningQueries;
        }
    }
}
