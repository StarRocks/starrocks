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

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.DecommissionType;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class extends the primary identifier of a compute node with computing capabilities
 * and no storage capacity。
 */
public class ComputeNode implements IComputable, Writable {
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
    @SerializedName("cpuCores")
    private volatile int cpuCores = 0; // Cpu cores of node

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

    private volatile int numRunningQueries = 0;
    private volatile long memLimitBytes = 0;
    private volatile long memUsedBytes = 0;
    private volatile int cpuUsedPermille = 0;
    private volatile long lastUpdateResourceUsageMs = 0;

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

        this.backendState = Backend.BackendState.free.ordinal();

        this.decommissionType = DecommissionType.SystemDecommission.ordinal();
    }

    public ComputeNode(long id, String host, int heartbeatPort) {
        this.id = id;
        this.host = host;
        this.version = "";
        this.heartbeatPort = heartbeatPort;
        this.bePort = -1;
        this.httpPort = -1;
        this.beRpcPort = -1;
        this.lastUpdateMs = -1L;
        this.lastStartTime = -1L;

        this.isAlive = new AtomicBoolean(false);
        this.isDecommissioned = new AtomicBoolean(false);

        this.backendState = Backend.BackendState.free.ordinal();
        this.decommissionType = DecommissionType.SystemDecommission.ordinal();
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

    public long getId() {
        return id;
    }

    public String getHost() {
        return host;
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

    public TNetworkAddress getBrpcAddress() {
        return new TNetworkAddress(host, brpcPort);
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    // for test only
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
        if (!isAlive.get()) {
            this.lastStartTime = currentTime;
            LOG.info("{} is alive,", this.toString());
            this.isAlive.set(true);
        }

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

    public void setAlive(boolean isAlive) {
        this.isAlive.set(isAlive);
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

    public void setIsAlive(boolean isAlive) {
        this.isAlive.set(isAlive);
    }

    public boolean isDecommissioned() {
        return this.isDecommissioned.get();
    }

    public void setIsDecommissioned(boolean isDecommissioned) {
        this.isDecommissioned.set(isDecommissioned);
    }

    public boolean isAvailable() {
        return this.isAlive.get() && !this.isDecommissioned.get();
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

    public void updateResourceUsage(int numRunningQueries, long memLimitBytes, long memUsedBytes,
                                    int cpuUsedPermille) {

        this.numRunningQueries = numRunningQueries;
        this.memLimitBytes = memLimitBytes;
        this.memUsedBytes = memUsedBytes;
        this.cpuUsedPermille = cpuUsedPermille;
        this.lastUpdateResourceUsageMs = System.currentTimeMillis();
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
                isAlive.get() + "]";
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

    public void setIsAlive(AtomicBoolean isAlive) {
        this.isAlive = isAlive;
    }

    public AtomicBoolean getIsDecommissioned() {
        return isDecommissioned;
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

    public void setCpuCores(int cpuCores) {
        this.cpuCores = cpuCores;
    }

    /**
     * handle Compute node's heartbeat response.
     * return true if any port changed, or alive state is changed.
     */
    public boolean handleHbResponse(BackendHbResponse hbResponse, boolean isReplay) {
        boolean becomeDead = false;
        boolean isChanged = false;
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

            if (RunMode.allowCreateLakeTable() && this.starletPort != hbResponse.getStarletPort()) {
                isChanged = true;
                this.starletPort = hbResponse.getStarletPort();
            }

            this.lastUpdateMs = hbResponse.getHbTime();
            if (!isAlive.get()) {
                isChanged = true;
                // From version 2.5 we not use isAlive to determine whether to update the lastStartTime 
                // This line to set 'lastStartTime' will be removed in due time
                this.lastStartTime = hbResponse.getHbTime();
                LOG.info("{} is alive, last start time: {}", this.toString(), hbResponse.getHbTime());
                this.isAlive.set(true);
            } else if (this.lastStartTime <= 0) {
                this.lastStartTime = hbResponse.getHbTime();
            }

            if (hbResponse.getRebootTime() > this.lastStartTime) {
                this.lastStartTime = hbResponse.getRebootTime();
                isChanged = true;
                // reboot time change means the BE has been restarted
                // but alive state may be not changed since the BE may be restarted in a short time
                // we need notify coordinator to cancel query
                becomeDead = true;
            }

            if (this.cpuCores != hbResponse.getCpuCores()) {
                isChanged = true;
                this.cpuCores = hbResponse.getCpuCores();
                BackendCoreStat.setNumOfHardwareCoresOfBe(hbResponse.getBeId(), hbResponse.getCpuCores());
            }

            heartbeatErrMsg = "";
            this.heartbeatRetryTimes = 0;
        } else {
            if (this.heartbeatRetryTimes < Config.heartbeat_retry_times) {
                this.heartbeatRetryTimes++;
            } else {
                if (isAlive.compareAndSet(true, false)) {
                    becomeDead = true;
                    LOG.info("{} is dead due to exceed heartbeatRetryTimes", this);
                }
                heartbeatErrMsg = hbResponse.getMsg() == null ? "Unknown error" : hbResponse.getMsg();
                lastMissingHeartbeatTime = System.currentTimeMillis();
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
                // The metadata before the upgrade does not contain hbResponse.aliveStatus,
                // in which case the alive status needs to be handled according to the original logic
                boolean newIsAlive = hbResponse.aliveStatus == HeartbeatResponse.AliveStatus.ALIVE;
                if (isAlive.compareAndSet(!newIsAlive, newIsAlive)) {
                    becomeDead = !newIsAlive;
                    LOG.info("{} alive status is changed to {}", this, newIsAlive);
                }
                heartbeatRetryTimes = 0;
            }
        }

        if (becomeDead) {
            CoordinatorMonitor.getInstance().addDeadBackend(id);
        }

        return isChanged;
    }

    public boolean isResourceOverloaded() {
        if (!isAvailable()) {
            return false;
        }

        long currentMs = System.currentTimeMillis();
        if (currentMs - lastUpdateResourceUsageMs > GlobalVariable.getQueryQueueResourceUsageIntervalMs()) {
            // The resource usage is not fresh enough to decide whether it is overloaded.
            return false;
        }

        if (GlobalVariable.isQueryQueueConcurrencyLimitEffective() &&
                numRunningQueries >= GlobalVariable.getQueryQueueConcurrencyLimit()) {
            return true;
        }

        if (GlobalVariable.isQueryQueueCpuUsedPermilleLimitEffective() &&
                cpuUsedPermille >= GlobalVariable.getQueryQueueCpuUsedPermilleLimit()) {
            return true;
        }

        return GlobalVariable.isQueryQueueMemUsedPctLimitEffective() &&
                getMemUsedPct() >= GlobalVariable.getQueryQueueMemUsedPctLimit();
    }
}
