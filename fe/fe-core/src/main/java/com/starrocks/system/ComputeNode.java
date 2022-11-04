// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.system;

import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.DecommissionType;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.CoordinatorMonitor;
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

    @SerializedName("label")
    private String label;
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
    @SerializedName("ownerClusterName")
    private volatile String ownerClusterName;
    // to index the state in some cluster
    @SerializedName("backendState")
    private volatile int backendState;
    // private BackendState backendState;

    @SerializedName("heartbeatErrMsg")
    private String heartbeatErrMsg = "";

    @SerializedName("lastMissingHeartbeatTime")
    private long lastMissingHeartbeatTime = -1;

    @SerializedName("heartbeatRetryTimes")
    private int heartbeatRetryTimes = 0;

    // port of starlet on BE
    @SerializedName("starletPort")
    private volatile int starletPort;

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

        this.ownerClusterName = "";
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

        this.ownerClusterName = "";
        this.backendState = Backend.BackendState.free.ordinal();
        this.decommissionType = DecommissionType.SystemDecommission.ordinal();
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

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
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

    public void clearClusterName() {
        ownerClusterName = "";
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
        boolean isChanged = false;
        if (hbResponse.getStatus() == HeartbeatResponse.HbStatus.OK) {
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

            if (Config.integrate_starmgr && this.starletPort != hbResponse.getStarletPort()) {
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
                    CoordinatorMonitor.getInstance().addDeadBackend(id);
                    LOG.info("{} is dead,", this.toString());
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
                isAlive.getAndSet(hbResponse.aliveStatus == HeartbeatResponse.AliveStatus.ALIVE);
                heartbeatRetryTimes = 0;
            }
        }
        return isChanged;
    }
}
