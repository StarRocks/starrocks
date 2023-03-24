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

import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.DecommissionType;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataNode implements Writable {
    private static final Logger LOG = LogManager.getLogger(DataNode.class);


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

    @SerializedName("isDecommissioned")
    private final AtomicBoolean isDecommissioned;
    @SerializedName("decommissionType")
    private volatile int decommissionType;
    @SerializedName("ownerClusterName")
    private volatile String ownerClusterName;


    @SerializedName("isAlive")
    private AtomicBoolean isAlive;

    @SerializedName("lastWriteFail")
    private volatile boolean lastWriteFail = false;

    private volatile long memLimitBytes = 0;
    private volatile long memUsedBytes = 0;
    private volatile int cpuUsedPermille = 0;
    private volatile long lastUpdateResourceUsageMs = 0;

    public DataNode() {
        this.host = "";
        this.version = "";
        this.isDecommissioned = new AtomicBoolean(false);

        this.bePort = 0;
        this.httpPort = 0;
        this.beRpcPort = 0;

        this.ownerClusterName = "";

        this.decommissionType = DecommissionType.SystemDecommission.ordinal();
    }

    public DataNode(long id, String host, int heartbeatPort) {
        this.id = id;
        this.host = host;
        this.version = "";
        this.heartbeatPort = heartbeatPort;
        this.bePort = -1;
        this.httpPort = -1;
        this.beRpcPort = -1;

        this.isDecommissioned = new AtomicBoolean(false);

        this.ownerClusterName = "";
        this.decommissionType = DecommissionType.SystemDecommission.ordinal();
    }

    public String getHost() {
        return host;
    }

    public int getHeartbeatPort() {
        return heartbeatPort;
    }

    public long getId() {
        return id;
    }

    public boolean isDecommissioned() {
        return this.isDecommissioned.get();
    }

    public boolean setDecommissioned(boolean isDecommissioned) {
        if (this.isDecommissioned.compareAndSet(!isDecommissioned, isDecommissioned)) {
            LOG.warn("{} set decommission: {}", this.toString(), isDecommissioned);
            return true;
        }
        return false;
    }

    public boolean isAlive() {
        return this.isAlive.get();
    }

    public boolean isAvailable() {
        return this.isAlive.get() && !this.isDecommissioned.get();
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static DataNode read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DataNode.class);
    }
}
