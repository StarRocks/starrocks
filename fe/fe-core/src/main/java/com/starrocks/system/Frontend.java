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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/Frontend.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.system;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.HeartbeatResponse.HbStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Frontend implements Writable {
    @SerializedName(value = "r")
    private FrontendNodeType role;
    @SerializedName(value = "n")
    private String nodeName;
    @SerializedName(value = "h")
    private String host;
    @SerializedName(value = "e")
    private int editLogPort;

    private int queryPort;
    private int rpcPort;

    private long replayedJournalId;
    private long lastUpdateTime;
    private long startTime;
    private String heartbeatErrMsg = "";
    private String feVersion;

    private boolean isAlive = false;

    private int heartbeatRetryTimes = 0;

    public Frontend() {
    }

    public Frontend(FrontendNodeType role, String nodeName, String host, int editLogPort) {
        this.role = role;
        this.nodeName = nodeName;
        this.host = host;
        this.editLogPort = editLogPort;
    }

    public FrontendNodeType getRole() {
        return this.role;
    }

    public String getHost() {
        return this.host;
    }

    public String getNodeName() {
        return nodeName;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public int getEditLogPort() {
        return this.editLogPort;
    }

    public long getReplayedJournalId() {
        return replayedJournalId;
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getFeVersion() {
        return feVersion;
    }

    public void updateHostAndEditLogPort(String host, int editLogPort) {
        this.host = host;
        this.editLogPort = editLogPort;
    }

    @VisibleForTesting
    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    @VisibleForTesting
    public void setAlive(boolean isAlive) {
        this.isAlive = isAlive;
    }

    /**
     * handle Frontend's heartbeat response.
     * Because the replayed journal id is very likely to be changed at each heartbeat response,
     * so we simple return true if the heartbeat status is OK.
     * But if heartbeat status is BAD, only return true if it is the first time to transfer from alive to dead.
     */
    public boolean handleHbResponse(FrontendHbResponse hbResponse, boolean isReplay) {
        boolean isChanged = false;
        boolean prevIsAlive = isAlive;
        long prevStartTime = startTime;
        if (hbResponse.getStatus() == HbStatus.OK) {
            if (!isAlive && !isReplay) {
                if (GlobalStateMgr.getCurrentState().getHaProtocol() instanceof BDBHA) {
                    BDBHA ha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
                    ha.removeUnstableNode(host, GlobalStateMgr.getCurrentState().getFollowerCnt());
                }
            }
            isAlive = true;
            queryPort = hbResponse.getQueryPort();
            rpcPort = hbResponse.getRpcPort();
            replayedJournalId = hbResponse.getReplayedJournalId();
            lastUpdateTime = hbResponse.getHbTime();
            startTime = hbResponse.getFeStartTime();
            feVersion = hbResponse.getFeVersion();
            heartbeatErrMsg = "";
            isChanged = true;
            this.heartbeatRetryTimes = 0;
        } else {
            if (this.heartbeatRetryTimes < Config.heartbeat_retry_times) {
                this.heartbeatRetryTimes++;
            } else {
                if (isAlive) {
                    isAlive = false;
                }
                heartbeatErrMsg = hbResponse.getMsg() == null ? "Unknown error" : hbResponse.getMsg();
            }
            // When the master receives an error heartbeat info which status not ok, 
            // this heartbeat info also need to be synced to follower.
            // Since the failed heartbeat info also modifies fe's memory, (this.heartbeatRetryTimes++;)
            // if this heartbeat is not synchronized to the follower, 
            // that will cause the Follower and leader’s memory to be inconsistent
            isChanged = true;
        }
        if (!isReplay) {
            hbResponse.aliveStatus = isAlive ?
                HeartbeatResponse.AliveStatus.ALIVE : HeartbeatResponse.AliveStatus.NOT_ALIVE;
        } else {
            if (hbResponse.aliveStatus != null) {
                // The metadata before the upgrade does not contain hbResponse.aliveStatus,
                // in which case the alive status needs to be handled according to the original logic
                isAlive = hbResponse.aliveStatus == HeartbeatResponse.AliveStatus.ALIVE;
                heartbeatRetryTimes = 0;
            }
        }

        if (!GlobalStateMgr.isCheckpointThread()) {
            if (prevIsAlive && !isAlive) {
                GlobalStateMgr.getCurrentState().getSlotManager().notifyFrontendDeadAsync(nodeName);
            } else if (prevStartTime != 0 && prevStartTime != startTime) {
                GlobalStateMgr.getCurrentState().getSlotManager().notifyFrontendRestartAsync(nodeName, startTime);
            }
        }

        return isChanged;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, role.name());
        Text.writeString(out, host);
        out.writeInt(editLogPort);
        Text.writeString(out, nodeName);
    }

    public void readFields(DataInput in) throws IOException {
        role = FrontendNodeType.valueOf(Text.readString(in));
        host = Text.readString(in);
        editLogPort = in.readInt();
        nodeName = Text.readString(in);
    }

    public static Frontend read(DataInput in) throws IOException {
        Frontend frontend = new Frontend();
        frontend.readFields(in);
        return frontend;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name: ").append(nodeName).append(", role: ").append(role.name());
        sb.append(", ").append(host).append(":").append(editLogPort);
        return sb.toString();
    }
}
