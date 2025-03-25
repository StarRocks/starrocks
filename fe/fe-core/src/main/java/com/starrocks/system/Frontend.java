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
import com.starrocks.common.io.JsonWriter;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Frontend extends JsonWriter {
    public static final Logger LOG = LogManager.getLogger(Frontend.class);

    @SerializedName(value = "r")
    private FrontendNodeType role;
    @SerializedName(value = "n")
    private String nodeName;
    @SerializedName(value = "h")
    private String host;
    @SerializedName(value = "e")
    private int editLogPort;
    @SerializedName(value = "fid")
    private int fid = -1;

    private int queryPort;
    private int rpcPort;

    private long replayedJournalId;
    private long lastUpdateTime;
    private long startTime;
    private String heartbeatErrMsg = "";
    private String feVersion;

    private boolean isAlive = false;

    private int heartbeatRetryTimes = 0;

    private float heapUsedPercent;

    public Frontend() {
    }

    public Frontend(FrontendNodeType role, String nodeName, String host, int editLogPort) {
        this.role = role;
        this.nodeName = nodeName;
        this.host = host;
        this.editLogPort = editLogPort;
    }

    public Frontend(int fid, FrontendNodeType role, String nodeName, String host, int editLogPort) {
        this.fid = fid;
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

    public float getHeapUsedPercent() {
        return heapUsedPercent;
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

    public int getFid() {
        return fid;
    }

    public void setFid(int fid) {
        this.fid = fid;
    }

    /**
     * handle Frontend's heartbeat response.
     */
    public boolean handleHbResponse(FrontendHbResponse hbResponse, boolean isReplay) {
        boolean prevIsAlive = isAlive;
        long prevStartTime = startTime;
        if (hbResponse.getStatus() == HbStatus.OK) {
            isAlive = true;
            queryPort = hbResponse.getQueryPort();
            rpcPort = hbResponse.getRpcPort();
            replayedJournalId = hbResponse.getReplayedJournalId();
            lastUpdateTime = hbResponse.getHbTime();
            startTime = hbResponse.getFeStartTime();
            feVersion = hbResponse.getFeVersion();
            heartbeatErrMsg = "";
            heartbeatRetryTimes = 0;
            heapUsedPercent = hbResponse.getHeapUsedPercent();
        } else {
            if (this.heartbeatRetryTimes < Config.heartbeat_retry_times) {
                this.heartbeatRetryTimes++;
            } else {
                if (isAlive) {
                    isAlive = false;
                }
                heartbeatErrMsg = hbResponse.getMsg() == null ? "Unknown error" : hbResponse.getMsg();
            }
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

        try {
            if (!prevIsAlive && isAlive) {
                changeToAlive(isReplay);
            } else if (prevIsAlive && !isAlive) {
                changeToDead(isReplay);
            } else if (prevStartTime != 0 && prevStartTime != startTime) {
                restartHappened(isReplay);
            }
        } catch (Throwable t) {
            LOG.warn("call status hook failed", t);
        }

        return true;
    }

    private void changeToAlive(boolean isReplay) {
        if (!isReplay && GlobalStateMgr.getCurrentState().getHaProtocol() instanceof BDBHA) {
            BDBHA ha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
            ha.removeUnstableNode(host, GlobalStateMgr.getCurrentState().getNodeMgr().getFollowerCnt());
        }
    }

    private void changeToDead(boolean isReplay) {
        if (!GlobalStateMgr.isCheckpointThread()) {
            GlobalStateMgr.getCurrentState().getSlotManager().notifyFrontendDeadAsync(nodeName);
        }

        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getCheckpointController().cancelCheckpoint(nodeName, "FE is dead");
            if (RunMode.isSharedDataMode()) {
                StarMgrServer.getCurrentState().getCheckpointController().cancelCheckpoint(nodeName, "FE is dead");
            }
        }
    }

    /**
     * restartHappened will be called when the restart time interval is in
     * (heartbeat_retry_times * heartbeat_timeout_second).
     * If the restart time interval exceed (heartbeat_retry_times * heartbeat_timeout_second),
     * changeToAlive will be called.
     */
    private void restartHappened(boolean isReplay) {
        if (!GlobalStateMgr.isCheckpointThread()) {
            GlobalStateMgr.getCurrentState().getSlotManager().notifyFrontendRestartAsync(nodeName, startTime);
        }

        // When the Worker node is its own node, the Checkpoint can be started at startup.
        // Therefore, if the restarted node is its own node, do not cancel the Checkpoint.
        if (!isReplay
                && !nodeName.equals(GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName())) {
            GlobalStateMgr.getCurrentState().getCheckpointController().workerRestarted(nodeName, startTime);
            if (RunMode.isSharedDataMode()) {
                StarMgrServer.getCurrentState().getCheckpointController().workerRestarted(nodeName, startTime);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name: ").append(nodeName).append(", role: ").append(role.name());
        sb.append(", ").append(host).append(":").append(editLogPort);
        return sb.toString();
    }
}
