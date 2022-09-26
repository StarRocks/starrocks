// This file is made available under Elastic License 2.0.
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

import com.starrocks.catalog.Catalog;
import com.starrocks.common.Config;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.system.HeartbeatResponse.HbStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Frontend implements Writable {
    private FrontendNodeType role;
    private String nodeName;
    private String host;
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

    /**
     * handle Frontend's heartbeat response.
     * Because the replayed journal id is very likely to be changed at each heartbeat response,
     * so we simple return true if the heartbeat status is OK.
     * But if heartbeat status is BAD, only return true if it is the first time to transfer from alive to dead.
     */
    public boolean handleHbResponse(FrontendHbResponse hbResponse, boolean isReplay) {
        boolean isChanged = false;
        if (hbResponse.getStatus() == HbStatus.OK) {
            if (!isAlive && !isReplay) {
                BDBHA ha = (BDBHA) Catalog.getCurrentCatalog().getHaProtocol();
                ha.removeUnstableNode(host, Catalog.getCurrentCatalog().getFollowerCnt());
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
            // When the leader receives an error heartbeat info which status not ok, 
            // this heartbeat info also need to be synced to follower.
            // Since the failed heartbeat info also modifies fe's memory, (this.heartbeatRetryTimes++;)
            // if this heartbeat is not synchronized to the follower, 
            // that will cause the Follower and master’s memory to be inconsistent
            isChanged = true;
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
        if (role == FrontendNodeType.REPLICA) {
            // this is for compatibility.
            // we changed REPLICA to FOLLOWER
            role = FrontendNodeType.FOLLOWER;
        }
        host = Text.readString(in);
        editLogPort = in.readInt();
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_41) {
            nodeName = Text.readString(in);
        } else {
            nodeName = Catalog.genFeNodeName(host, editLogPort, true /* old style */);
        }
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
