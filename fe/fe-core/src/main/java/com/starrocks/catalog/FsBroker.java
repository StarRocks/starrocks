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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/FsBroker.java

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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.BrokerHbResponse;
import com.starrocks.system.HeartbeatResponse;
import com.starrocks.system.HeartbeatResponse.HbStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FsBroker implements Writable, Comparable<FsBroker> {
    @SerializedName(value = "ip")
    public String ip;
    @SerializedName(value = "port")
    public int port;
    // msg for ping result
    public String heartbeatErrMsg = "";
    public long lastUpdateTime = -1;

    @SerializedName(value = "lastStartTime")
    public long lastStartTime = -1;
    @SerializedName(value = "isAlive")
    public boolean isAlive;

    private int heartbeatRetryTimes = 0;

    public FsBroker() {
    }

    public FsBroker(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    /*
     * handle Broker's heartbeat response.
     * return true if alive state is changed.
     */
    public boolean handleHbResponse(BrokerHbResponse hbResponse, boolean isReplay) {
        boolean isChanged = false;
        if (hbResponse.getStatus() == HbStatus.OK) {
            if (!isAlive) {
                isAlive = true;
                isChanged = true;
                lastStartTime = hbResponse.getHbTime();
            } else if (lastStartTime == -1) {
                lastStartTime = hbResponse.getHbTime();
            }
            lastUpdateTime = hbResponse.getHbTime();
            heartbeatErrMsg = "";
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
        return isChanged;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FsBroker)) {
            return false;
        }

        FsBroker that = (FsBroker) o;

        if (port != that.port) {
            return false;
        }
        return ip.equals(that.ip);

    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public int compareTo(FsBroker o) {
        int ret = ip.compareTo(o.ip);
        if (ret != 0) {
            return ret;
        }
        return port - o.port;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    private void readFields(DataInput in) throws IOException {
        ip = Text.readString(in);
        port = in.readInt();
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }

    public static FsBroker readIn(DataInput in) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_73) {
            FsBroker broker = new FsBroker();
            broker.readFields(in);
            return broker;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, FsBroker.class);
        }
    }
}

