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

package com.starrocks.http.rest.v2.vo;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.system.ComputeNode;

public class ComputeNodeInfo {

    @SerializedName("id")
    private Long id;

    @SerializedName("hostName")
    private String hostName;

    @SerializedName("heartPort")
    private Long heartPort;

    @SerializedName("bePort")
    private String bePort;

    @SerializedName("httpPort")
    private String httpPort;

    @SerializedName("brpcPort")
    private String brpcPort;

    @SerializedName("state")
    private String state;

    @SerializedName("startTime")
    private String startTime;

    @SerializedName("version")
    private String version;

    @SerializedName("lastUpdateTime")
    private String lastUpdateTime;

    @SerializedName("cpuUsedPermille")
    private int cpuUsedPermille;

    @SerializedName("cpuCores")
    private int cpuCores;

    @SerializedName("lastMissingHeartbeatTime")
    private Long lastMissingHeartbeatTime;

    @SerializedName("heartbeatErrMsg")
    private String heartbeatErrMsg;

    @SerializedName("isAlive")
    private boolean isAlive;

    public ComputeNodeInfo(ComputeNode node) {
        this.id = node.getId();
        this.hostName = node.getHost();
        this.heartPort = (long) node.getHeartbeatPort();
        this.bePort = String.valueOf(node.getBePort());
        this.httpPort = String.valueOf(node.getHttpPort());
        this.brpcPort = String.valueOf(node.getBrpcPort());
        this.state = node.getBackendState().name();
        this.startTime = TimeUtils.longToTimeString(node.getLastStartTime());
        this.version = node.getVersion();
        this.lastUpdateTime = TimeUtils.longToTimeString(node.getLastUpdateMs());
        this.cpuUsedPermille = node.getCpuUsedPermille();
        this.cpuCores = node.getCpuCores();
        this.lastMissingHeartbeatTime = node.getLastMissingHeartbeatTime();
        this.heartbeatErrMsg = node.getHeartbeatErrMsg();
        this.isAlive = node.getIsAlive().get();
    }
}
