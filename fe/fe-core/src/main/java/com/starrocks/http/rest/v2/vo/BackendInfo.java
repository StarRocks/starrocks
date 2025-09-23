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

public class BackendInfo {

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

    @SerializedName("lastReportTabletsTime")
    private String lastReportTabletsTime;

    @SerializedName("version")
    private String version;

    @SerializedName("lastUpdateTime")
    private String lastUpdateTime;

    @SerializedName("memUsed")
    private Long memUsed;

    @SerializedName("memLimit")
    private Long memLimit;

    @SerializedName("cpuUsedPermille")
    private int cpuUsedPermille;

    @SerializedName("cpuCores")
    private int cpuCores;

    @SerializedName("dataUsedCapacity")
    private Long dataUsedCapacity;

    @SerializedName("dataTotalCapacity")
    private Long dataTotalCapacity;

    @SerializedName("availableCapacity")
    private Long availableCapacity;

    @SerializedName("totalCapacity")
    private Long totalCapacity;

    @SerializedName("lastMissingHeartbeatTime")
    private Long lastMissingHeartbeatTime;

    @SerializedName("heartbeatErrMsg")
    private String heartbeatErrMsg;

    @SerializedName("isAlive")
    private boolean isAlive;

    public BackendInfo() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Long getHeartPort() {
        return heartPort;
    }

    public void setHeartPort(Long heartPort) {
        this.heartPort = heartPort;
    }

    public String getBePort() {
        return bePort;
    }

    public void setBePort(String bePort) {
        this.bePort = bePort;
    }

    public String getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(String httpPort) {
        this.httpPort = httpPort;
    }

    public String getBrpcPort() {
        return brpcPort;
    }

    public void setBrpcPort(String brpcPort) {
        this.brpcPort = brpcPort;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getLastReportTabletsTime() {
        return lastReportTabletsTime;
    }

    public void setLastReportTabletsTime(String lastReportTabletsTime) {
        this.lastReportTabletsTime = lastReportTabletsTime;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(String lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public Long getMemUsed() {
        return memUsed;
    }

    public void setMemUsed(Long memUsed) {
        this.memUsed = memUsed;
    }

    public Long getMemLimit() {
        return memLimit;
    }

    public void setMemLimit(Long memLimit) {
        this.memLimit = memLimit;
    }

    public int getCpuUsedPermille() {
        return cpuUsedPermille;
    }

    public void setCpuUsedPermille(int cpuUsedPermille) {
        this.cpuUsedPermille = cpuUsedPermille;
    }

    public int getCpuCores() {
        return cpuCores;
    }

    public void setCpuCores(int cpuCores) {
        this.cpuCores = cpuCores;
    }

    public Long getDataUsedCapacity() {
        return dataUsedCapacity;
    }

    public void setDataUsedCapacity(Long dataUsedCapacity) {
        this.dataUsedCapacity = dataUsedCapacity;
    }

    public Long getDataTotalCapacity() {
        return dataTotalCapacity;
    }

    public void setDataTotalCapacity(Long dataTotalCapacity) {
        this.dataTotalCapacity = dataTotalCapacity;
    }

    public Long getAvailableCapacity() {
        return availableCapacity;
    }

    public void setAvailableCapacity(Long availableCapacity) {
        this.availableCapacity = availableCapacity;
    }

    public Long getTotalCapacity() {
        return totalCapacity;
    }

    public void setTotalCapacity(Long totalCapacity) {
        this.totalCapacity = totalCapacity;
    }

    public Long getLastMissingHeartbeatTime() {
        return lastMissingHeartbeatTime;
    }

    public void setLastMissingHeartbeatTime(Long lastMissingHeartbeatTime) {
        this.lastMissingHeartbeatTime = lastMissingHeartbeatTime;
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    public void setHeartbeatErrMsg(String heartbeatErrMsg) {
        this.heartbeatErrMsg = heartbeatErrMsg;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
    }
}
