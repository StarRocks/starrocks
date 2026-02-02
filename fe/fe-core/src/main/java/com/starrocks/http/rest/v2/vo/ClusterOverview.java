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

public class ClusterOverview {

    @SerializedName("dbCount")
    private Long dbCount;

    @SerializedName("tableCount")
    private Long tableCount;

    @SerializedName("diskUsedCapacity")
    private Long diskUsedCapacity;

    @SerializedName("diskAvailableCapacity")
    private Long diskAvailableCapacity;

    @SerializedName("diskTotalCapacity")
    private Long diskTotalCapacity;

    @SerializedName("totalBackendNum")
    private Long totalBackendNum;

    @SerializedName("aliveBackendNum")
    private Long aliveBackendNum;

    @SerializedName("totalCnNum")
    private Long totalCnNum;

    @SerializedName("aliveCnNum")
    private Long aliveCnNum;

    @SerializedName("totalFeNum")
    private Long totalFeNum;

    @SerializedName("aliveFeNum")
    private Long aliveFeNum;

    public ClusterOverview(Long dbCount, Long tableCount, Long diskUsedCapacity, Long diskAvailableCapacity,
                           Long diskTotalCapacity, Long totalBackendNum, Long aliveBackendNum, Long totalCnNum,
                           Long aliveCnNum, Long totalFeNum, Long aliveFeNum) {
        this.dbCount = dbCount;
        this.tableCount = tableCount;
        this.diskUsedCapacity = diskUsedCapacity;
        this.diskAvailableCapacity = diskAvailableCapacity;
        this.diskTotalCapacity = diskTotalCapacity;
        this.totalBackendNum = totalBackendNum;
        this.aliveBackendNum = aliveBackendNum;
        this.totalCnNum = totalCnNum;
        this.aliveCnNum = aliveCnNum;
        this.totalFeNum = totalFeNum;
        this.aliveFeNum = aliveFeNum;
    }

    public Long getDbCount() {
        return dbCount;
    }

    public void setDbCount(Long dbCount) {
        this.dbCount = dbCount;
    }

    public Long getTableCount() {
        return tableCount;
    }

    public void setTableCount(Long tableCount) {
        this.tableCount = tableCount;
    }

    public Long getDiskUsedCapacity() {
        return diskUsedCapacity;
    }

    public void setDiskUsedCapacity(Long diskUsedCapacity) {
        this.diskUsedCapacity = diskUsedCapacity;
    }

    public Long getDiskAvailableCapacity() {
        return diskAvailableCapacity;
    }

    public void setDiskAvailableCapacity(Long diskAvailableCapacity) {
        this.diskAvailableCapacity = diskAvailableCapacity;
    }

    public Long getDiskTotalCapacity() {
        return diskTotalCapacity;
    }

    public void setDiskTotalCapacity(Long diskTotalCapacity) {
        this.diskTotalCapacity = diskTotalCapacity;
    }

    public Long getTotalBackendNum() {
        return totalBackendNum;
    }

    public void setTotalBackendNum(Long totalBackendNum) {
        this.totalBackendNum = totalBackendNum;
    }

    public Long getAliveBackendNum() {
        return aliveBackendNum;
    }

    public void setAliveBackendNum(Long aliveBackendNum) {
        this.aliveBackendNum = aliveBackendNum;
    }

    public Long getTotalCnNum() {
        return totalCnNum;
    }

    public void setTotalCnNum(Long totalCnNum) {
        this.totalCnNum = totalCnNum;
    }

    public Long getAliveCnNum() {
        return aliveCnNum;
    }

    public void setAliveCnNum(Long aliveCnNum) {
        this.aliveCnNum = aliveCnNum;
    }

    public Long getTotalFeNum() {
        return totalFeNum;
    }

    public void setTotalFeNum(Long totalFeNum) {
        this.totalFeNum = totalFeNum;
    }

    public Long getAliveFeNum() {
        return aliveFeNum;
    }

    public void setAliveFeNum(Long aliveFeNum) {
        this.aliveFeNum = aliveFeNum;
    }
}
