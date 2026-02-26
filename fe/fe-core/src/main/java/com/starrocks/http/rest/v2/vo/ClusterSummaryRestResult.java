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

public class ClusterSummaryRestResult {

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

    private ClusterSummaryRestResult(Builder builder) {
        this.dbCount = builder.dbCount;
        this.tableCount = builder.tableCount;
        this.diskUsedCapacity = builder.diskUsedCapacity;
        this.diskAvailableCapacity = builder.diskAvailableCapacity;
        this.diskTotalCapacity = builder.diskTotalCapacity;
        this.totalBackendNum = builder.totalBackendNum;
        this.aliveBackendNum = builder.aliveBackendNum;
        this.totalCnNum = builder.totalCnNum;
        this.aliveCnNum = builder.aliveCnNum;
        this.totalFeNum = builder.totalFeNum;
        this.aliveFeNum = builder.aliveFeNum;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Long dbCount;
        private Long tableCount;
        private Long diskUsedCapacity;
        private Long diskAvailableCapacity;
        private Long diskTotalCapacity;
        private Long totalBackendNum;
        private Long aliveBackendNum;
        private Long totalCnNum;
        private Long aliveCnNum;
        private Long totalFeNum;
        private Long aliveFeNum;

        public Builder dbCount(Long dbCount) {
            this.dbCount = dbCount;
            return this;
        }

        public Builder tableCount(Long tableCount) {
            this.tableCount = tableCount;
            return this;
        }

        public Builder diskUsedCapacity(Long diskUsedCapacity) {
            this.diskUsedCapacity = diskUsedCapacity;
            return this;
        }

        public Builder diskAvailableCapacity(Long diskAvailableCapacity) {
            this.diskAvailableCapacity = diskAvailableCapacity;
            return this;
        }

        public Builder diskTotalCapacity(Long diskTotalCapacity) {
            this.diskTotalCapacity = diskTotalCapacity;
            return this;
        }

        public Builder totalBackendNum(Long totalBackendNum) {
            this.totalBackendNum = totalBackendNum;
            return this;
        }

        public Builder aliveBackendNum(Long aliveBackendNum) {
            this.aliveBackendNum = aliveBackendNum;
            return this;
        }

        public Builder totalCnNum(Long totalCnNum) {
            this.totalCnNum = totalCnNum;
            return this;
        }

        public Builder aliveCnNum(Long aliveCnNum) {
            this.aliveCnNum = aliveCnNum;
            return this;
        }

        public Builder totalFeNum(Long totalFeNum) {
            this.totalFeNum = totalFeNum;
            return this;
        }

        public Builder aliveFeNum(Long aliveFeNum) {
            this.aliveFeNum = aliveFeNum;
            return this;
        }

        public ClusterSummaryRestResult build() {
            return new ClusterSummaryRestResult(this);
        }
    }
}

