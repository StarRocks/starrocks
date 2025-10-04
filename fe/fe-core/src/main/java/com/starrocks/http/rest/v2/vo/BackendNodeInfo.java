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
import com.starrocks.system.Backend;

public class BackendNodeInfo extends ComputeNodeInfo {

    @SerializedName("lastReportTabletsTime")
    private String lastReportTabletsTime;

    @SerializedName("memUsed")
    private Long memUsed;

    @SerializedName("memLimit")
    private Long memLimit;

    @SerializedName("dataUsedCapacity")
    private Long dataUsedCapacity;

    @SerializedName("dataTotalCapacity")
    private Long dataTotalCapacity;

    @SerializedName("availableCapacity")
    private Long availableCapacity;

    @SerializedName("totalCapacity")
    private Long totalCapacity;

    public BackendNodeInfo(Backend backend) {
        super(backend);
        this.lastReportTabletsTime = backend.getBackendStatus().lastSuccessReportTabletsTime;
        this.memUsed = backend.getMemUsedBytes();
        this.memLimit = backend.getMemLimitBytes();
        this.dataUsedCapacity = backend.getDataUsedCapacityB();
        this.dataTotalCapacity = backend.getDataTotalCapacityB();
        this.availableCapacity = backend.getAvailableCapacityB();
        this.totalCapacity = backend.getTotalCapacityB();
    }
}
