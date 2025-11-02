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
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.Map;

public class UpdateBackendInfo implements Writable {

    @SerializedName("id")
    private final long id;

    @SerializedName("host")
    private String host;

    @SerializedName("isDecommissioned")
    private Boolean isDecommissioned;

    @SerializedName("loc")
    private Map<String, String> location;

    @SerializedName(value = "d")
    private Map<String, UpdateDiskInfo> diskInfoMap;

    public UpdateBackendInfo(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Boolean getDecommissioned() {
        return isDecommissioned;
    }

    public void setDecommissioned(Boolean decommissioned) {
        isDecommissioned = decommissioned;
    }

    public Map<String, String> getLocation() {
        return location;
    }

    public void setLocation(Map<String, String> location) {
        this.location = location;
    }

    public Map<String, UpdateDiskInfo> getDiskInfoMap() {
        return diskInfoMap;
    }

    public void setDiskInfoMap(Map<String, UpdateDiskInfo> diskInfoMap) {
        this.diskInfoMap = diskInfoMap;
    }
}
