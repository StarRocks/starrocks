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

package com.starrocks.lake.snapshot;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Basic identity of one source-cluster storage volume, embedded in {@code snapshot_meta.json}. It
 * lets an external tool (e.g. the SaaS control plane) know which storage volumes the source cluster
 * had when rebuilding the {@code storage_volumes} section of cluster_snapshot.yaml for a
 * cross-cluster restore, even when the source cluster is no longer reachable. Only non-sensitive
 * identity is recorded (name/type/locations); credentials are never written, and target-side
 * credentials are supplied by the operator in the yaml.
 */
public class StorageVolumeMetaInfo {
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "type")
    private String type;
    @SerializedName(value = "locations")
    private List<String> locations;

    public StorageVolumeMetaInfo(String name, String type, List<String> locations) {
        this.name = name;
        this.type = type;
        this.locations = locations;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public List<String> getLocations() {
        return locations;
    }
}
