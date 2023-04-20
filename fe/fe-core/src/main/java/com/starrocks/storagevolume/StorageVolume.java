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

package com.starrocks.storagevolume;

import com.starrocks.storagevolume.storageparams.StorageParams;

import java.util.Map;

public class StorageVolume {
    public enum StorageVolumeType {
        S3,
        HDFS,
        UNKNOWN
    }

    private String name;

    private StorageVolumeType svt;

    private String storageLocation;

    private StorageParams storageParams;

    private String comment;

    private int refCount;

    private Boolean enabled;

    public StorageVolume(String name, String svt, String storageLocation,
                         Map<String, String> storageParams, Boolean enabled, String comment) {
        this.name = name;
        this.svt = toStorageVolumeType(svt);
        this.storageLocation = storageLocation;
        this.comment = comment;
        refCount = 0;
        this.enabled = enabled;
    }

    public void setStorageParams(Map<String, String> params) {

    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void increaseRefCount() {
        ++refCount;
    }

    public void decreaseRefCount() {
        --refCount;
    }

    private StorageVolumeType toStorageVolumeType(String svt) {
        switch (svt) {
            case "S3":
                return StorageVolumeType.S3;
            case "HDFS":
                return StorageVolumeType.HDFS;
            default:
                return StorageVolumeType.UNKNOWN;
        }
    }
}
