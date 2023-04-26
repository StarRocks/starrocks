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

import com.starrocks.common.AnalysisException;

import java.util.List;
import java.util.Map;

public class StorageVolume {
    public enum StorageVolumeType {
        UNKNOWN,
        S3,
        HDFS
    }

    private String name;

    private StorageVolumeType svt;

    private List<String> locations;

    private StorageParams storageParams;

    private String comment;

    private int refCount;

    private boolean enabled;

    public StorageVolume(String name, String svt, List<String> locations,
                         Map<String, String> storageParams, boolean enabled, String comment) throws AnalysisException {
        this.name = name;
        this.svt = toStorageVolumeType(svt);
        this.locations = locations;
        this.comment = comment;
        this.refCount = 0;
        this.enabled = enabled;
        this.storageParams = toStorageParams(storageParams);
    }

    public void setStorageParams(Map<String, String> params) throws AnalysisException {
        this.storageParams = toStorageParams(params);
    }

    public StorageParams getStorageParams() {
        return storageParams;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    private StorageVolumeType toStorageVolumeType(String svt) {
        switch (svt.toLowerCase()) {
            case "s3":
                return StorageVolumeType.S3;
            case "hdfs":
                return StorageVolumeType.HDFS;
            default:
                return StorageVolumeType.UNKNOWN;
        }
    }

    private StorageParams toStorageParams(Map<String, String> storageParams) throws AnalysisException {
        switch (svt) {
            case S3:
                return new S3StorageParams(storageParams);
            case HDFS:
                return new HDFSStorageParams(storageParams);
            default:
                return null;
        }
    }
}
