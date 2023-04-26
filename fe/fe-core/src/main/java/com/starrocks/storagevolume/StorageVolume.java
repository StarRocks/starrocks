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
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;

import java.util.HashMap;
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

    private CloudConfiguration cloudConfiguration;

    private Map<String, String> params;

    private String comment;

    private int refCount;

    private boolean enabled;

    public StorageVolume(String name, String svt, List<String> locations,
                         Map<String, String> params, boolean enabled, String comment) throws AnalysisException {
        this.name = name;
        this.svt = toStorageVolumeType(svt);
        this.locations = locations;
        this.comment = comment;
        this.refCount = 0;
        this.enabled = enabled;
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(params);
        if (!isValidCloudConfiguration()) {
            throw new AnalysisException("Storage params is not valid");
        }
        this.params = params;
    }

    public void setCloudConfiguration(Map<String, String> params) throws AnalysisException {
        Map<String, String> newParams = new HashMap<>(this.params);
        newParams.putAll(params);
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(newParams);
        if (!isValidCloudConfiguration()) {
            throw new AnalysisException("Storage params is not valid");
        }
        this.params = newParams;
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
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

    private boolean isValidCloudConfiguration() {
        switch (svt) {
            case S3:
                return cloudConfiguration.getCloudType() == CloudType.AWS;
            case HDFS:
                return cloudConfiguration.getCloudType() == CloudType.HDFS;
            default:
                return false;
        }
    }
}
