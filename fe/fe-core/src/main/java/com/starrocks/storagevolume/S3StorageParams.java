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
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;

import java.util.Map;


public class S3StorageParams implements StorageParams {
    private String region;
    private String endpoint;
    private CloudConfiguration cloudConfiguration;

    @Override
    public StorageVolume.StorageVolumeType type() {
        return StorageVolume.StorageVolumeType.S3;
    }

    public S3StorageParams(Map<String, String> params) throws AnalysisException {
        region = params.get(CloudConfigurationConstants.AWS_S3_REGION);
        endpoint = params.get(CloudConfigurationConstants.AWS_S3_ENDPOINT);
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(params);
        if (cloudConfiguration.getCloudType() != CloudType.AWS) {
            throw new AnalysisException("Storage params is not valid");
        }
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}
