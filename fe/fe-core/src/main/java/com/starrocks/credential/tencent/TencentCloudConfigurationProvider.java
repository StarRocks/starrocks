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

package com.starrocks.credential.tencent;

import com.google.common.base.Preconditions;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationProvider;

import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.TENCENT_COS_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.TENCENT_COS_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.TENCENT_COS_SECRET_KEY;

public class TencentCloudConfigurationProvider implements CloudConfigurationProvider {
    @Override
    public CloudConfiguration build(Map<String, String> properties) {
        Preconditions.checkNotNull(properties);

        TencentCloudCredential tencentCloudCredential = new TencentCloudCredential(
                properties.getOrDefault(TENCENT_COS_ACCESS_KEY, ""),
                properties.getOrDefault(TENCENT_COS_SECRET_KEY, ""),
                properties.getOrDefault(TENCENT_COS_ENDPOINT, "")
        );
        if (!tencentCloudCredential.validate()) {
            return null;
        }
        return new TencentCloudConfiguration(tencentCloudCredential);
    }
}
