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

package com.starrocks.credential.azure;

import com.staros.proto.FileStoreInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class AzureCloudConfiguration implements CloudConfiguration {

    private final AzureStorageCloudCredential azureStorageCloudCredential;

    public AzureCloudConfiguration(AzureStorageCloudCredential azureStorageCloudCredential) {
        this.azureStorageCloudCredential = azureStorageCloudCredential;
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        tCloudConfiguration.setCloud_type(TCloudType.AZURE);

        Map<String, String> properties = new HashMap<>();
        azureStorageCloudCredential.toThrift(properties);
        tCloudConfiguration.setCloud_properties_v2(properties);
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        azureStorageCloudCredential.applyToConfiguration(configuration);
    }

    @Override
    public CloudType getCloudType() {
        return CloudType.AZURE;
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        return azureStorageCloudCredential.toFileStoreInfo();
    }

    @Override
    public String getCredentialString() {
        return "AzureCloudConfiguration{" +
                "azureStorageCloudCredential=" + azureStorageCloudCredential +
                '}';
    }
}
