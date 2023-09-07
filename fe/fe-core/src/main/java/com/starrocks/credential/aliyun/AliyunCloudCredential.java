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

package com.starrocks.credential.aliyun;

import com.google.common.base.Preconditions;
import com.staros.proto.FileStoreInfo;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class AliyunCloudCredential implements CloudCredential {

    private final String accessKey;
    private final String secretKey;
    private final String endpoint;

    public AliyunCloudCredential(String accessKey, String secretKey, String endpoint) {
        Preconditions.checkNotNull(accessKey);
        Preconditions.checkNotNull(secretKey);
        Preconditions.checkNotNull(endpoint);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        configuration.set("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
        configuration.set("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.OSS");
        configuration.set("fs.oss.accessKeyId", accessKey);
        configuration.set("fs.oss.accessKeySecret", secretKey);
        configuration.set("fs.oss.endpoint", endpoint);
    }

    @Override
    public boolean validate() {
        return !this.accessKey.isEmpty() && !this.secretKey.isEmpty() && !this.endpoint.isEmpty();
    }

    // reuse aws client logic of BE
    @Override
    public void toThrift(Map<String, String> properties) {
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, accessKey);
        properties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, secretKey);
        properties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, endpoint);
    }

    @Override
    public String toCredString() {
        return "AliyunCloudCredential{" +
                "accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", endpoint='" + endpoint + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        // TODO: Support oss credential
        return null;
    }
}
