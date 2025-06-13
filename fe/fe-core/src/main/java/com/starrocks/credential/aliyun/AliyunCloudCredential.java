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
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

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

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        configuration.set("fs.oss.impl", S3AFileSystem.class.getName());
        configuration.set(Constants.ACCESS_KEY, accessKey);
        configuration.set(Constants.SECRET_KEY, secretKey);
        configuration.set(Constants.ENDPOINT, endpoint);
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
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY, accessKey);
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY, secretKey);
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT, endpoint);
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
