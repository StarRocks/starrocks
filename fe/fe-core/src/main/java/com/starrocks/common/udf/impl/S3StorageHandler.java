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

package com.starrocks.common.udf.impl;

import com.starrocks.common.udf.StorageHandler;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.storagevolume.StorageVolume;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.net.URI;
import java.util.Map;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;

public class S3StorageHandler implements StorageHandler {

    private static final Logger LOG = LogManager.getLogger(S3StorageHandler.class);

    private S3Client s3Client;

    public S3StorageHandler(StorageVolume sv) {
        Map<String, String> svProperties = sv.getProperties();
        String s3Region = svProperties.get(AWS_S3_REGION);
        AwsCloudConfiguration awsCloudConfiguration = (AwsCloudConfiguration) sv.getCloudConfiguration();
        AwsCredentialsProvider awsCredentialsProvider = awsCloudConfiguration
                .getAwsCloudCredential()
                .generateAWSCredentialsProvider();
        this.s3Client = S3Client.builder()
                .region(Region.of(s3Region))
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    public void getObject(String s3FilePath, String localPath) {
        try {
            URI uri = URI.create(s3FilePath);
            String bucket = uri.getHost();
            String key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
            s3Client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build(), new File(localPath).toPath());
            if (LOG.isDebugEnabled()) {
                LOG.debug("get S3 file success for " + s3FilePath);
            }
        } catch (S3Exception s3Exception) {
            LOG.warn("connect to s3 failed with s3 exception", s3Exception);
            throw s3Exception;
        }  catch (Exception exception) {
            LOG.warn("connect to s3 failed with exception", exception);
            throw exception;
        }
    }
}
