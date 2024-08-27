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

package com.starrocks.credential.aws;

import com.staros.proto.FileStoreInfo;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.util.Map;

public class AWSCloudConfiguration extends CloudConfiguration {

    private static final int DEFAULT_NUM_OF_PARTITIONED_PREFIX = 256;

    private final AWSCloudCredential awsCloudCredential;

    private boolean enablePathStyleAccess = false;

    private boolean enableSSL = true;

    private boolean enablePartitionedPrefix = false;

    private int numOfPartitionedPrefix = 0;

    public AWSCloudConfiguration(AWSCloudCredential awsCloudCredential) {
        this.awsCloudCredential = awsCloudCredential;
    }

    public void setEnablePathStyleAccess(boolean enablePathStyleAccess) {
        this.enablePathStyleAccess = enablePathStyleAccess;
    }

    public boolean getEnablePathStyleAccess() {
        return this.enablePathStyleAccess;
    }

    public void setEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public boolean getEnableSSL() {
        return this.enableSSL;
    }

    public AWSCloudCredential getAWSCloudCredential() {
        return this.awsCloudCredential;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        super.applyToConfiguration(configuration);
        final String S3AFileSystem = S3AFileSystem.class.getName();
        configuration.set("fs.s3.impl", S3AFileSystem);
        configuration.set("fs.s3a.impl", S3AFileSystem);
        configuration.set("fs.s3n.impl", S3AFileSystem);
        // Below storage using s3 compatible storage api
        configuration.set("fs.oss.impl", S3AFileSystem);
        configuration.set("fs.ks3.impl", S3AFileSystem);
        configuration.set("fs.obs.impl", S3AFileSystem);
        configuration.set("fs.tos.impl", S3AFileSystem);
        configuration.set("fs.cosn.impl", S3AFileSystem);

        // By default, S3AFileSystem will need 4 minutes to timeout when endpoint is unreachable,
        // after change, it will need 30 seconds.
        // Default value is 7.
        configuration.set(Constants.RETRY_LIMIT, "3");
        // Default value is 20
        configuration.set(Constants.MAX_ERROR_RETRIES, "5");

        configuration.set(Constants.PATH_STYLE_ACCESS, String.valueOf(enablePathStyleAccess));
        configuration.set(Constants.SECURE_CONNECTIONS, String.valueOf(enableSSL));
        awsCloudCredential.applyToConfiguration(configuration);
    }

    @Override
    public void loadCommonFields(Map<String, String> properties) {
        super.loadCommonFields(properties);
        enablePartitionedPrefix = Boolean.parseBoolean(
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, "false"));
        String value = properties.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX);
        if (enablePartitionedPrefix) {
            this.numOfPartitionedPrefix = DEFAULT_NUM_OF_PARTITIONED_PREFIX;
            if (value != null) {
                try {
                    int val = Integer.parseInt(value);
                    if (val < 0) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Invalid integer value '%s' for property: '%s', must be a positive integer.",
                                        value,
                                        CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
                    } else if (val > 0) {
                        this.numOfPartitionedPrefix = val;
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid integer value '%s' for property: '%s'", value,
                                    CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
                }
            }
        }
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        super.toThrift(tCloudConfiguration);
        tCloudConfiguration.setCloud_type(TCloudType.AWS);
        Map<String, String> properties = tCloudConfiguration.getCloud_properties();
        properties.put(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS,
                String.valueOf(enablePathStyleAccess));
        properties.put(CloudConfigurationConstants.AWS_S3_ENABLE_SSL, String.valueOf(enableSSL));
        awsCloudCredential.toThrift(properties);
    }

    @Override
    public CloudType getCloudType() {
        return CloudType.AWS;
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder builder = awsCloudCredential.toFileStoreInfo().toBuilder();
        // update the FileStoreInfo with enablePartitionedPrefix and numOfPartitionedPrefix
        builder.getS3FsInfoBuilder()
                .setPartitionedPrefixEnabled(enablePartitionedPrefix)
                .setNumPartitionedPrefix(numOfPartitionedPrefix);
        return builder.build();
    }

    @Override
    public String toConfString() {
        // TODO: add enable_partitioned_prefix, num_partitioned_prefix output
        return "AWSCloudConfiguration{" + getCommonFieldsString() +
                ", cred=" + awsCloudCredential.toCredString() +
                ", enablePathStyleAccess=" + enablePathStyleAccess +
                ", enableSSL=" + enableSSL +
                '}';
    }
}
