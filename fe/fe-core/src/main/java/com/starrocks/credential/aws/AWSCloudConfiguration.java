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
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudType;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class AWSCloudConfiguration implements CloudConfiguration {

    private final AWSCloudCredential awsCloudCredential;

    private boolean enablePathStyleAccess = false;

    private boolean enableSSL = true;

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
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        // Below storage using s3 compatible storage api
        configuration.set("fs.oss.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.ks3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.obs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.tos.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.cosn.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // By default, S3AFileSystem will need 4 minutes to timeout when endpoint is unreachable,
        // after change, it will need 30 seconds.
        // Default value is 7.
        configuration.set("fs.s3a.retry.limit", "3");
        // Default value is 20
        configuration.set("fs.s3a.attempts.maximum", "5");

        configuration.set("fs.s3a.path.style.access", String.valueOf(enablePathStyleAccess));
        configuration.set("fs.s3a.connection.ssl.enabled", String.valueOf(enableSSL));
        awsCloudCredential.applyToConfiguration(configuration);
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        tCloudConfiguration.setCloud_type(TCloudType.AWS);

        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS,
                String.valueOf(enablePathStyleAccess));
        properties.put(CloudConfigurationConstants.AWS_S3_ENABLE_SSL, String.valueOf(enableSSL));
        awsCloudCredential.toThrift(properties);
        tCloudConfiguration.setCloud_properties_v2(properties);
    }

    @Override
    public CloudType getCloudType() {
        return CloudType.AWS;
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        return awsCloudCredential.toFileStoreInfo();
    }

    @Override
    public String getCredentialString() {
        return "AWSCloudConfiguration{" +
                "awsCloudCredential=" + awsCloudCredential +
                ", enablePathStyleAccess=" + enablePathStyleAccess +
                ", enableSSL=" + enableSSL +
                '}';
    }
}
