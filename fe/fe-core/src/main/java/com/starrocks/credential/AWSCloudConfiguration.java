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

package com.starrocks.credential;

import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudProperty;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;

import java.util.LinkedList;
import java.util.List;
public class AWSCloudConfiguration implements CloudConfiguration {

    private AWSCloudCredential awsCloudCredential;

    private boolean enablePathStyleAccess = false;

    private boolean enableSSL = true;

    public AWSCloudConfiguration(AWSCloudCredential awsCloudCredential) {
        this.awsCloudCredential = awsCloudCredential;
    }

    public void setEnablePathStyleAccess(boolean enablePathStyleAccess) {
        this.enablePathStyleAccess = enablePathStyleAccess;
    }

    public void setEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public AWSCloudCredential getAWSCloudCredential() {
        return this.awsCloudCredential;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        configuration.set("fs.s3a.path.style.access", String.valueOf(enablePathStyleAccess));
        configuration.set("fs.s3a.connection.ssl.enabled", String.valueOf(enableSSL));
        awsCloudCredential.applyToConfiguration(configuration);
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        tCloudConfiguration.setCloud_type(TCloudType.AWS);

        List<TCloudProperty> properties = new LinkedList<>();
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS,
                String.valueOf(enablePathStyleAccess)));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_ENABLE_SSL, String.valueOf(enableSSL)));
        awsCloudCredential.toThrift(properties);
        tCloudConfiguration.setCloud_properties(properties);
    }

    @Override
    public String toString() {
        return "AWSCloudConfiguration{" +
                "awsCloudCredential=" + awsCloudCredential +
                ", enablePathStyleAccess=" + enablePathStyleAccess +
                ", enableSSL=" + enableSSL +
                '}';
    }
}
