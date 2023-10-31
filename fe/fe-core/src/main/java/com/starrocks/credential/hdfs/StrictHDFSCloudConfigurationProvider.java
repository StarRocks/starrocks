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

package com.starrocks.credential.hdfs;

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationConstants;

import java.util.Map;

public class StrictHDFSCloudConfigurationProvider extends HDFSCloudConfigurationProvider {
    @Override
    public CloudConfiguration build(Map<String, String> properties) {
        Map<String, String> prop = preprocessProperties(properties);

        HDFSCloudCredential hdfsCloudCredential = new StrictHDFSCloudCredential(
                getOrDefault(properties, CloudConfigurationConstants.HDFS_AUTHENTICATION),
                getOrDefault(properties, CloudConfigurationConstants.HDFS_USERNAME,
                        CloudConfigurationConstants.HDFS_USERNAME_DEPRECATED),
                getOrDefault(properties, CloudConfigurationConstants.HDFS_PASSWORD,
                        CloudConfigurationConstants.HDFS_PASSWORD_DEPRECATED),
                getOrDefault(properties, CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL,
                        CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL_DEPRECATED),
                getOrDefault(properties, CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB,
                        CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_DEPRECATED),
                getOrDefault(properties, CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB_CONTENT,
                        CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED),
                prop
        );
        if (!hdfsCloudCredential.validate()) {
            return null;
        }

        return new HDFSCloudConfiguration(hdfsCloudCredential);
    }
}
