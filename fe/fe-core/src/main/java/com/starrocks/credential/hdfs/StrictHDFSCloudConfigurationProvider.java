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

import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB;
import static com.starrocks.credential.CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB_CONTENT;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_AUTHENTICATION;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_DEPRECATED;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL_DEPRECATED;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_PASSWORD;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_PASSWORD_DEPRECATED;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_USERNAME;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_USERNAME_DEPRECATED;

public class StrictHDFSCloudConfigurationProvider extends HDFSCloudConfigurationProvider {
    @Override
    public CloudConfiguration build(Map<String, String> properties) {
        Map<String, String> prop = preprocessProperties(properties);

        HDFSCloudCredential hdfsCloudCredential = new StrictHDFSCloudCredential(
                getOrDefault(properties, HDFS_AUTHENTICATION),
                getOrDefault(properties, HDFS_USERNAME, HDFS_USERNAME_DEPRECATED),
                getOrDefault(properties, HDFS_PASSWORD, HDFS_PASSWORD_DEPRECATED),
                getOrDefault(properties, HDFS_KERBEROS_PRINCIPAL, HDFS_KERBEROS_PRINCIPAL_DEPRECATED),
                getOrDefault(properties, HADOOP_KERBEROS_KEYTAB, HDFS_KERBEROS_KEYTAB_DEPRECATED),
                getOrDefault(properties, HADOOP_KERBEROS_KEYTAB_CONTENT, HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED),
                prop
        );
        if (!hdfsCloudCredential.validate()) {
            return null;
        }

        return new HDFSCloudConfiguration(hdfsCloudCredential);
    }
}
