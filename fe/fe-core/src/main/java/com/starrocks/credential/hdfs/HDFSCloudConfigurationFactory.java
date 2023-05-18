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

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.HDFS_AUTHENTICATION;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_CONTENT;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_PASSWORD;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_USER_NAME;

public class HDFSCloudConfigurationFactory extends CloudConfigurationFactory {
    private final Map<String, String> properties;

    public HDFSCloudConfigurationFactory(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    protected CloudConfiguration buildForStorage() {
        Preconditions.checkNotNull(properties);
        Map<String, String> haConfigurations = new HashMap<>(properties);
        haConfigurations.remove(HDFS_AUTHENTICATION);
        haConfigurations.remove(HDFS_USER_NAME);
        haConfigurations.remove(HDFS_PASSWORD);
        haConfigurations.remove(HDFS_KERBEROS_PRINCIPAL);
        haConfigurations.remove(HDFS_KERBEROS_KEYTAB);
        haConfigurations.remove(HDFS_KERBEROS_KEYTAB_CONTENT);
        HDFSCloudCredential hdfsCloudCredential = new HDFSCloudCredential(
                properties.getOrDefault(HDFS_AUTHENTICATION, ""),
                properties.getOrDefault(HDFS_USER_NAME, ""),
                properties.getOrDefault(HDFS_PASSWORD, ""),
                properties.getOrDefault(HDFS_KERBEROS_PRINCIPAL, ""),
                properties.getOrDefault(HDFS_KERBEROS_KEYTAB, ""),
                properties.getOrDefault(HDFS_KERBEROS_KEYTAB_CONTENT, ""),
                haConfigurations
        );
        if (!hdfsCloudCredential.validate()) {
            return null;
        }
        return new HDFSCloudConfiguration(hdfsCloudCredential);
    }
}
