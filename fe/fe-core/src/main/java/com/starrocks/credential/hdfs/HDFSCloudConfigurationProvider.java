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
import com.starrocks.credential.CloudConfigurationProvider;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.HDFS_AUTHENTICATION;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_CONTENT;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_DATA;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_FILE;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL2;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_PASSWORD;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_PASSWORD2;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_USER_NAME;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_USER_NAME2;

public class HDFSCloudConfigurationProvider implements CloudConfigurationProvider {

    private static String getOrDefault(Map<String, String> prop, String... args) {
        for (String k : args) {
            String v = prop.get(k);
            if (v != null) {
                return v;
            }
        }
        return "";
    }

    @Override
    public CloudConfiguration build(Map<String, String> properties) {
        Preconditions.checkNotNull(properties);
        Map<String, String> prop = new HashMap<>(properties);

        String[] keys = {
                HDFS_AUTHENTICATION, HDFS_USER_NAME, HDFS_USER_NAME2, HDFS_PASSWORD, HDFS_PASSWORD2,
                HDFS_KERBEROS_PRINCIPAL, HDFS_KERBEROS_PRINCIPAL2, HDFS_KERBEROS_KEYTAB, HDFS_KERBEROS_KEYTAB_FILE,
                HDFS_KERBEROS_KEYTAB_CONTENT, HDFS_KERBEROS_KEYTAB_DATA
        };
        for (String k : keys) {
            prop.remove(k);
        }

        HDFSCloudCredential hdfsCloudCredential = new HDFSCloudCredential(
                getOrDefault(properties, HDFS_AUTHENTICATION),
                getOrDefault(properties, HDFS_USER_NAME2, HDFS_USER_NAME),
                getOrDefault(properties, HDFS_PASSWORD2, HDFS_PASSWORD2),
                getOrDefault(properties, HDFS_KERBEROS_PRINCIPAL2, HDFS_KERBEROS_PRINCIPAL),
                getOrDefault(properties, HDFS_KERBEROS_KEYTAB_FILE, HDFS_KERBEROS_KEYTAB),
                getOrDefault(properties, HDFS_KERBEROS_KEYTAB_DATA, HDFS_KERBEROS_KEYTAB_CONTENT),
                prop
        );
        if (!hdfsCloudCredential.validate()) {
            return null;
        }
        return new HDFSCloudConfiguration(hdfsCloudCredential);
    }
}
