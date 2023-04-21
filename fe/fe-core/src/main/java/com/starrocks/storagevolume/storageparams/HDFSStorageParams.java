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

package com.starrocks.storagevolume.storageparams;

import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.storagevolume.credential.hdfs.HDFSCredential;
import com.starrocks.storagevolume.credential.hdfs.HDFSKerberosCredential;
import com.starrocks.storagevolume.credential.hdfs.HDFSSimpleCredential;

import java.util.Map;

public class HDFSStorageParams implements StorageParams {
    public static final String HDFS_AUTHENTICATION = "hadoop.security.authentication";
    public static final String HDFS_USER_NAME = "username";
    public static final String HDFS_PASSWORD = "password";
    public static final String HDFS_KERBEROS_PRINCIPAL = "kerberos_principal";
    public static final String HDFS_KERBEROS_KEYTAB = "kerberos_keytab";
    public static final String HDFS_KERBEROS_KEYTAB_CONTENT = "kerberos_keytab_content";

    private HDFSCredential credential;

    @Override
    public StorageVolume.StorageVolumeType type() {
        return StorageVolume.StorageVolumeType.HDFS;
    }

    public HDFSStorageParams(Map<String, String> params) {
        credential = buildCredential(params);
    }

    private HDFSCredential buildCredential(Map<String, String> params) {
        String authentication = params.get(HDFS_AUTHENTICATION);
        switch (authentication) {
            case "simple":
                String userName = params.getOrDefault(HDFS_USER_NAME, "");
                String password = params.getOrDefault(HDFS_PASSWORD, "");
                params.remove(HDFS_AUTHENTICATION);
                params.remove(HDFS_USER_NAME);
                params.remove(HDFS_PASSWORD);
                return new HDFSSimpleCredential(userName, password, params);
            case "kerberos":
                String principal = params.getOrDefault(HDFS_KERBEROS_PRINCIPAL, "");
                String keyTab = params.getOrDefault(HDFS_KERBEROS_KEYTAB, "");
                String keyTabContent = params.getOrDefault(HDFS_KERBEROS_KEYTAB_CONTENT, "");
                params.remove(HDFS_AUTHENTICATION);
                params.remove(HDFS_KERBEROS_PRINCIPAL);
                params.remove(HDFS_KERBEROS_KEYTAB);
                params.remove(HDFS_KERBEROS_KEYTAB_CONTENT);
                return new HDFSKerberosCredential(principal, keyTab, keyTabContent, params);
            default:
                return null;
        }
    }

    public HDFSCredential getCredential() {
        return credential;
    }
}
