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

package com.starrocks.storagevolume;

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.hdfs.HDFSCloudConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HDFSStorageParamsTest {
    @Test
    public void testSimpleCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("hadoop.security.authentication", "simple");
        storageParams.put("username", "username");
        storageParams.put("password", "password");
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        HDFSStorageParams sp = new HDFSStorageParams(storageParams);
        CloudConfiguration cloudConfiguration = sp.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals("simple", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assert.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHaConfigurations().size());

        Map<String, String> storageParams1 = new HashMap<>();
        storageParams1.put("hadoop.security.authentication", "simple");
        storageParams1.put("username", "username");
        storageParams1.put("password", "password");
        HDFSStorageParams sp1 = new HDFSStorageParams(storageParams1);
        cloudConfiguration = sp.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals("simple", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assert.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHaConfigurations().size());
    }

    @Test
    public void testKerberosCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("hadoop.security.authentication", "kerberos");
        storageParams.put("kerberos_principal", "nn/abc@ABC.COM");
        storageParams.put("kerberos_keytab", "/keytab/hive.keytab");
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        HDFSStorageParams sp = new HDFSStorageParams(storageParams);
        CloudConfiguration cloudConfiguration = sp.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals("kerberos", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assert.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHaConfigurations().size());
    }
}
