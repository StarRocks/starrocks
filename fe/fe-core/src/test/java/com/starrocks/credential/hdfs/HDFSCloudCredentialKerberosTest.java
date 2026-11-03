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

import com.starrocks.common.Config;
import com.starrocks.connector.hadoop.HadoopExt;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HDFSCloudCredentialKerberosTest {

    private static final String PRINCIPAL = "user_a@EXAMPLE.COM";
    private static final String KEYTAB = "/path/to/user_a.keytab";

    @AfterEach
    public void restoreConfig() {
        Config.enable_catalog_kerberos_credentials = true;
    }

    private static Map<String, String> kerberosProps(String principalKey, String keytabKey, String keytab) {
        Map<String, String> props = new HashMap<>();
        props.put(CloudConfigurationConstants.HDFS_AUTHENTICATION, "kerberos");
        props.put(principalKey, PRINCIPAL);
        props.put(keytabKey, keytab);
        return props;
    }

    private static Configuration applied(Map<String, String> props) {
        HDFSCloudConfiguration cc = (HDFSCloudConfiguration) new HDFSCloudConfigurationProvider().build(props);
        Assertions.assertNotNull(cc);
        Configuration conf = new Configuration(false);
        cc.applyToConfiguration(conf);
        return conf;
    }

    private static Map<String, String> thrift(Map<String, String> props) {
        HDFSCloudConfiguration cc = (HDFSCloudConfiguration) new HDFSCloudConfigurationProvider().build(props);
        Assertions.assertNotNull(cc);
        Map<String, String> out = new HashMap<>();
        cc.getHdfsCloudCredential().toThrift(out);
        return out;
    }

    @Test
    public void kerberosWithKeytabAddsInternalKeys() {
        Map<String, String> props = kerberosProps(CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL,
                CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB, KEYTAB);
        Configuration conf = applied(props);
        Assertions.assertEquals(PRINCIPAL, conf.get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertEquals(KEYTAB, conf.get(HadoopExt.CATALOG_KERBEROS_KEYTAB));
        Map<String, String> be = thrift(props);
        Assertions.assertEquals(PRINCIPAL, be.get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertEquals(KEYTAB, be.get(HadoopExt.CATALOG_KERBEROS_KEYTAB));
    }

    @Test
    public void deprecatedKeysAreHonored() {
        Map<String, String> props = kerberosProps(CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL_DEPRECATED,
                CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_DEPRECATED, KEYTAB);
        Assertions.assertEquals(PRINCIPAL, applied(props).get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertEquals(KEYTAB, thrift(props).get(HadoopExt.CATALOG_KERBEROS_KEYTAB));
    }

    @Test
    public void keytabContentOnlyAddsNothing() {
        Map<String, String> props = kerberosProps(CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL,
                CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB_CONTENT, "a2V5dGFi");
        Assertions.assertNull(applied(props).get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertFalse(thrift(props).containsKey(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
    }

    @Test
    public void simpleAuthAddsNothing() {
        Map<String, String> props = kerberosProps(CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL,
                CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB, KEYTAB);
        props.put(CloudConfigurationConstants.HDFS_AUTHENTICATION, "simple");
        Assertions.assertNull(applied(props).get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertFalse(thrift(props).containsKey(HadoopExt.CATALOG_KERBEROS_KEYTAB));
    }

    @Test
    public void configDisabledAddsNothing() {
        Config.enable_catalog_kerberos_credentials = false;
        Map<String, String> props = kerberosProps(CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL,
                CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB, KEYTAB);
        Assertions.assertNull(applied(props).get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertFalse(thrift(props).containsKey(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
    }
}
