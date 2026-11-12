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

package com.starrocks.connector.hive;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.hadoop.HadoopExt;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HiveMetaClientKerberosKeysTest {

    private static Map<String, String> propsWithRawInternalKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", "thrift://hms.example.com:9083");
        props.put(HadoopExt.CATALOG_KERBEROS_PRINCIPAL, "user_b@EXAMPLE.COM");
        props.put(HadoopExt.CATALOG_KERBEROS_KEYTAB, "/path/to/user_b.keytab");
        return props;
    }

    @Test
    public void rawInternalKeysDoNotReachHiveConf() {
        HiveMetaClient client = HiveMetaClient.createHiveMetaClient("c", new HdfsEnvironment(),
                propsWithRawInternalKeys());
        Assertions.assertNull(client.getHiveConf().get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertNull(client.getHiveConf().get(HadoopExt.CATALOG_KERBEROS_KEYTAB));
    }

    @Test
    public void credentialKeysFromEnvironmentAreKept() {
        Configuration envConf = new Configuration(false);
        envConf.set(HadoopExt.CATALOG_KERBEROS_PRINCIPAL, "user_a@EXAMPLE.COM");
        envConf.set(HadoopExt.CATALOG_KERBEROS_KEYTAB, "/path/to/user_a.keytab");
        HdfsEnvironment env = new HdfsEnvironment() {
            @Override
            public Configuration getConfiguration() {
                return envConf;
            }
        };
        HiveMetaClient client = HiveMetaClient.createHiveMetaClient("c", env, propsWithRawInternalKeys());
        Assertions.assertEquals("user_a@EXAMPLE.COM", client.getHiveConf().get(HadoopExt.CATALOG_KERBEROS_PRINCIPAL));
        Assertions.assertEquals("/path/to/user_a.keytab", client.getHiveConf().get(HadoopExt.CATALOG_KERBEROS_KEYTAB));
    }
}
