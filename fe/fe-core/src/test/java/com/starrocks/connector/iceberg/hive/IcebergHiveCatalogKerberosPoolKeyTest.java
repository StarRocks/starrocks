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

package com.starrocks.connector.iceberg.hive;

import com.starrocks.connector.hadoop.HadoopExt;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergHiveCatalogKerberosPoolKeyTest {

    @Test
    public void kerberosPrincipalAddsPoolCacheKey() {
        Configuration conf = new Configuration(false);
        conf.set(HadoopExt.CATALOG_KERBEROS_PRINCIPAL, "user_a@EXAMPLE.COM");
        Map<String, String> props = new HashMap<>();
        IcebergHiveCatalog.applyKerberosClientPoolKey(conf, props);
        Assertions.assertEquals("conf:" + HadoopExt.CATALOG_KERBEROS_PRINCIPAL,
                props.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS));
    }

    @Test
    public void noPrincipalAddsNothing() {
        Map<String, String> props = new HashMap<>();
        IcebergHiveCatalog.applyKerberosClientPoolKey(new Configuration(false), props);
        Assertions.assertFalse(props.containsKey(CatalogProperties.CLIENT_POOL_CACHE_KEYS));
    }

    @Test
    public void userPoolCacheKeysKept() {
        Configuration conf = new Configuration(false);
        conf.set(HadoopExt.CATALOG_KERBEROS_PRINCIPAL, "user_a@EXAMPLE.COM");
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.CLIENT_POOL_CACHE_KEYS, "user_name");
        IcebergHiveCatalog.applyKerberosClientPoolKey(conf, props);
        Assertions.assertEquals("user_name", props.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS));
    }
}
