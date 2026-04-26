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

package com.starrocks.connector.delta;

import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.delta.unity.CachingUnityCatalogClient;
import com.starrocks.connector.delta.unity.UnityBackedDeltaMetastore;
import com.starrocks.connector.delta.unity.UnityCatalogApi;
import com.starrocks.connector.delta.unity.UnityCatalogClient;
import com.starrocks.connector.delta.unity.UnityMetastore;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

public class DeltaLakeConnectorTest {
    @Test
    public void testCreateDeltaLakeConnector() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.type", "hive", "hive.metastore.uris", "thrift://localhost:9083");
        DeltaLakeConnector connector = new DeltaLakeConnector(new ConnectorContext("delta0", "deltalake",
                properties));
        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof DeltaLakeMetadata);
        DeltaLakeMetadata deltaLakeMetadata = (DeltaLakeMetadata) metadata;
        Assertions.assertEquals("delta0", deltaLakeMetadata.getCatalogName());
        Assertions.assertEquals(deltaLakeMetadata.getMetastoreType(), MetastoreType.HMS);
    }

    @Test
    public void testCreateDeltaLakeConnectorWithException1() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.TYPE", "glue", "aws.glue.access_key", "xxxxx",
                "aws.glue.secret_key", "xxxx",
                "aws.glue.region", "us-west-2");
        try {
            ConnectorFactory.createConnector(new ConnectorContext("delta0", "deltalake", properties), false);
            Assertions.fail("Should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertEquals("Failed to init connector [type: deltalake, name: delta0]. msg: " +
                            "hive.metastore.uris must be set in properties when creating catalog of hive-metastore",
                    e.getMessage());
        }
    }

    @Test
    public void testCreateDeltaLakeConnectorWithException2() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.type", "error_metastore", "aws.glue.access_key", "xxxxx",
                "aws.glue.secret_key", "xxxx",
                "aws.glue.region", "us-west-2");
        try {
            ConnectorFactory.createConnector(new ConnectorContext("delta0", "deltalake", properties), false);
            Assertions.fail("Should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertEquals("Failed to init connector [type: deltalake, name: delta0]. " +
                    "msg: Getting analyzing error. Detail message: hive metastore type [error_metastore] " +
                    "is not supported.", e.getMessage());
        }
    }

    @Test
    public void testDeltaLakeConnectorMemUsage() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.type", "hive", "hive.metastore.uris", "thrift://localhost:9083");
        CatalogConnector catalogConnector = ConnectorFactory.createConnector(
                new ConnectorContext("delta0", "deltalake", properties), false);
        Assertions.assertTrue(catalogConnector.supportMemoryTrack());
        Assertions.assertEquals(840, catalogConnector.estimateSize());
        Assertions.assertEquals(4, catalogConnector.estimateCount().size());
    }

    @Test
    public void testDeltaLakeRemoteFileInfo() {
        FileScanTask fileScanTask = null;
        DeltaRemoteFileInfo deltaRemoteFileInfo = new DeltaRemoteFileInfo(fileScanTask);
        Assertions.assertNull(deltaRemoteFileInfo.getFileScanTask());
    }

    @Test
    public void testUnityCatalogCachingClientWiredByDefault() throws Exception {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "deltalake")
                .put("hive.metastore.type", "unity")
                .put("unity.catalog.host", "https://example.cloud.databricks.com")
                .put("unity.catalog.token", "dapiTEST")
                .put("unity.catalog.name", "main")
                .build();
        UnityCatalogApi client = extractUnityClient(properties);
        Assertions.assertInstanceOf(CachingUnityCatalogClient.class, client,
                "unity.catalog.cache.enabled defaults to true, so the REST client should be wrapped");
    }

    @Test
    public void testUnityCatalogCachingClientBypassedWhenDisabled() throws Exception {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "deltalake")
                .put("hive.metastore.type", "unity")
                .put("unity.catalog.host", "https://example.cloud.databricks.com")
                .put("unity.catalog.token", "dapiTEST")
                .put("unity.catalog.name", "main")
                .put("unity.catalog.cache.enabled", "false")
                .build();
        UnityCatalogApi client = extractUnityClient(properties);
        Assertions.assertInstanceOf(UnityCatalogClient.class, client,
                "when the cache is disabled the raw REST client must be handed to UnityMetastore");
        Assertions.assertFalse(client instanceof CachingUnityCatalogClient);
    }

    @Test
    public void testUnitySnapshotCacheNotBypassedWhenUnityClientCacheEnabled() {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "deltalake")
                .put("hive.metastore.type", "unity")
                .put("unity.catalog.host", "https://example.cloud.databricks.com")
                .put("unity.catalog.token", "dapiTEST")
                .put("unity.catalog.name", "main")
                .build();
        UnityBackedDeltaMetastore unity = extractUnityBackedMetastore(properties);
        Assertions.assertTrue(unity.isVendedCredentialsEnabled(),
                "vended credentials default to true for Unity Catalog");
        Assertions.assertFalse(unity.isSnapshotCacheBypassed(),
                "with the unity client cache active the snapshot cache must be reused");
    }

    @Test
    public void testUnitySnapshotCacheBypassedWhenUnityClientCacheDisabled() {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "deltalake")
                .put("hive.metastore.type", "unity")
                .put("unity.catalog.host", "https://example.cloud.databricks.com")
                .put("unity.catalog.token", "dapiTEST")
                .put("unity.catalog.name", "main")
                .put("unity.catalog.cache.enabled", "false")
                .build();
        UnityBackedDeltaMetastore unity = extractUnityBackedMetastore(properties);
        Assertions.assertTrue(unity.isVendedCredentialsEnabled());
        Assertions.assertTrue(unity.isSnapshotCacheBypassed(),
                "disabling the unity client cache must force snapshot bypass to keep credentials fresh");
    }

    @Test
    public void testUnitySnapshotCacheBypassedWhenUnityClientCacheTtlIsZero() {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "deltalake")
                .put("hive.metastore.type", "unity")
                .put("unity.catalog.host", "https://example.cloud.databricks.com")
                .put("unity.catalog.token", "dapiTEST")
                .put("unity.catalog.name", "main")
                .put("unity.catalog.cache.ttl-sec", "0")
                .build();
        UnityBackedDeltaMetastore unity = extractUnityBackedMetastore(properties);
        Assertions.assertTrue(unity.isSnapshotCacheBypassed(),
                "ttl-sec=0 means no client-side cache lifetime, so the snapshot cache must bypass too");
    }

    @Test
    public void testUnitySnapshotCacheNotBypassedWhenVendedCredentialsDisabled() {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "deltalake")
                .put("hive.metastore.type", "unity")
                .put("unity.catalog.host", "https://example.cloud.databricks.com")
                .put("unity.catalog.token", "dapiTEST")
                .put("unity.catalog.name", "main")
                .put("unity.catalog.vended-credentials-enabled", "false")
                .put("unity.catalog.cache.enabled", "false")
                .build();
        UnityBackedDeltaMetastore unity = extractUnityBackedMetastore(properties);
        Assertions.assertFalse(unity.isVendedCredentialsEnabled());
        Assertions.assertFalse(unity.isSnapshotCacheBypassed());
    }

    private static UnityBackedDeltaMetastore extractUnityBackedMetastore(Map<String, String> properties) {
        DeltaLakeInternalMgr mgr = new DeltaLakeInternalMgr("uc_delta", properties, new HdfsEnvironment());
        IDeltaLakeMetastore metastore = mgr.createUnityBackedDeltaLakeMetastore();
        if (metastore instanceof CachingDeltaLakeMetastore) {
            metastore = ((CachingDeltaLakeMetastore) metastore).delegate;
        }
        Assertions.assertInstanceOf(UnityBackedDeltaMetastore.class, metastore);
        return (UnityBackedDeltaMetastore) metastore;
    }

    private static UnityCatalogApi extractUnityClient(Map<String, String> properties) throws Exception {
        DeltaLakeInternalMgr mgr = new DeltaLakeInternalMgr("uc_delta", properties, new HdfsEnvironment());
        IDeltaLakeMetastore metastore = mgr.createUnityBackedDeltaLakeMetastore();
        if (metastore instanceof CachingDeltaLakeMetastore) {
            metastore = ((CachingDeltaLakeMetastore) metastore).delegate;
        }
        Assertions.assertInstanceOf(UnityBackedDeltaMetastore.class, metastore);
        Field delegateField = DeltaLakeMetastore.class.getDeclaredField("delegate");
        delegateField.setAccessible(true);
        Object imetastore = delegateField.get(metastore);
        Assertions.assertInstanceOf(UnityMetastore.class, imetastore);
        Method accessor = UnityMetastore.class.getDeclaredMethod("getClientForTest");
        accessor.setAccessible(true);
        return (UnityCatalogApi) accessor.invoke(imetastore);
    }
}
