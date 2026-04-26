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

package com.starrocks.connector.delta.unity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class UnityMetastoreTest {

    private static UnityCatalogProperties propsWithVendedCredentials(boolean enabled) {
        return new UnityCatalogProperties(ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiTEST",
                "unity.catalog.name", "main",
                "unity.catalog.vended-credentials-enabled", Boolean.toString(enabled)));
    }

    private static UnityCatalogProperties propsWithAwsRegionOverride(String region) {
        return new UnityCatalogProperties(ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiTEST",
                "unity.catalog.name", "main",
                "unity.catalog.vended-credentials-enabled", "true",
                "unity.catalog.aws.region", region));
    }

    @Test
    public void testGetAllDatabaseNamesFiltersBlank(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.Schema s1 = new UnityCatalogTypes.Schema();
        s1.name = "sales";
        UnityCatalogTypes.Schema s2 = new UnityCatalogTypes.Schema();
        s2.name = "";
        UnityCatalogTypes.Schema s3 = new UnityCatalogTypes.Schema();
        s3.name = "marketing";

        new Expectations() {
            {
                client.listSchemas("main");
                result = ImmutableList.of(s1, s2, s3);
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        List<String> dbs = metastore.getAllDatabaseNames();
        Assertions.assertEquals(ImmutableList.of("sales", "marketing"), dbs);
    }

    @Test
    public void testGetAllTableNamesFiltersNonDelta(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableSummary t1 = new UnityCatalogTypes.TableSummary();
        t1.name = "orders";
        t1.dataSourceFormat = "DELTA";
        UnityCatalogTypes.TableSummary t2 = new UnityCatalogTypes.TableSummary();
        t2.name = "iceberg_tbl";
        t2.dataSourceFormat = "ICEBERG";
        UnityCatalogTypes.TableSummary t3 = new UnityCatalogTypes.TableSummary();
        t3.name = "delta_lower";
        t3.dataSourceFormat = "delta";

        new Expectations() {
            {
                client.listTables("main", "sales");
                result = ImmutableList.of(t1, t2, t3);
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        List<String> tables = metastore.getAllTableNames("sales");
        Assertions.assertEquals(ImmutableList.of("orders", "delta_lower"), tables);
    }

    @Test
    public void testGetMetastoreTableReturnsPlainMetastoreTable(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.name = "orders";
        info.fullName = "main.sales.orders";
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";
        info.createdAt = 1_700_000_000_000L;

        new Expectations() {
            {
                client.getTable("main.sales.orders");
                result = info;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(false));
        MetastoreTable mt = metastore.getMetastoreTable("sales", "orders");
        Assertions.assertEquals("s3://bucket/prefix/orders", mt.getTableLocation());
        Assertions.assertEquals(1_700_000_000_000L, mt.getCreateTime());
    }

    @Test
    public void testResolveCloudConfigurationWithAwsVendedCredentials(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "AKIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        UnityCatalogTypes.MetastoreSummary summary = new UnityCatalogTypes.MetastoreSummary();
        summary.region = "us-east-1";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                client.getMetastoreSummary();
                result = summary;
                times = 1;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        CloudConfiguration cc = metastore.resolveCloudConfiguration(info);
        Assertions.assertNotNull(cc);
        Assertions.assertEquals(CloudType.AWS, cc.getCloudType());
        Assertions.assertInstanceOf(AwsCloudConfiguration.class, cc);
    }

    @Test
    public void testResolveCloudConfigurationUsesMetastoreRegion(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "AKIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        UnityCatalogTypes.MetastoreSummary summary = new UnityCatalogTypes.MetastoreSummary();
        summary.region = "eu-central-1";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                times = 2;
                client.getMetastoreSummary();
                result = summary;
                times = 1;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        // First call resolves and memoizes the region.
        CloudConfiguration cc = metastore.resolveCloudConfiguration(info);
        Assertions.assertNotNull(cc);
        Assertions.assertEquals(CloudType.AWS, cc.getCloudType());
        Assertions.assertInstanceOf(AwsCloudConfiguration.class, cc);
        Assertions.assertEquals("eu-central-1",
                ((AwsCloudConfiguration) cc).getAwsCloudCredential().getRegion());
        // Second call must reuse the memoized region
        CloudConfiguration cc2 = metastore.resolveCloudConfiguration(info);
        Assertions.assertNotNull(cc2);
        Assertions.assertInstanceOf(AwsCloudConfiguration.class, cc2);
        Assertions.assertEquals("eu-central-1",
                ((AwsCloudConfiguration) cc2).getAwsCloudCredential().getRegion());
    }

    @Test
    public void testResolveCloudConfigurationRetriesAfterRegionLookupFailure(
            @Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "AKIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        UnityCatalogTypes.MetastoreSummary summary = new UnityCatalogTypes.MetastoreSummary();
        summary.region = "eu-central-1";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                times = 2;
                client.getMetastoreSummary();
                result = new Object[] {
                        new StarRocksConnectorException("simulated 503 on metastore_summary"),
                        summary
                };
                times = 2;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        CloudConfiguration first = metastore.resolveCloudConfiguration(info);
        Assertions.assertNull(first, "transient region lookup failure must surface as null");
        CloudConfiguration second = metastore.resolveCloudConfiguration(info);
        Assertions.assertInstanceOf(AwsCloudConfiguration.class, second);
        Assertions.assertEquals("eu-central-1",
                ((AwsCloudConfiguration) second).getAwsCloudCredential().getRegion());
    }

    @Test
    public void testResolveCloudConfigurationReturnsNullWhenSummaryFails(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "AKIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                client.getMetastoreSummary();
                result = new StarRocksConnectorException("403 forbidden on metastore_summary");
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        CloudConfiguration cc = metastore.resolveCloudConfiguration(info);
        Assertions.assertNull(cc, "summary failure must surface as null cloud config so the BE " +
                "falls back to the catalog-level config (and the operator sees the warn log)");
    }

    @Test
    public void testResolveCloudConfigurationReturnsNullWhenSummaryHasNoRegion(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "AKIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                client.getMetastoreSummary();
                result = new UnityCatalogTypes.MetastoreSummary();
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        Assertions.assertNull(metastore.resolveCloudConfiguration(info),
                "missing region must fail loudly instead of silently falling back to us-east-1");
    }

    @Test
    public void testResolveCloudConfigurationUsesAwsRegionOverride(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "AKIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                client.getMetastoreSummary();
                times = 0;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithAwsRegionOverride("us-west-2"));
        CloudConfiguration cc = metastore.resolveCloudConfiguration(info);
        Assertions.assertInstanceOf(AwsCloudConfiguration.class, cc);
        Assertions.assertEquals("us-west-2",
                ((AwsCloudConfiguration) cc).getAwsCloudCredential().getRegion());
    }

    @Test
    public void testResolveCloudConfigurationDoesNotQueryRegionForAzure(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-azure";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "abfss://container@account.dfs.core.windows.net/prefix/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.azureUserDelegationSas = new UnityCatalogTypes.AzureUserDelegationSas();
        creds.azureUserDelegationSas.sasToken = "sv=2022-11-02&sig=fakesignature";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-azure", "READ");
                result = creds;
                client.getMetastoreSummary();
                times = 0;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        CloudConfiguration cc = metastore.resolveCloudConfiguration(info);
        Assertions.assertNotNull(cc, "Azure SAS credentials must produce a valid cloud configuration");
        Assertions.assertEquals(CloudType.AZURE, cc.getCloudType());
    }

    @Test
    public void testResolveCloudConfigurationReturnsNullWhenDisabled(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        new Expectations() {
            {
                client.getTemporaryTableCredentials(anyString, anyString);
                times = 0;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(false));
        Assertions.assertNull(metastore.resolveCloudConfiguration(info));
    }

    @Test
    public void testGetMetastoreTableRejectsNonDelta(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.name = "t";
        info.fullName = "main.sales.t";
        info.tableId = "id";
        info.dataSourceFormat = "ICEBERG";
        info.storageLocation = "s3://bucket/t";

        new Expectations() {
            {
                client.getTable("main.sales.t");
                result = info;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(false));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metastore.getMetastoreTable("sales", "t"));
    }

    @Test
    public void testGetMetastoreTableRejectsMissingLocation(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.name = "managed_tbl";
        info.fullName = "main.sales.managed_tbl";
        info.tableId = "id";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = null;

        new Expectations() {
            {
                client.getTable("main.sales.managed_tbl");
                result = info;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(false));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metastore.getMetastoreTable("sales", "managed_tbl"));
    }

    @Test
    public void testResolveCloudConfigurationReturnsNullOnVendingFailure(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";

        new Expectations() {
            {
                client.getTemporaryTableCredentials("abc-123", "READ");
                result = new StarRocksConnectorException("forbidden");
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        Assertions.assertNull(metastore.resolveCloudConfiguration(info));
    }

    @Test
    public void testTableExists(@Mocked UnityCatalogClient client) {
        new Expectations() {
            {
                client.tableExists("main.sales.orders");
                result = true;
                client.tableExists("main.sales.missing");
                result = false;
            }
        };

        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(false));
        Assertions.assertTrue(metastore.tableExists("sales", "orders"));
        Assertions.assertFalse(metastore.tableExists("sales", "missing"));
    }

    @Test
    public void testInvalidateTableForwardsToClient(@Mocked UnityCatalogClient client) {
        UnityMetastore metastore = new UnityMetastore(client, propsWithVendedCredentials(true));
        metastore.invalidateTable("sales", "orders");

        new Verifications() {
            {
                client.invalidate("main.sales.orders");
                times = 1;
            }
        };
    }
}
