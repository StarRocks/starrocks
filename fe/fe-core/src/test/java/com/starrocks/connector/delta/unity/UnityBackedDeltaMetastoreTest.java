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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnityBackedDeltaMetastoreTest {

    private static UnityCatalogProperties propsWithVendedCredentials() {
        return new UnityCatalogProperties(ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiTEST",
                "unity.catalog.name", "main",
                "unity.catalog.vended-credentials-enabled", "true",
                "unity.catalog.aws.region", "us-east-1"));
    }

    private static UnityCatalogTypes.TableInfo tableInfo() {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.name = "orders";
        info.fullName = "main.sales.orders";
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/prefix/orders";
        info.createdAt = 1_700_000_000_000L;
        return info;
    }

    private static UnityCatalogTypes.TemporaryTableCredentials creds() {
        UnityCatalogTypes.TemporaryTableCredentials c = new UnityCatalogTypes.TemporaryTableCredentials();
        c.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        c.awsTempCredentials.accessKeyId = "AKIA_TEST";
        c.awsTempCredentials.secretAccessKey = "secret";
        c.awsTempCredentials.sessionToken = "session";
        return c;
    }

    private static UnityBackedDeltaMetastore newUnityBacked(UnityMetastore unityMetastore,
                                                            UnityCatalogProperties props) {
        return new UnityBackedDeltaMetastore(
                "delta_unity",
                unityMetastore,
                new Configuration(false),
                new DeltaLakeCatalogProperties(Maps.newHashMap()),
                props);
    }

    @Test
    public void testRefreshTableTriggersFreshCredentialVend(@Mocked UnityCatalogClient client) {
        UnityCatalogTypes.TableInfo info = tableInfo();
        UnityCatalogTypes.TemporaryTableCredentials credentials = creds();

        new Expectations() {
            {
                client.getTable("main.sales.orders");
                result = info;
                times = 2;

                client.getTemporaryTableCredentials("abc-123", "READ");
                result = credentials;
                times = 2;
            }
        };

        UnityMetastore unityMetastore = new UnityMetastore(client, propsWithVendedCredentials());
        UnityBackedDeltaMetastore backed = newUnityBacked(unityMetastore, propsWithVendedCredentials());

        CloudConfiguration first = backed.resolveTableCloudConfiguration("sales", "orders");
        Assertions.assertNotNull(first);
        Assertions.assertEquals(CloudType.AWS, first.getCloudType());
        Assertions.assertInstanceOf(AwsCloudConfiguration.class, first);

        backed.refreshTable("sales", "orders");

        CloudConfiguration second = backed.resolveTableCloudConfiguration("sales", "orders");
        Assertions.assertNotNull(second);
        Assertions.assertEquals(CloudType.AWS, second.getCloudType());

        new Verifications() {
            {
                client.invalidate("main.sales.orders");
                times = 1;
            }
        };
    }

    @Test
    public void testRefreshTableNoOpWhenVendedCredentialsDisabled(@Mocked UnityCatalogClient client) {
        UnityCatalogProperties propsNoVend = new UnityCatalogProperties(ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiTEST",
                "unity.catalog.name", "main",
                "unity.catalog.vended-credentials-enabled", "false"));

        UnityMetastore unityMetastore = new UnityMetastore(client, propsNoVend);
        UnityBackedDeltaMetastore backed = new UnityBackedDeltaMetastore(
                "delta_unity",
                unityMetastore,
                new Configuration(false),
                new DeltaLakeCatalogProperties(Maps.newHashMap()),
                propsNoVend);

        backed.refreshTable("sales", "orders");

        new Verifications() {
            {
                client.invalidate("main.sales.orders");
                times = 1;
            }
        };
    }
}
