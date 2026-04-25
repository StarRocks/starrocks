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

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnityCatalogCredentialTranslatorTest {

    @Test
    public void testAwsSessionCredentialsYieldAwsConfig() {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "ASIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                "s3://bucket/path/table");
        Assertions.assertEquals(CloudType.AWS, cc.getCloudType());
    }

    @Test
    public void testAwsSessionCredentialsPropagateRegionWhenProvided() {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "ASIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                "s3://bucket/path/table", "eu-central-1");
        Assertions.assertEquals(CloudType.AWS, cc.getCloudType());
        Assertions.assertInstanceOf(AwsCloudConfiguration.class, cc);
        Assertions.assertEquals("eu-central-1",
                ((AwsCloudConfiguration) cc).getAwsCloudCredential().getRegion());
    }

    @Test
    public void testAwsSessionCredentialsNullRegionLeavesRegionUnset() {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "ASIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        creds.awsTempCredentials.sessionToken = "session";

        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                "s3://bucket/path/table", null);
        Assertions.assertEquals(CloudType.AWS, cc.getCloudType());
        // No explicit region should have been plumbed through; AwsCloudConfigurationProvider may
        // stamp a default, but that value must not be the caller-supplied "eu-central-1".
        Assertions.assertNotEquals("eu-central-1",
                ((AwsCloudConfiguration) cc).getAwsCloudCredential().getRegion());
    }

    @Test
    public void testAwsMissingSessionTokenFallsBackToDefault() {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.awsTempCredentials = new UnityCatalogTypes.AwsTempCredentials();
        creds.awsTempCredentials.accessKeyId = "ASIA_TEST";
        creds.awsTempCredentials.secretAccessKey = "secret";
        // session_token intentionally missing

        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                "s3://bucket/path/table");
        Assertions.assertEquals(CloudType.DEFAULT, cc.getCloudType());
    }

    @Test
    public void testAzureAdlsGen2SasYieldsAzureConfig() {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.azureUserDelegationSas = new UnityCatalogTypes.AzureUserDelegationSas();
        creds.azureUserDelegationSas.sasToken = "sv=2023-08-03&ss=b&sig=xxx";

        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                "abfss://container@account.dfs.core.windows.net/path/table");
        Assertions.assertEquals(CloudType.AZURE, cc.getCloudType());
    }

    @Test
    public void testAzureSasWithoutLocationFallsBack() {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.azureUserDelegationSas = new UnityCatalogTypes.AzureUserDelegationSas();
        creds.azureUserDelegationSas.sasToken = "sv=2023-08-03&ss=b&sig=xxx";

        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds, null);
        Assertions.assertEquals(CloudType.DEFAULT, cc.getCloudType());
    }

    @Test
    public void testGcpTokenIsIgnoredInV1() {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.gcpOauthToken = new UnityCatalogTypes.GcpOauthToken();
        creds.gcpOauthToken.oauthToken = "ya29.test";

        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(creds,
                "gs://bucket/path/table");
        Assertions.assertEquals(CloudType.DEFAULT, cc.getCloudType(),
                "GCS should not be supported in v1 and should fall back to DEFAULT");
    }

    @Test
    public void testNullCredsReturnsDefault() {
        CloudConfiguration cc = UnityCatalogCredentialTranslator.toCloudConfiguration(null,
                "s3://bucket/path/table");
        Assertions.assertEquals(CloudType.DEFAULT, cc.getCloudType());
    }

    @Test
    public void testExtractAdlsEndpointAbfss() {
        Assertions.assertEquals("account.dfs.core.windows.net",
                UnityCatalogCredentialTranslator.extractAdlsEndpoint(
                        "abfss://container@account.dfs.core.windows.net/path"));
    }

    @Test
    public void testExtractAdlsEndpointAbfs() {
        Assertions.assertEquals("account.dfs.core.windows.net",
                UnityCatalogCredentialTranslator.extractAdlsEndpoint(
                        "abfs://container@account.dfs.core.windows.net/path"));
    }

    @Test
    public void testExtractAdlsEndpointS3ReturnsNull() {
        Assertions.assertNull(UnityCatalogCredentialTranslator.extractAdlsEndpoint("s3://bucket/path"));
    }

    @Test
    public void testExtractAdlsEndpointBlobEndpointReturnsNull() {
        // Only ADLS Gen2 endpoints are supported for SAS in v1.
        Assertions.assertNull(UnityCatalogCredentialTranslator.extractAdlsEndpoint(
                "abfss://container@account.blob.core.windows.net/path"));
    }

    @Test
    public void testExtractAdlsEndpointNullInputReturnsNull() {
        Assertions.assertNull(UnityCatalogCredentialTranslator.extractAdlsEndpoint(null));
        Assertions.assertNull(UnityCatalogCredentialTranslator.extractAdlsEndpoint(""));
    }
}
