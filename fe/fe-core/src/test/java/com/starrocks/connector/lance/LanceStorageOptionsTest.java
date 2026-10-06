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

package com.starrocks.connector.lance;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LanceStorageOptionsTest {
    private static Map<String, String> options(String uri, Map<String, String> properties) {
        CloudConfiguration config = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        return LanceStorageOptions.from(uri, config);
    }

    @Test
    void s3SessionCredentialsAndEndpoint() {
        Map<String, String> result = options("s3://bucket/data", Map.of(
                "aws.s3.access_key", "access", "aws.s3.secret_key", "secret", "aws.s3.session_token", "session",
                "aws.s3.region", "us-east-1", "aws.s3.endpoint", "localhost:9000",
                "aws.s3.enable_ssl", "false", "aws.s3.enable_path_style_access", "true"));
        assertEquals("access", result.get("aws_access_key_id"));
        assertEquals("secret", result.get("aws_secret_access_key"));
        assertEquals("session", result.get("aws_session_token"));
        assertEquals("http://localhost:9000", result.get("aws_endpoint"));
        assertEquals("true", result.get("aws_allow_http"));
        assertEquals("false", result.get("aws_virtual_hosted_style_request"));
    }

    @Test
    void s3DefaultChainAndUnsupportedRole() {
        assertFalse(options("s3://bucket/data", Map.of("aws.s3.use_aws_sdk_default_behavior", "true"))
                .containsKey("aws_access_key_id"));
        assertThrows(StarRocksConnectorException.class, () -> options("s3://bucket/data", Map.of(
                "aws.s3.use_aws_sdk_default_behavior", "true", "aws.s3.iam_role_arn", "role")));
    }

    @Test
    void azureSasAndAccountMismatch() {
        Map<String, String> properties = Map.of("azure.adls2.storage_account", "account",
                "azure.adls2.sas_token", "?sig=test-token");
        Map<String, String> result = options("abfss://data@account.dfs.core.windows.net/warehouse", properties);
        assertEquals("account", result.get("azure_storage_account_name"));
        assertEquals("sig=test-token", result.get("azure_storage_sas_key"));
        assertThrows(StarRocksConnectorException.class,
                () -> options("abfss://data@other.dfs.core.windows.net/warehouse", properties));
        assertThrows(StarRocksConnectorException.class, () -> options("az://data/warehouse", properties));
    }

    @Test
    void azureKeyAndServicePrincipal() {
        String uri = "abfss://data@account.dfs.core.windows.net/warehouse";
        assertEquals("key", options(uri, Map.of("azure.adls2.storage_account", "account",
                "azure.adls2.shared_key", "key")).get("azure_storage_account_key"));
        Map<String, String> result = options(uri, Map.of("azure.adls2.storage_account", "account",
                "azure.adls2.oauth2_client_id", "client", "azure.adls2.oauth2_client_secret", "secret",
                "azure.adls2.oauth2_client_endpoint", "https://login.microsoftonline.com/tenant/oauth2/token"));
        assertEquals("client", result.get("azure_storage_client_id"));
        assertEquals("tenant", result.get("azure_storage_tenant_id"));
        assertEquals("secret", result.get("azure_storage_client_secret"));
    }
}
