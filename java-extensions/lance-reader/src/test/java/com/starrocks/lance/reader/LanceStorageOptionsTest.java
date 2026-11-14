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

package com.starrocks.lance.reader;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceStorageOptionsTest {
    private Map<String, String> params(String type, Map<String, String> cloud) {
        Map<String, String> params = new HashMap<>();
        params.put("lance.cloud_type", type);
        cloud.forEach((key, value) -> params.put(LanceStorageOptions.PREFIX + key, value));
        return params;
    }

    @Test
    public void testS3SessionAndEndpoint() {
        Map<String, String> options = LanceStorageOptions.from("s3://bucket/table.lance", params("AWS", Map.of(
                "aws.s3.access_key", "test-key", "aws.s3.secret_key", "test-secret",
                "aws.s3.session_token", "test-session", "aws.s3.region", "us-east-1",
                "aws.s3.endpoint", "localhost:9000", "aws.s3.enable_ssl", "false",
                "aws.s3.enable_path_style_access", "true", "aws.s3.iam_role_arn", "")));
        assertEquals(Map.of("aws_access_key_id", "test-key", "aws_secret_access_key", "test-secret",
                "aws_session_token", "test-session", "aws_region", "us-east-1",
                "aws_endpoint", "http://localhost:9000", "aws_allow_http", "true",
                "aws_virtual_hosted_style_request", "false"), options);
    }

    @Test
    public void testUnsupportedRoleFailsExplicitly() {
        assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions.from("s3://bucket/table", params("AWS",
                Map.of("aws.s3.iam_role_arn", "test-role"))));
    }

    @Test
    public void testAdlsSasAndBlobSharedKey() {
        String uri = "abfss://data@account.dfs.core.windows.net/table.lance";
        assertEquals(Map.of("azure_storage_account_name", "account", "azure_storage_sas_key", "sig=test"),
                LanceStorageOptions.from(uri, params("AZURE",
                        Map.of("fs.azure.sas.fixed.token.account.dfs.core.windows.net", "?sig=test"))));
        assertEquals(Map.of("azure_storage_account_name", "account", "azure_storage_account_key", "test-key"),
                LanceStorageOptions.from(uri, params("AZURE",
                        Map.of("fs.azure.account.key.account.blob.core.windows.net", "test-key"))));
        assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions.from(uri, params("AZURE",
                Map.of("fs.azure.sas.fixed.token.other.dfs.core.windows.net", "sig=test"))));
    }

    @Test
    public void testAzureServicePrincipal() {
        Map<String, String> options = LanceStorageOptions.from(
                "abfss://data@account.dfs.core.windows.net/table.lance", params("AZURE", Map.of(
                        "fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                        "fs.azure.account.oauth2.client.id", "test-client",
                        "fs.azure.account.oauth2.client.secret", "test-secret",
                        "fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/test-tenant/oauth2/token")));
        assertEquals("test-tenant", options.get("azure_storage_tenant_id"));
        assertEquals("test-client", options.get("azure_storage_client_id"));
        assertEquals("test-secret", options.get("azure_storage_client_secret"));
    }

    @Test
    public void testScannerStringDoesNotExposeSignedUriOrCredentials() {
        Map<String, String> params = params("AWS", Map.of("aws.s3.secret_key", "test-secret"));
        params.put("required_fields", "id");
        params.put("lance_dataset_uri", "s3://bucket/table?sig=test-signature");
        String description = new LanceSplitScanner(10, params).toString();
        assertFalse(description.contains("test-signature"));
        assertFalse(description.contains("test-secret"));
    }
}
