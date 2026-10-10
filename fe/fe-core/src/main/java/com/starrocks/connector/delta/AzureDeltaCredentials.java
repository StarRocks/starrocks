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

import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.time.Duration;

/** Translate the effective, snapshot-scoped ABFS credentials, without an ambient default credential chain. */
final class AzureDeltaCredentials {
    private static final String PROVIDER_PREFIX = "org.apache.hadoop.fs.azurebfs.oauth2.";

    private AzureDeltaCredentials() {
    }

    static BlobContainerClient createContainer(String tablePath, Configuration conf) {
        URI uri = AzureDeltaFileIO.parseUri(tablePath);
        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .endpoint("https://" + uri.getHost().replace(".dfs.", ".blob."))
                .containerName(uri.getUserInfo())
                .retryOptions(new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 3, Duration.ofSeconds(30),
                        Duration.ofSeconds(1), Duration.ofSeconds(5), null));
        configure(builder, uri, conf);
        return builder.buildClient();
    }

    static void configure(BlobContainerClientBuilder builder, URI uri, Configuration conf) {
        String host = uri.getHost();
        String authType = get(conf, host, "fs.azure.account.auth.type", "SharedKey");
        switch (authType) {
            case "SharedKey":
                rejectCustomProvider(conf, host, "fs.azure.account.keyprovider");
                builder.credential(new StorageSharedKeyCredential(host.substring(0, host.indexOf('.')),
                        required(conf, host, "fs.azure.account.key")));
                break;
            case "SAS":
                String sasProvider = get(conf, host, "fs.azure.sas.token.provider.type", "");
                if (!sasProvider.isEmpty() &&
                        !sasProvider.equals("org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")) {
                    throw new IllegalArgumentException("Native ADLS supports fixed SAS tokens only");
                }
                String token = required(conf, host, "fs.azure.sas.fixed.token");
                builder.sasToken(token.startsWith("?") ? token.substring(1) : token);
                break;
            case "OAuth":
                configureOAuth(builder, uri, conf);
                break;
            default:
                throw new IllegalArgumentException("Unsupported native ADLS authentication type");
        }
    }

    private static void configureOAuth(BlobContainerClientBuilder builder, URI uri, Configuration conf) {
        String host = uri.getHost();
        String provider = required(conf, host, "fs.azure.account.oauth.provider.type");
        String clientId = required(conf, host, "fs.azure.account.oauth2.client.id");
        if (provider.equals(PROVIDER_PREFIX + "ClientCredsTokenProvider")) {
            URI endpoint;
            try {
                endpoint = URI.create(required(conf, host, "fs.azure.account.oauth2.client.endpoint"));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid native ADLS OAuth token endpoint");
            }
            String[] parts = endpoint.getPath() == null ? new String[0] : endpoint.getPath().split("/");
            if (!"https".equals(endpoint.getScheme()) || endpoint.getHost() == null ||
                    endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null ||
                    !((parts.length == 4 && "oauth2".equals(parts[2]) && "token".equals(parts[3])) ||
                    (parts.length == 5 && "oauth2".equals(parts[2]) && "v2.0".equals(parts[3]) &&
                            "token".equals(parts[4]))) || parts[1].isEmpty()) {
                throw new IllegalArgumentException("Native ADLS requires an HTTPS tenant OAuth token endpoint");
            }
            builder.credential(new ClientSecretCredentialBuilder().clientId(clientId).tenantId(parts[1])
                    .clientSecret(required(conf, host, "fs.azure.account.oauth2.client.secret"))
                    .authorityHost("https://" + endpoint.getAuthority()).build());
        } else if (provider.equals(PROVIDER_PREFIX + "MsiTokenProvider")) {
            rejectCustomProvider(conf, host, "fs.azure.account.oauth2.msi.endpoint");
            builder.credential(new ManagedIdentityCredentialBuilder().clientId(clientId).build());
        } else if (provider.equals(PROVIDER_PREFIX + "WorkloadIdentityTokenProvider")) {
            String authority = get(conf, host, "fs.azure.account.oauth2.msi.authority", authorityFor(host));
            builder.credential(new WorkloadIdentityCredentialBuilder().clientId(clientId)
                    .tenantId(required(conf, host, "fs.azure.account.oauth2.msi.tenant"))
                    .tokenFilePath(required(conf, host, "fs.azure.account.oauth2.token.file"))
                    .authorityHost(authority).build());
        } else {
            throw new IllegalArgumentException("Unsupported native ADLS OAuth token provider");
        }
    }

    private static String authorityFor(String host) {
        if (host.endsWith(".chinacloudapi.cn")) {
            return "https://login.chinacloudapi.cn/";
        }
        if (host.endsWith(".usgovcloudapi.net")) {
            return "https://login.microsoftonline.us/";
        }
        return "https://login.microsoftonline.com/";
    }

    private static void rejectCustomProvider(Configuration conf, String host, String key) {
        if (!get(conf, host, key, "").isEmpty()) {
            throw new IllegalArgumentException("Native ADLS does not support " + key);
        }
    }

    private static String required(Configuration conf, String host, String key) {
        String value = get(conf, host, key, "");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Missing native ADLS credential setting: " + key);
        }
        return value;
    }

    private static String get(Configuration conf, String host, String key, String defaultValue) {
        return conf.get(key + "." + host, conf.get(key, defaultValue));
    }
}
