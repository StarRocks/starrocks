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
import com.starrocks.thrift.TCloudConfiguration;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** FE metadata equivalent of the native Lance reader's catalog credential mapping. */
final class LanceStorageOptions {
    private LanceStorageOptions() {
    }

    static Map<String, String> from(String warehouse, CloudConfiguration configuration) {
        TCloudConfiguration thrift = new TCloudConfiguration();
        configuration.toThrift(thrift);
        Map<String, String> cloud = new HashMap<>();
        thrift.getCloud_properties().forEach((key, value) -> {
            // Match the BE FFI boundary, which omits empty cloud properties.
            if (value != null && !value.isEmpty()) {
                cloud.put(key, value);
            }
        });
        Map<String, String> options = new HashMap<>();
        switch (thrift.getCloud_type()) {
            case DEFAULT:
                break;
            case AWS:
                aws(cloud, options);
                break;
            case AZURE:
                azure(warehouse, cloud, options);
                break;
            default:
                throw new StarRocksConnectorException("Unsupported Lance catalog cloud configuration");
        }
        return options;
    }

    private static void copy(Map<String, String> source, String key, Map<String, String> target, String option) {
        if (source.containsKey(key)) {
            target.put(option, source.get(key));
        }
    }

    private static boolean flag(Map<String, String> cloud, String key) {
        return Boolean.parseBoolean(cloud.get(key));
    }

    private static void aws(Map<String, String> cloud, Map<String, String> options) {
        if (List.of("aws.s3.iam_role_arn", "aws.s3.external_id", "aws.s3.sts.endpoint", "aws.s3.sts.region")
                .stream().anyMatch(cloud::containsKey)
                || flag(cloud, "aws.s3.use_instance_profile") || flag(cloud, "aws.s3.use_web_identity_token_file")) {
            throw new StarRocksConnectorException("Use the native default credential chain for Lance identity credentials; "
                    + "catalog-configured AWS role assumption is unsupported");
        }
        if (!flag(cloud, "aws.s3.use_aws_sdk_default_behavior")) {
            if (cloud.containsKey("aws.s3.access_key") != cloud.containsKey("aws.s3.secret_key")) {
                throw new StarRocksConnectorException("Lance S3 access key and secret key must be supplied together");
            }
            copy(cloud, "aws.s3.access_key", options, "aws_access_key_id");
            copy(cloud, "aws.s3.secret_key", options, "aws_secret_access_key");
            copy(cloud, "aws.s3.session_token", options, "aws_session_token");
        }
        copy(cloud, "aws.s3.region", options, "aws_region");
        if (cloud.containsKey("aws.s3.endpoint")) {
            String endpoint = cloud.get("aws.s3.endpoint");
            if (!endpoint.contains("://")) {
                endpoint = ("false".equalsIgnoreCase(cloud.get("aws.s3.enable_ssl")) ? "http://" : "https://") + endpoint;
            }
            options.put("aws_endpoint", endpoint);
            if (endpoint.startsWith("http://")) {
                options.put("aws_allow_http", "true");
            }
        }
        if (cloud.containsKey("aws.s3.enable_path_style_access")) {
            options.put("aws_virtual_hosted_style_request", String.valueOf(!flag(cloud, "aws.s3.enable_path_style_access")));
        }
    }

    private static String scoped(Map<String, String> cloud, String key, String host) {
        return cloud.getOrDefault(key + "." + host, cloud.get(key));
    }

    private static void azure(String warehouse, Map<String, String> cloud, Map<String, String> options) {
        URI uri = URI.create(warehouse);
        String host = uri.getHost();
        if (!List.of("abfs", "abfss", "wasb", "wasbs").contains(uri.getScheme()) || uri.getUserInfo() == null
                || host == null || !(host.endsWith(".dfs.core.windows.net") || host.endsWith(".blob.core.windows.net"))) {
            throw new StarRocksConnectorException("Lance Azure credentials require an account-qualified abfss or wasbs URI");
        }
        String account = host.substring(0, host.indexOf('.'));
        String blob = account + ".blob.core.windows.net";
        options.put("azure_storage_account_name", account);
        String key = scoped(cloud, "fs.azure.account.key", host);
        if (key == null) {
            key = scoped(cloud, "fs.azure.account.key", blob);
        }
        String sas = scoped(cloud, "fs.azure.sas.fixed.token", host);
        if (sas == null) {
            sas = cloud.get("fs.azure.sas." + uri.getUserInfo() + "." + blob);
        }
        if (key != null) {
            options.put("azure_storage_account_key", key);
        } else if (sas != null) {
            options.put("azure_storage_sas_key", sas.replaceFirst("^\\?+", ""));
        } else {
            String provider = scoped(cloud, "fs.azure.account.oauth.provider.type", host);
            if (provider == null) {
                throw new StarRocksConnectorException("Unsupported or mismatched Lance Azure credentials");
            }
            String clientId = scoped(cloud, "fs.azure.account.oauth2.client.id", host);
            if (clientId != null) {
                options.put("azure_storage_client_id", clientId);
            }
            if (provider.endsWith(".MsiTokenProvider")) {
                return;
            }
            String endpoint = scoped(cloud, "fs.azure.account.oauth2.client.endpoint", host);
            String secret = scoped(cloud, "fs.azure.account.oauth2.client.secret", host);
            if (!provider.endsWith(".ClientCredsTokenProvider") || clientId == null || endpoint == null || secret == null) {
                throw new StarRocksConnectorException("Unsupported or incomplete Lance Azure OAuth credentials");
            }
            URI authority = URI.create(endpoint);
            String[] path = authority.getPath().split("/");
            if (!"https".equals(authority.getScheme()) || !"login.microsoftonline.com".equals(authority.getHost())
                    || path.length < 2 || path[1].isEmpty()) {
                throw new StarRocksConnectorException("Unsupported Lance Azure OAuth authority");
            }
            options.put("azure_storage_tenant_id", path[1]);
            options.put("azure_storage_client_secret", secret);
        }
    }
}
