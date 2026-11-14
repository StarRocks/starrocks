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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** Converts the catalog's Thrift cloud configuration to Lance object-store options. */
final class LanceStorageOptions {
    static final String PREFIX = "lance.cloud.";

    private LanceStorageOptions() {
    }

    static Map<String, String> from(String datasetUri, Map<String, String> params) {
        Map<String, String> cloud = new HashMap<>();
        params.forEach((key, value) -> {
            if (key.startsWith(PREFIX) && value != null && !value.isEmpty()) {
                cloud.put(key.substring(PREFIX.length()), value);
            }
        });
        Map<String, String> options = new HashMap<>();
        String type = params.getOrDefault("lance.cloud_type", "DEFAULT");
        switch (type) {
            case "DEFAULT":
                break;
            case "AWS":
                aws(cloud, options);
                break;
            case "AZURE":
                azure(URI.create(datasetUri), cloud, options);
                break;
            default:
                throw new IllegalArgumentException("Unsupported Lance cloud configuration type: " + type);
        }
        return options;
    }

    private static void copy(Map<String, String> source, String key, Map<String, String> target, String option) {
        String value = source.get(key);
        if (value != null && !value.isEmpty()) {
            target.put(option, value);
        }
    }

    private static void aws(Map<String, String> cloud, Map<String, String> options) {
        // Do not silently ignore role assumptions and read as the base principal instead.
        if (cloud.containsKey("aws.s3.iam_role_arn") || cloud.containsKey("aws.s3.external_id")
                || cloud.containsKey("aws.s3.sts.endpoint") || cloud.containsKey("aws.s3.sts.region")) {
            throw new IllegalArgumentException("Lance does not support catalog-configured AWS STS role assumption");
        }
        if (Boolean.parseBoolean(cloud.get("aws.s3.use_instance_profile"))
                || Boolean.parseBoolean(cloud.get("aws.s3.use_web_identity_token_file"))) {
            throw new IllegalArgumentException("Use the native default credential chain for Lance BE identity credentials");
        }
        if (!Boolean.parseBoolean(cloud.get("aws.s3.use_aws_sdk_default_behavior"))) {
            copy(cloud, "aws.s3.access_key", options, "aws_access_key_id");
            copy(cloud, "aws.s3.secret_key", options, "aws_secret_access_key");
            copy(cloud, "aws.s3.session_token", options, "aws_session_token");
        }
        copy(cloud, "aws.s3.region", options, "aws_region");
        String endpoint = cloud.get("aws.s3.endpoint");
        if (endpoint != null) {
            if (!endpoint.contains("://")) {
                endpoint = (Boolean.parseBoolean(cloud.getOrDefault("aws.s3.enable_ssl", "true"))
                        ? "https://" : "http://") + endpoint;
            }
            options.put("aws_endpoint", endpoint);
            if (endpoint.startsWith("http://")) {
                options.put("aws_allow_http", "true");
            }
        }
        if (cloud.containsKey("aws.s3.enable_path_style_access")) {
            options.put("aws_virtual_hosted_style_request",
                    String.valueOf(!Boolean.parseBoolean(cloud.get("aws.s3.enable_path_style_access"))));
        }
        // Without explicit keys, Lance resolves the native environment / instance identity chain on the BE.
    }

    private static String azureProperty(Map<String, String> cloud, String key, String host) {
        return cloud.getOrDefault(key + "." + host, cloud.get(key));
    }

    private static void azure(URI uri, Map<String, String> cloud, Map<String, String> options) {
        String host = uri.getHost();
        String container = uri.getUserInfo();
        if (host == null || container == null || !(host.endsWith(".dfs.core.windows.net")
                || host.endsWith(".blob.core.windows.net"))) {
            throw new IllegalArgumentException(
                    "Lance catalog Azure credentials require an abfss or wasbs URI with an account host");
        }
        String account = host.substring(0, host.indexOf('.'));
        String blobHost = account + ".blob.core.windows.net";
        options.put("azure_storage_account_name", account);
        String key = azureProperty(cloud, "fs.azure.account.key", host);
        if (key == null) {
            key = azureProperty(cloud, "fs.azure.account.key", blobHost);
        }
        String sas = azureProperty(cloud, "fs.azure.sas.fixed.token", host);
        if (sas == null) {
            sas = cloud.get("fs.azure.sas." + container + "." + blobHost);
        }
        if (key != null) {
            options.put("azure_storage_account_key", key);
        } else if (sas != null) {
            options.put("azure_storage_sas_key", sas.startsWith("?") ? sas.substring(1) : sas);
        } else {
            String provider = azureProperty(cloud, "fs.azure.account.oauth.provider.type", host);
            String clientId = azureProperty(cloud, "fs.azure.account.oauth2.client.id", host);
            if (clientId != null) {
                options.put("azure_storage_client_id", clientId);
            }
            if (provider != null && provider.endsWith(".MsiTokenProvider")) {
                // Lance obtains managed-identity tokens on the BE.
                return;
            }
            if (provider != null && provider.endsWith(".ClientCredsTokenProvider")) {
                String endpoint = azureProperty(cloud, "fs.azure.account.oauth2.client.endpoint", host);
                URI authority = URI.create(endpoint);
                if (!"https".equals(authority.getScheme()) || !"login.microsoftonline.com".equals(authority.getHost())) {
                    throw new IllegalArgumentException("Unsupported Lance Azure OAuth authority");
                }
                String[] path = authority.getPath().split("/");
                if (path.length < 2 || path[1].isEmpty()) {
                    throw new IllegalArgumentException("Missing Azure OAuth tenant");
                }
                options.put("azure_storage_tenant_id", path[1]);
                options.put("azure_storage_client_secret",
                        azureProperty(cloud, "fs.azure.account.oauth2.client.secret", host));
            } else {
                throw new IllegalArgumentException("Unsupported or mismatched Lance catalog Azure credentials");
            }
        }
    }
}
