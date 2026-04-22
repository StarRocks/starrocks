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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Minimal POJOs for the subset of the Databricks Unity Catalog REST API that this connector uses.
 * Only fields we actually read are modeled; unknown fields are ignored so the models stay
 * forward-compatible with API additions.
 */
public final class UnityCatalogTypes {

    private UnityCatalogTypes() {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Schema {
        @JsonProperty("name")
        public String name;
        @JsonProperty("catalog_name")
        public String catalogName;
        @JsonProperty("full_name")
        public String fullName;
        @JsonProperty("storage_location")
        public String storageLocation;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ListSchemasResponse {
        @JsonProperty("schemas")
        public List<Schema> schemas;
        @JsonProperty("next_page_token")
        public String nextPageToken;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TableSummary {
        @JsonProperty("name")
        public String name;
        @JsonProperty("catalog_name")
        public String catalogName;
        @JsonProperty("schema_name")
        public String schemaName;
        @JsonProperty("full_name")
        public String fullName;
        @JsonProperty("table_type")
        public String tableType;
        @JsonProperty("data_source_format")
        public String dataSourceFormat;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ListTablesResponse {
        @JsonProperty("tables")
        public List<TableSummary> tables;
        @JsonProperty("next_page_token")
        public String nextPageToken;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TableInfo {
        @JsonProperty("name")
        public String name;
        @JsonProperty("catalog_name")
        public String catalogName;
        @JsonProperty("schema_name")
        public String schemaName;
        @JsonProperty("full_name")
        public String fullName;
        @JsonProperty("table_id")
        public String tableId;
        @JsonProperty("table_type")
        public String tableType;
        @JsonProperty("data_source_format")
        public String dataSourceFormat;
        @JsonProperty("storage_location")
        public String storageLocation;
        @JsonProperty("created_at")
        public Long createdAt;
        @JsonProperty("updated_at")
        public Long updatedAt;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TemporaryTableCredentialsRequest {
        @JsonProperty("table_id")
        public String tableId;
        @JsonProperty("operation")
        public String operation;

        public TemporaryTableCredentialsRequest() {
        }

        public TemporaryTableCredentialsRequest(String tableId, String operation) {
            this.tableId = tableId;
            this.operation = operation;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AwsTempCredentials {
        @JsonProperty("access_key_id")
        public String accessKeyId;
        @JsonProperty("secret_access_key")
        public String secretAccessKey;
        @JsonProperty("session_token")
        public String sessionToken;
        @JsonProperty("access_point")
        public String accessPoint;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AzureUserDelegationSas {
        @JsonProperty("sas_token")
        public String sasToken;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GcpOauthToken {
        @JsonProperty("oauth_token")
        public String oauthToken;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TemporaryTableCredentials {
        @JsonProperty("aws_temp_credentials")
        public AwsTempCredentials awsTempCredentials;
        @JsonProperty("azure_user_delegation_sas")
        public AzureUserDelegationSas azureUserDelegationSas;
        @JsonProperty("gcp_oauth_token")
        public GcpOauthToken gcpOauthToken;
        @JsonProperty("expiration_time")
        public Long expirationTime;
        @JsonProperty("url")
        public String url;
    }
}
