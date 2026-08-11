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

package com.starrocks.connector.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BigQueryConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(BigQueryConnector.class);

    private final String catalogName;
    private final BigQueryProperties properties;
    private final BigQuery bigQuery;
    private final BigQueryReadClient readClient;
    private final GoogleCredentials credentials;

    private ConnectorMetadata metadata;

    public BigQueryConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = new BigQueryProperties(context.getProperties());
        this.credentials = buildCredentials();
        this.bigQuery = buildBigQueryClient();
        this.readClient = buildReadClient();
    }

    private GoogleCredentials buildCredentials() {
        String credJson = properties.get(BigQueryProperties.CREDENTIALS_JSON);
        String credFile = properties.get(BigQueryProperties.CREDENTIALS_FILE);
        String authType = properties.get(BigQueryProperties.AUTH_TYPE);

        try {
            if (credJson != null && !credJson.isEmpty()) {
                return ServiceAccountCredentials.fromStream(
                        new ByteArrayInputStream(credJson.getBytes(StandardCharsets.UTF_8)))
                        .createScoped("https://www.googleapis.com/auth/bigquery",
                                "https://www.googleapis.com/auth/cloud-platform");
            }
            if (credFile != null && !credFile.isEmpty()) {
                return ServiceAccountCredentials.fromStream(new FileInputStream(credFile))
                        .createScoped("https://www.googleapis.com/auth/bigquery",
                                "https://www.googleapis.com/auth/cloud-platform");
            }
            // ADC — explicit or implicit default. Works on GCE/GKE node SA, Workload Identity,
            // gcloud credentials, and GOOGLE_APPLICATION_CREDENTIALS env var.
            if (authType == null || authType.isEmpty()
                    || BigQueryProperties.AUTH_TYPE_APPLICATION_DEFAULT.equals(authType)) {
                return GoogleCredentials.getApplicationDefault()
                        .createScoped("https://www.googleapis.com/auth/bigquery",
                                "https://www.googleapis.com/auth/cloud-platform");
            }
            throw new StarRocksConnectorException(
                    "Unsupported bigquery.auth.type: '" + authType + "'. " +
                            "Valid values: service_account_json, service_account_file, application_default");
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed to load BigQuery credentials: " + e.getMessage(), e);
        }
    }

    private BigQuery buildBigQueryClient() {
        String projectId = properties.get(BigQueryProperties.PROJECT_ID);
        return BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(credentials)
                .build()
                .getService();
    }

    private BigQueryReadClient buildReadClient() {
        try {
            return BigQueryReadClient.create(
                    BigQueryReadSettings.newBuilder()
                            .setCredentialsProvider(() -> credentials)
                            .build());
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed to create BigQuery Storage Read client: " + e.getMessage(), e);
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new BigQueryMetadata(bigQuery, readClient, credentials, catalogName, properties);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to create BigQuery metadata for catalog '{}'", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }

    @Override
    public void shutdown() {
        try {
            if (readClient != null) {
                readClient.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing BigQuery read client for catalog '{}'", catalogName, e);
        }
    }
}
