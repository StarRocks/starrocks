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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.BigQueryTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.informationschema.InformationSchemaConnector;
import com.starrocks.connector.metadata.TableMetaConnector;
import com.starrocks.credential.gcp.GCPCloudConfiguration;
import com.starrocks.credential.gcp.GCPCloudCredential;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class BigQueryMockedBase {

    protected static final String CATALOG_NAME = "bq_catalog";
    protected static final String PROJECT_ID   = "test-project";
    protected static final String DATASET_ID   = "test_dataset";
    protected static final String TABLE_NAME   = "test_table";

    // BigQuery SDK mocks
    protected static BigQuery mockBigQuery = Mockito.mock(BigQuery.class);
    protected static BigQueryReadClient mockReadClient = Mockito.mock(BigQueryReadClient.class);
    protected static GoogleCredentials mockCredentials = Mockito.mock(GoogleCredentials.class);

    // Connector layer mocks
    protected static BigQueryConnector mockConnector = Mockito.mock(BigQueryConnector.class);
    protected static BigQueryMetadata mockMetadata = Mockito.mock(BigQueryMetadata.class);

    // StarRocks infrastructure mocks
    protected static MockedStatic<GlobalStateMgr> mockedStatic = Mockito.mockStatic(GlobalStateMgr.class);
    protected static GlobalStateMgr globalStateMgr = Mockito.mock(GlobalStateMgr.class);
    protected static CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
    protected static ConnectorMgr connectorMgr = Mockito.mock(ConnectorMgr.class);
    protected static MetadataMgr metadataMgr = Mockito.mock(MetadataMgr.class);
    protected static VariableMgr variableMgr = new VariableMgr();

    // Shared table and read-session data
    protected static BigQueryTable bigQueryTable;
    protected static ReadSession mockReadSession;

    protected static BigQueryProperties buildProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, PROJECT_ID);
        return new BigQueryProperties(props);
    }

    public static void initMock() {
        // Build a simple 2-column BigQuery table
        Schema schema = Schema.of(
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING)
        );
        List<com.starrocks.catalog.Column> columns = BigQuerySchemaUtils.toStarRocksColumns(schema);
        bigQueryTable = new BigQueryTable(CATALOG_NAME, DATASET_ID, TABLE_NAME, columns, 1000L, false);

        // Build a mock ReadSession with 2 streams
        mockReadSession = ReadSession.newBuilder()
                .setName("projects/test-project/locations/us/sessions/sess_abc")
                .addStreams(ReadStream.newBuilder()
                        .setName("projects/test-project/locations/us/sessions/sess_abc/streams/0").build())
                .addStreams(ReadStream.newBuilder()
                        .setName("projects/test-project/locations/us/sessions/sess_abc/streams/1").build())
                .build();

        // RemoteFileInfo that scan node will receive from MetadataMgr
        Map<String, String> commonParams = new HashMap<>();
        commonParams.put("project_id", PROJECT_ID);
        commonParams.put("dataset_id", DATASET_ID);
        commonParams.put("table_id", TABLE_NAME);
        commonParams.put("required_fields", "id,name");
        commonParams.put("credentials_base64", "");
        commonParams.put("read_session_name", mockReadSession.getName());

        RemoteFileInfo fileInfo = new RemoteFileInfo();
        fileInfo.setFiles(ImmutableList.of(
                BigQueryRemoteFileDesc.createBigQueryRemoteFileDesc(
                        mockReadSession.getName(),
                        mockReadSession.getStreams(0).getName(), 0),
                BigQueryRemoteFileDesc.createBigQueryRemoteFileDesc(
                        mockReadSession.getName(),
                        mockReadSession.getStreams(1).getName(), 1)
        ));
        fileInfo.setAttachment(commonParams);

        // GCP cloud configuration (used by BigQueryScanNode.setupCloudCredential)
        GCPCloudCredential gcpCredential = new GCPCloudCredential(
                "", true, "", "", "", "", "", "");
        GCPCloudConfiguration gcpConfig = new GCPCloudConfiguration(gcpCredential);
        gcpConfig.loadCommonFields(new HashMap<>(0));

        when(mockMetadata.getCloudConfiguration()).thenReturn(gcpConfig);
        when(mockMetadata.getRemoteFiles(any(), any())).thenReturn(ImmutableList.of(fileInfo));
        when(mockConnector.getMetadata()).thenReturn(mockMetadata);

        // Wire up GlobalStateMgr statics
        mockedStatic.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
        when(globalStateMgr.getCatalogMgr()).thenReturn(catalogMgr);
        when(globalStateMgr.getConnectorMgr()).thenReturn(connectorMgr);
        when(globalStateMgr.getMetadataMgr()).thenReturn(metadataMgr);
        when(globalStateMgr.getVariableMgr()).thenReturn(variableMgr);

        when(connectorMgr.getConnector(anyString())).thenReturn(new CatalogConnector(
                mockConnector,
                new InformationSchemaConnector(CATALOG_NAME),
                new TableMetaConnector(CATALOG_NAME, "bigquery")));
        when(metadataMgr.getRemoteFiles(any(), any())).thenReturn(ImmutableList.of(fileInfo));
    }
}
