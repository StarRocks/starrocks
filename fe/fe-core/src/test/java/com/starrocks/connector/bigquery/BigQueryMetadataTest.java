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
import com.google.cloud.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.starrocks.catalog.BigQueryTable;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryMetadataTest {

    @Mock
    private BigQuery mockBigQuery;
    @Mock
    private BigQueryReadClient mockReadClient;
    @Mock
    private GoogleCredentials mockCredentials;

    private BigQueryMetadata metadata;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "test-project");
        BigQueryProperties properties = new BigQueryProperties(props);
        metadata = new BigQueryMetadata(mockBigQuery, mockReadClient, mockCredentials, "bq_catalog", properties);
    }

    @Test
    public void testListDbNamesReturnsDatasets() {
        Dataset ds1 = mock(Dataset.class);
        when(ds1.getDatasetId()).thenReturn(DatasetId.of("test-project", "dataset1"));
        Dataset ds2 = mock(Dataset.class);
        when(ds2.getDatasetId()).thenReturn(DatasetId.of("test-project", "dataset2"));

        Page<Dataset> page = mock(Page.class);
        when(page.iterateAll()).thenReturn(Arrays.asList(ds1, ds2));
        when(mockBigQuery.listDatasets("test-project")).thenReturn(page);

        List<String> names = metadata.listDbNames(null);
        Assert.assertEquals(2, names.size());
        Assert.assertTrue(names.contains("dataset1"));
        Assert.assertTrue(names.contains("dataset2"));
    }

    @Test
    public void testGetTableReturnsTable() {
        Schema schema = Schema.of(
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING)
        );
        StandardTableDefinition def = StandardTableDefinition.of(schema);

        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.getDefinition()).thenReturn(def);
        when(tableInfo.getCreationTime()).thenReturn(1000L);

        when(mockBigQuery.getTable(TableId.of("test-project", "ds", "tbl"))).thenReturn(tableInfo);

        com.starrocks.catalog.Table table = metadata.getTable(null, "ds", "tbl");
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof BigQueryTable);
        BigQueryTable bqTable = (BigQueryTable) table;
        Assert.assertFalse(bqTable.isView());
        Assert.assertEquals(2, bqTable.getFullSchema().size());
    }

    @Test
    public void testGetTableForViewReturnsViewTable() {
        Schema schema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));
        com.google.cloud.bigquery.ViewDefinition viewDef = mock(com.google.cloud.bigquery.ViewDefinition.class);
        when(viewDef.getType()).thenReturn(TableDefinition.Type.VIEW);
        when(viewDef.getSchema()).thenReturn(schema);

        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.getDefinition()).thenReturn(viewDef);
        when(tableInfo.getCreationTime()).thenReturn(1000L);

        when(mockBigQuery.getTable(TableId.of("test-project", "ds", "my_view"))).thenReturn(tableInfo);

        com.starrocks.catalog.Table table = metadata.getTable(null, "ds", "my_view");
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof BigQueryTable);
        BigQueryTable bqTable = (BigQueryTable) table;
        Assert.assertTrue(bqTable.isView());
    }

    @Test
    public void testGetTableReturnsNullWhenViewDisabled() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "test-project");
        props.put(BigQueryProperties.VIEW_ENABLED, "false");
        BigQueryMetadata metaNoViews = new BigQueryMetadata(
                mockBigQuery, mockReadClient, mockCredentials, "bq_catalog", new BigQueryProperties(props));

        com.google.cloud.bigquery.ViewDefinition viewDef = mock(com.google.cloud.bigquery.ViewDefinition.class);
        when(viewDef.getType()).thenReturn(TableDefinition.Type.VIEW);
        when(viewDef.getSchema()).thenReturn(Schema.of(Field.of("id", StandardSQLTypeName.INT64)));

        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.getDefinition()).thenReturn(viewDef);
        when(mockBigQuery.getTable(TableId.of("test-project", "ds", "v1"))).thenReturn(tableInfo);

        com.starrocks.catalog.Table result = metaNoViews.getTable(null, "ds", "v1");
        Assert.assertNull(result);
    }

    @Test
    public void testGetRemoteFilesReturnsOneDescPerStream() {
        // Set up a BigQueryTable for use.
        Schema schema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));
        BigQueryTable bqTable = new BigQueryTable("bq_catalog", "ds", "tbl",
                BigQuerySchemaUtils.toStarRocksColumns(schema), 1000L, false);

        // Mock CreateReadSession response with 3 streams.
        ReadSession session = ReadSession.newBuilder()
                .setName("projects/test-project/locations/us/sessions/abc123")
                .addStreams(ReadStream.newBuilder().setName("projects/test-project/locations/us/sessions/abc123/streams/0"))
                .addStreams(ReadStream.newBuilder().setName("projects/test-project/locations/us/sessions/abc123/streams/1"))
                .addStreams(ReadStream.newBuilder().setName("projects/test-project/locations/us/sessions/abc123/streams/2"))
                .build();
        when(mockReadClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(session);

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setFieldNames(Collections.singletonList("id"))
                .build();

        List<RemoteFileInfo> infos = metadata.getRemoteFiles(bqTable, params);
        Assert.assertEquals(1, infos.size());
        Assert.assertEquals(3, infos.get(0).getFiles().size());

        BigQueryRemoteFileDesc desc = (BigQueryRemoteFileDesc) infos.get(0).getFiles().get(0);
        Assert.assertEquals("projects/test-project/locations/us/sessions/abc123", desc.getReadSessionName());
        Assert.assertEquals(0, desc.getStreamIndex());
        Assert.assertFalse(desc.isTempTable());
    }
}
