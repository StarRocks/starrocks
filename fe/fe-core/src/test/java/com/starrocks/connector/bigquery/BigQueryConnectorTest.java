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

import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class BigQueryConnectorTest {

    private ConnectorContext mockContext(Map<String, String> props) {
        ConnectorContext ctx = Mockito.mock(ConnectorContext.class);
        when(ctx.getCatalogName()).thenReturn("bq_catalog");
        when(ctx.getType()).thenReturn("bigquery");
        when(ctx.getProperties()).thenReturn(props);
        return ctx;
    }

    // ---- ConnectorType registration ----

    @Test
    public void testBigQueryIsRegisteredInConnectorType() {
        Assertions.assertTrue(ConnectorType.isSupport("bigquery"));
        Assertions.assertTrue(ConnectorType.isSupport("BIGQUERY"));
        ConnectorType type = ConnectorType.from("bigquery");
        Assertions.assertEquals(BigQueryConnector.class, type.getConnectorClass());
    }

    // ---- Properties validation ----

    @Test
    public void testPropertiesMissingProjectIdThrows() {
        Map<String, String> props = new HashMap<>();
        // No project id
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new BigQueryProperties(props));
    }

    @Test
    public void testPropertiesBothCredentialsThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        props.put(BigQueryProperties.CREDENTIALS_JSON, "{\"type\":\"service_account\"}");
        props.put(BigQueryProperties.CREDENTIALS_FILE, "/path/to/sa.json");
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new BigQueryProperties(props));
    }

    // ---- BigQueryProperties defaults ----

    @Test
    public void testDefaultLocationIsUS() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertEquals("US", p.get(BigQueryProperties.LOCATION));
    }

    @Test
    public void testDefaultViewEnabled() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertTrue(p.getBoolean(BigQueryProperties.VIEW_ENABLED));
    }

    @Test
    public void testDefaultMaterializeDataset() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertEquals("_bq_tmp_sr_", p.get(BigQueryProperties.VIEW_MATERIALIZE_DATASET));
    }

    @Test
    public void testDefaultViewJobTimeout() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertEquals(300L, p.getLong(BigQueryProperties.VIEW_JOB_TIMEOUT_SECONDS));
    }

    // ---- Custom property overrides ----

    @Test
    public void testCustomLocation() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        props.put(BigQueryProperties.LOCATION, "EU");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertEquals("EU", p.get(BigQueryProperties.LOCATION));
    }

    @Test
    public void testCustomMaxStreams() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        props.put(BigQueryProperties.MAX_STREAMS, "8");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertEquals(8, p.getInt(BigQueryProperties.MAX_STREAMS));
    }

    @Test
    public void testViewDisabledByProperty() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        props.put(BigQueryProperties.VIEW_ENABLED, "false");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertFalse(p.getBoolean(BigQueryProperties.VIEW_ENABLED));
    }

    @Test
    public void testDatasetFilterProperty() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "p");
        props.put(BigQueryProperties.DATASET_FILTER, "ds1,ds2,ds3");
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertEquals("ds1,ds2,ds3", p.get(BigQueryProperties.DATASET_FILTER));
    }

    // ---- ADC as implicit default ----

    @Test
    public void testAdcIsImplicitDefaultWhenNoCredentialsSet() {
        // When neither credentials.json nor credentials.file is provided,
        // auth.type defaults to application_default. Validate that no
        // exception is thrown when constructing BigQueryProperties.
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "my-project");
        // No auth properties at all — ADC is the implicit default
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertNull(p.get(BigQueryProperties.CREDENTIALS_JSON));
        Assertions.assertNull(p.get(BigQueryProperties.CREDENTIALS_FILE));
        // auth.type defaults to null — connector uses ADC when both are absent
        Assertions.assertNull(p.get(BigQueryProperties.AUTH_TYPE));
    }

    @Test
    public void testExplicitAdcAuthType() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "my-project");
        props.put(BigQueryProperties.AUTH_TYPE, BigQueryProperties.AUTH_TYPE_APPLICATION_DEFAULT);
        BigQueryProperties p = new BigQueryProperties(props);
        Assertions.assertEquals(BigQueryProperties.AUTH_TYPE_APPLICATION_DEFAULT,
                p.get(BigQueryProperties.AUTH_TYPE));
    }
}
