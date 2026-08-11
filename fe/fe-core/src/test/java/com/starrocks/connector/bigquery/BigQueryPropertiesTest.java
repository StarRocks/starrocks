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

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BigQueryPropertiesTest {

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BigQueryProperties.PROJECT_ID, "my-project");
        return props;
    }

    @Test
    public void testMissingProjectIdThrows() {
        Map<String, String> props = new HashMap<>();
        try {
            new BigQueryProperties(props);
            Assert.fail("Expected StarRocksConnectorException");
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains(BigQueryProperties.PROJECT_ID));
        }
    }

    @Test
    public void testValidWithNoCredentials() {
        // ADC is the implicit default when no credentials are provided.
        BigQueryProperties p = new BigQueryProperties(baseProps());
        Assert.assertEquals("my-project", p.get(BigQueryProperties.PROJECT_ID));
    }

    @Test
    public void testValidWithCredentialsJson() {
        Map<String, String> props = baseProps();
        props.put(BigQueryProperties.CREDENTIALS_JSON, "{\"type\":\"service_account\"}");
        BigQueryProperties p = new BigQueryProperties(props);
        Assert.assertNotNull(p.get(BigQueryProperties.CREDENTIALS_JSON));
    }

    @Test
    public void testValidWithCredentialsFile() {
        Map<String, String> props = baseProps();
        props.put(BigQueryProperties.CREDENTIALS_FILE, "/path/to/sa.json");
        BigQueryProperties p = new BigQueryProperties(props);
        Assert.assertEquals("/path/to/sa.json", p.get(BigQueryProperties.CREDENTIALS_FILE));
    }

    @Test
    public void testBothCredentialsJsonAndFileThrows() {
        Map<String, String> props = baseProps();
        props.put(BigQueryProperties.CREDENTIALS_JSON, "{\"type\":\"service_account\"}");
        props.put(BigQueryProperties.CREDENTIALS_FILE, "/path/to/sa.json");
        try {
            new BigQueryProperties(props);
            Assert.fail("Expected StarRocksConnectorException");
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains(BigQueryProperties.CREDENTIALS_JSON));
        }
    }

    @Test
    public void testDefaultValues() {
        BigQueryProperties p = new BigQueryProperties(baseProps());
        Assert.assertEquals("US", p.get(BigQueryProperties.LOCATION));
        Assert.assertEquals("0", p.get(BigQueryProperties.MAX_STREAMS));
        Assert.assertEquals("true", p.get(BigQueryProperties.VIEW_ENABLED));
        Assert.assertEquals("_bq_tmp_sr_", p.get(BigQueryProperties.VIEW_MATERIALIZE_DATASET));
        Assert.assertEquals("300", p.get(BigQueryProperties.VIEW_JOB_TIMEOUT_SECONDS));
    }

    @Test
    public void testBooleanAndLongHelpers() {
        Map<String, String> props = baseProps();
        props.put(BigQueryProperties.VIEW_ENABLED, "false");
        props.put(BigQueryProperties.VIEW_JOB_TIMEOUT_SECONDS, "600");
        BigQueryProperties p = new BigQueryProperties(props);
        Assert.assertFalse(p.getBoolean(BigQueryProperties.VIEW_ENABLED));
        Assert.assertEquals(600L, p.getLong(BigQueryProperties.VIEW_JOB_TIMEOUT_SECONDS));
    }
}
