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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class IcebergConnectorTest {

    private static final String VALID_SSE_C_KEY =
            Base64.getEncoder().encodeToString("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));

    @Test
    public void testWithIcebergSsePropertiesTranslatesToS3FileIO() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "ak");
        properties.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "sse-c");
        properties.put(CloudConfigurationConstants.AWS_S3_SSE_KEY, VALID_SSE_C_KEY);

        Map<String, String> result = IcebergConnector.withIcebergSseProperties(properties);

        // Original map is left untouched; a new augmented map is returned.
        Assertions.assertFalse(properties.containsKey(S3FileIOProperties.SSE_TYPE));
        Assertions.assertEquals(S3FileIOProperties.SSE_TYPE_CUSTOM, result.get(S3FileIOProperties.SSE_TYPE));
        Assertions.assertEquals(VALID_SSE_C_KEY, result.get(S3FileIOProperties.SSE_KEY));
        Assertions.assertNotNull(result.get(S3FileIOProperties.SSE_MD5));
    }

    @Test
    public void testWithIcebergSsePropertiesNoopWhenDisabled() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "ak");

        Map<String, String> result = IcebergConnector.withIcebergSseProperties(properties);

        Assertions.assertSame(properties, result);
        Assertions.assertFalse(result.containsKey(S3FileIOProperties.SSE_TYPE));
    }
}
