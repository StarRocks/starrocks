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

package com.starrocks.connector.share.credential;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class AwsSseCUtilTest {

    // A valid SSE-C key is a base64-encoded 256-bit (32-byte) value.
    private static final String VALID_KEY =
            Base64.getEncoder().encodeToString("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));

    @Test
    public void testIsSseCEnabled() {
        Assertions.assertFalse(AwsSseCUtil.isSseCEnabled(new HashMap<>()));

        Map<String, String> none = new HashMap<>();
        none.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "none");
        Assertions.assertFalse(AwsSseCUtil.isSseCEnabled(none));

        Map<String, String> enabled = new HashMap<>();
        enabled.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "SSE-C");
        Assertions.assertTrue(AwsSseCUtil.isSseCEnabled(enabled));
    }

    @Test
    public void testValidateAndComputeMd5() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "sse-c");
        props.put(CloudConfigurationConstants.AWS_S3_SSE_KEY, VALID_KEY);

        String md5 = AwsSseCUtil.validateAndGetKeyMd5(props);

        MessageDigest expected = MessageDigest.getInstance("MD5");
        String expectedMd5 = Base64.getEncoder().encodeToString(
                expected.digest(Base64.getDecoder().decode(VALID_KEY)));
        Assertions.assertEquals(expectedMd5, md5);
    }

    @Test
    public void testValidateReturnsNullWhenDisabled() {
        Assertions.assertNull(AwsSseCUtil.validateAndGetKeyMd5(new HashMap<>()));

        Map<String, String> none = new HashMap<>();
        none.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "none");
        Assertions.assertNull(AwsSseCUtil.validateAndGetKeyMd5(none));
    }

    @Test
    public void testValidateRejectsUnknownType() {
        Map<String, String> props = new HashMap<>();
        props.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "sse-kms");
        props.put(CloudConfigurationConstants.AWS_S3_SSE_KEY, VALID_KEY);
        Assertions.assertThrows(IllegalArgumentException.class, () -> AwsSseCUtil.validateAndGetKeyMd5(props));
    }

    @Test
    public void testValidateRejectsMissingKey() {
        Map<String, String> props = new HashMap<>();
        props.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "sse-c");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AwsSseCUtil.validateAndGetKeyMd5(props));
    }

    @Test
    public void testValidateRejectsWrongLengthKey() {
        Map<String, String> props = new HashMap<>();
        props.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "sse-c");
        props.put(CloudConfigurationConstants.AWS_S3_SSE_KEY,
                Base64.getEncoder().encodeToString("too-short".getBytes(StandardCharsets.UTF_8)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> AwsSseCUtil.validateAndGetKeyMd5(props));
    }

    @Test
    public void testValidateRejectsNonBase64Key() {
        Map<String, String> props = new HashMap<>();
        props.put(CloudConfigurationConstants.AWS_S3_SSE_TYPE, "sse-c");
        props.put(CloudConfigurationConstants.AWS_S3_SSE_KEY, "not valid base64 !!!");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AwsSseCUtil.validateAndGetKeyMd5(props));
    }
}
