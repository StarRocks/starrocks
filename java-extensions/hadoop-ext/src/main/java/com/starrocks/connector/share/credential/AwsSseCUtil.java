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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;

/**
 * Helpers for S3 Server-Side Encryption with Customer-provided keys (SSE-C).
 *
 * SSE-C requires the caller to send the customer key on every GetObject/HeadObject request. StarRocks
 * accepts the key on an external catalog through the {@code aws.s3.sse.*} properties; this class parses,
 * validates, and normalizes that material so both the FE metadata reader (Iceberg S3FileIO) and the BE
 * data reader receive a consistent key and MD5.
 */
public class AwsSseCUtil {
    // Value of the x-amz-server-side-encryption-customer-algorithm header; AWS only supports AES256.
    public static final String SSE_C_ALGORITHM = "AES256";
    // Value of the Hadoop S3A "fs.s3a.encryption.algorithm" key that selects SSE-C.
    public static final String SSE_C_ALGORITHM_S3A = "SSE-C";
    // A 256-bit key, i.e. 32 raw bytes.
    private static final int SSE_C_KEY_LENGTH_BYTES = 32;

    private AwsSseCUtil() {
    }

    /**
     * Returns true when the properties request SSE-C.
     */
    public static boolean isSseCEnabled(Map<String, String> properties) {
        String type = properties.get(CloudConfigurationConstants.AWS_S3_SSE_TYPE);
        return type != null && CloudConfigurationConstants.AWS_S3_SSE_TYPE_SSE_C.equalsIgnoreCase(type.trim());
    }

    /**
     * Validates the SSE-C properties and, when enabled, returns the base64-encoded MD5 of the key (either
     * the caller-provided one or a freshly computed one). Returns null when SSE-C is not enabled.
     *
     * @throws IllegalArgumentException if the type is unknown, the key is missing, or the key is not a
     *         base64-encoded 256-bit value.
     */
    public static String validateAndGetKeyMd5(Map<String, String> properties) {
        String type = properties.get(CloudConfigurationConstants.AWS_S3_SSE_TYPE);
        if (type == null || type.trim().isEmpty()
                || CloudConfigurationConstants.AWS_S3_SSE_TYPE_NONE.equalsIgnoreCase(type.trim())) {
            return null;
        }
        if (!CloudConfigurationConstants.AWS_S3_SSE_TYPE_SSE_C.equalsIgnoreCase(type.trim())) {
            throw new IllegalArgumentException(String.format(
                    "Unsupported value '%s' for property '%s'. Only '%s' is supported.",
                    type, CloudConfigurationConstants.AWS_S3_SSE_TYPE,
                    CloudConfigurationConstants.AWS_S3_SSE_TYPE_SSE_C));
        }

        String key = properties.get(CloudConfigurationConstants.AWS_S3_SSE_KEY);
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' is required when '%s' is '%s'.",
                    CloudConfigurationConstants.AWS_S3_SSE_KEY, CloudConfigurationConstants.AWS_S3_SSE_TYPE,
                    CloudConfigurationConstants.AWS_S3_SSE_TYPE_SSE_C));
        }

        byte[] rawKey;
        try {
            rawKey = Base64.getDecoder().decode(key.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be a base64-encoded 256-bit key.",
                    CloudConfigurationConstants.AWS_S3_SSE_KEY), e);
        }
        if (rawKey.length != SSE_C_KEY_LENGTH_BYTES) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must decode to a 256-bit (32-byte) key, but got %d bytes.",
                    CloudConfigurationConstants.AWS_S3_SSE_KEY, rawKey.length));
        }

        return computeMd5(rawKey);
    }

    /**
     * Computes the base64-encoded MD5 digest of the raw key bytes, as required by the
     * x-amz-server-side-encryption-customer-key-MD5 header.
     */
    public static String computeMd5(byte[] rawKey) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return Base64.getEncoder().encodeToString(md5.digest(rawKey));
        } catch (NoSuchAlgorithmException e) {
            // MD5 is guaranteed to be available on every JVM.
            throw new IllegalStateException("MD5 algorithm is unavailable", e);
        }
    }
}
