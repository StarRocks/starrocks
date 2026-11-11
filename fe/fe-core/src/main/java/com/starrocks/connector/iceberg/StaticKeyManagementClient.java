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

import com.google.common.base.Preconditions;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * An Iceberg {@link KeyManagementClient} whose master keys are given in the catalog properties.
 *
 * <p><b>For testing only.</b> A real KMS never hands out its master keys; this one is configured with
 * them, in the clear, so anyone who can read the catalog definition can unwrap every data key. It exists
 * so an end-to-end test can write and read an encrypted Iceberg table on a Hive catalog without a cloud
 * KMS: Iceberg's production jars ship only AWS, GCP and Azure clients, and its in-memory test client
 * lives in the iceberg-core test jar.
 *
 * <p>Configure it on an Iceberg Hive catalog:
 * <pre>
 *   "encryption.kms-impl" = "com.starrocks.connector.iceberg.StaticKeyManagementClient",
 *   "encryption.static-kms.keys" = "key-id-1:&lt;base64 AES key&gt;,key-id-2:&lt;base64 AES key&gt;"
 * </pre>
 * Keys are 16, 24 or 32 bytes. Wrapping is AES-GCM under the named master key, with the key id as AAD.
 */
public class StaticKeyManagementClient implements KeyManagementClient {
    public static final String KEYS_PROPERTY = "encryption.static-kms.keys";

    private Map<String, byte[]> masterKeys;

    @Override
    public void initialize(Map<String, String> properties) {
        String spec = properties.get(KEYS_PROPERTY);
        Preconditions.checkArgument(spec != null && !spec.isBlank(), "%s is required", KEYS_PROPERTY);
        Map<String, byte[]> keys = new HashMap<>();
        for (String entry : spec.split(",")) {
            int sep = entry.indexOf(':');
            // The entry holds a key, so errors name only its position in the list, never its content.
            Preconditions.checkArgument(sep > 0, "%s entry %s is not <key id>:<base64 key>",
                    KEYS_PROPERTY, keys.size() + 1);
            String keyId = entry.substring(0, sep).trim();
            byte[] key;
            try {
                key = Base64.getDecoder().decode(entry.substring(sep + 1).trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format("%s key %s is not valid base64", KEYS_PROPERTY, keyId));
            }
            Preconditions.checkArgument(key.length == 16 || key.length == 24 || key.length == 32,
                    "%s key %s must be 16, 24 or 32 bytes, got %s", KEYS_PROPERTY, keyId, key.length);
            Preconditions.checkArgument(keys.put(keyId, key) == null, "%s lists key %s twice", KEYS_PROPERTY,
                    keyId);
        }
        masterKeys = keys;
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
        byte[] wrapped = new Ciphers.AesGcmEncryptor(masterKey(wrappingKeyId))
                .encrypt(ByteBuffers.toByteArray(key), aad(wrappingKeyId));
        return ByteBuffer.wrap(wrapped);
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
        byte[] key = new Ciphers.AesGcmDecryptor(masterKey(wrappingKeyId))
                .decrypt(ByteBuffers.toByteArray(wrappedKey), aad(wrappingKeyId));
        return ByteBuffer.wrap(key);
    }

    private byte[] masterKey(String keyId) {
        Preconditions.checkState(masterKeys != null, "%s is not initialized", getClass().getSimpleName());
        byte[] key = masterKeys.get(keyId);
        Preconditions.checkArgument(key != null, "no master key %s in %s", keyId, KEYS_PROPERTY);
        return key;
    }

    // Binds a wrapped key to the master key id, so it cannot be unwrapped under a different id that happens to
    // hold the same key bytes.
    private static byte[] aad(String keyId) {
        return keyId.getBytes(StandardCharsets.UTF_8);
    }
}
