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

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

public class StaticKeyManagementClientTest {
    private static final byte[] KEY_A = filled(32, (byte) 1);
    private static final byte[] KEY_B = filled(16, (byte) 2);

    private static byte[] filled(int length, byte value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private static String spec(String id, byte[] key) {
        return id + ":" + Base64.getEncoder().encodeToString(key);
    }

    private static StaticKeyManagementClient client(String spec) {
        StaticKeyManagementClient client = new StaticKeyManagementClient();
        client.initialize(ImmutableMap.of(StaticKeyManagementClient.KEYS_PROPERTY, spec));
        return client;
    }

    @Test
    public void testWrapThenUnwrapReturnsTheKey() {
        StaticKeyManagementClient client = client(spec("a", KEY_A) + "," + spec("b", KEY_B));
        byte[] dek = filled(16, (byte) 7);
        for (String keyId : new String[] {"a", "b"}) {
            ByteBuffer wrapped = client.wrapKey(ByteBuffer.wrap(dek), keyId);
            Assertions.assertFalse(Arrays.equals(dek, ByteBuffers.toByteArray(wrapped)), "wrapped key is the DEK");
            Assertions.assertArrayEquals(dek, ByteBuffers.toByteArray(client.unwrapKey(wrapped, keyId)));
        }
    }

    @Test
    public void testAKeyWrappedUnderOneIdDoesNotUnwrapUnderAnother() {
        // Both ids hold the same bytes, so only the key id bound into the AAD tells them apart.
        StaticKeyManagementClient client = client(spec("a", KEY_A) + "," + spec("a2", KEY_A));
        ByteBuffer wrapped = client.wrapKey(ByteBuffer.wrap(filled(16, (byte) 7)), "a");
        Assertions.assertThrows(RuntimeException.class, () -> client.unwrapKey(wrapped, "a2"));
    }

    @Test
    public void testATamperedWrappedKeyIsRejected() {
        StaticKeyManagementClient client = client(spec("a", KEY_A));
        byte[] wrapped = ByteBuffers.toByteArray(client.wrapKey(ByteBuffer.wrap(filled(16, (byte) 7)), "a"));
        wrapped[wrapped.length - 1] ^= 1;
        Assertions.assertThrows(RuntimeException.class, () -> client.unwrapKey(ByteBuffer.wrap(wrapped), "a"));
    }

    @Test
    public void testAnUnknownKeyIdIsRejected() {
        StaticKeyManagementClient client = client(spec("a", KEY_A));
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> client.wrapKey(ByteBuffer.wrap(filled(16, (byte) 7)), "missing"));
        Assertions.assertTrue(e.getMessage().contains("no master key missing"), e.getMessage());
    }

    @Test
    public void testMalformedConfigurationIsRejectedWithoutEchoingTheKey() {
        String badLength = spec("a", filled(20, (byte) 1));
        String[] specs = {"", "no-separator", ":" + Base64.getEncoder().encodeToString(KEY_A), "a:not*base64",
                badLength, spec("a", KEY_A) + "," + spec("a", KEY_B)};
        for (String spec : specs) {
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> client(spec),
                    spec);
            Assertions.assertFalse(e.getMessage().contains(Base64.getEncoder().encodeToString(KEY_A)), e.getMessage());
            Assertions.assertFalse(e.getMessage().contains(badLength.substring(2)), e.getMessage());
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new StaticKeyManagementClient().initialize(ImmutableMap.of()));
    }

    @Test
    public void testIcebergLoadsItFromTheCatalogProperties() {
        // What a Hive catalog does with encryption.kms-impl.
        Map<String, String> catalogProperties = ImmutableMap.of(
                "encryption.kms-impl", StaticKeyManagementClient.class.getName(),
                StaticKeyManagementClient.KEYS_PROPERTY, spec("a", KEY_A));
        KeyManagementClient client = EncryptionUtil.createKmsClient(catalogProperties);
        Assertions.assertInstanceOf(StaticKeyManagementClient.class, client);
        byte[] dek = filled(16, (byte) 7);
        Assertions.assertArrayEquals(dek,
                ByteBuffers.toByteArray(client.unwrapKey(client.wrapKey(ByteBuffer.wrap(dek), "a"), "a")));
    }
}
