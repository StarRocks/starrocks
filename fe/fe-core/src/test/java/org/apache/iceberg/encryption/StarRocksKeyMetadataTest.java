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

package org.apache.iceberg.encryption;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

/**
 * StarRocksKeyMetadata is the only thing standing between a StarRocks-written encrypted file and
 * another engine being able to read it: it is the byte layout Iceberg stores in the manifest's
 * key_metadata column. This test lives in org.apache.iceberg.encryption so it can reach Iceberg's
 * own package-private StandardKeyMetadata and check both directions against it, rather than only
 * checking that StarRocks round-trips with itself -- which would pass just as happily on a format
 * no other engine understands.
 */
public class StarRocksKeyMetadataTest {

    private static byte[] bytes(int len, int seed) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) (seed + i);
        }
        return out;
    }

    @Test
    public void testRoundTripAllValidKeyLengths() {
        // 16, 24 and 32 are the lengths Parquet Modular Encryption accepts (AES-128/192/256).
        for (int keyLen : new int[] {16, 24, 32}) {
            byte[] dek = bytes(keyLen, 1);
            byte[] aadPrefix = bytes(16, 100);

            StarRocksKeyMetadata.Parsed parsed = StarRocksKeyMetadata.parse(
                    StarRocksKeyMetadata.serialize(dek, aadPrefix));

            Assertions.assertArrayEquals(dek, parsed.dek, "DEK must survive a round trip at length " + keyLen);
            Assertions.assertArrayEquals(aadPrefix, parsed.aadPrefix, "AAD prefix must survive the round trip");
        }
    }

    @Test
    public void testStarRocksBytesAreReadableByIcebergsOwnParser() {
        // The interop direction that matters for Spark/Trino reading a StarRocks-written table.
        byte[] dek = bytes(32, 7);
        byte[] aadPrefix = bytes(16, 70);

        StandardKeyMetadata icebergView = StandardKeyMetadata.parse(StarRocksKeyMetadata.serialize(dek, aadPrefix));

        Assertions.assertArrayEquals(dek, toArray(icebergView.encryptionKey()));
        Assertions.assertArrayEquals(aadPrefix, toArray(icebergView.aadPrefix()));
    }

    @Test
    public void testIcebergBytesAreReadableByStarRocks() {
        // The reverse direction: StarRocks reading a table another engine wrote.
        byte[] dek = bytes(16, 11);
        byte[] aadPrefix = bytes(16, 110);

        StarRocksKeyMetadata.Parsed parsed =
                StarRocksKeyMetadata.parse(new StandardKeyMetadata(dek, aadPrefix).buffer());

        Assertions.assertArrayEquals(dek, parsed.dek);
        Assertions.assertArrayEquals(aadPrefix, parsed.aadPrefix);
    }

    @Test
    public void testParseDoesNotConsumeTheCallersBuffer() {
        // parse() reads through a ByteBuffer, and a buffer whose position was advanced by a previous
        // read yields a truncated or empty key on the next one. Since the same key_metadata buffer
        // comes off a ContentFile and may be read more than once per query, parsing must be
        // repeatable and must leave the caller's buffer where it found it.
        byte[] dek = bytes(32, 3);
        byte[] aadPrefix = bytes(16, 30);
        ByteBuffer keyMetadata = StarRocksKeyMetadata.serialize(dek, aadPrefix);
        int positionBefore = keyMetadata.position();

        StarRocksKeyMetadata.Parsed first = StarRocksKeyMetadata.parse(keyMetadata);
        Assertions.assertEquals(positionBefore, keyMetadata.position(), "parse must not advance the caller's buffer");

        StarRocksKeyMetadata.Parsed second = StarRocksKeyMetadata.parse(keyMetadata);
        Assertions.assertArrayEquals(first.dek, second.dek, "a second parse of the same buffer must agree");
        Assertions.assertArrayEquals(first.aadPrefix, second.aadPrefix);
    }

    @Test
    public void testEmptyAadPrefixRoundTrips() {
        // IcebergMetadata.buildDataFile passes new byte[0] when the writer reported no AAD prefix,
        // so the empty case has to be representable rather than throwing.
        byte[] dek = bytes(16, 5);

        StarRocksKeyMetadata.Parsed parsed =
                StarRocksKeyMetadata.parse(StarRocksKeyMetadata.serialize(dek, new byte[0]));

        Assertions.assertArrayEquals(dek, parsed.dek);
        Assertions.assertTrue(parsed.aadPrefix == null || parsed.aadPrefix.length == 0,
                "an empty AAD prefix must come back empty or null, not as arbitrary bytes");
    }

    private static byte[] toArray(ByteBuffer buffer) {
        ByteBuffer dup = buffer.duplicate();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
    }
}
