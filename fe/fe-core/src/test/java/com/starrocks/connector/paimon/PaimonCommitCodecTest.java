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

package com.starrocks.connector.paimon;

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonCommitCodecTest {
    @Test
    void testEmptyNativeListHasSeparateVersionAndBigEndianCount() throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        // The pinned C++ CommitMessage::SerializeList writes the count in big-endian order.
        byte[] nativeEmptyList = new byte[] {0, 0, 0, 0};
        assertEquals(12, serializer.getVersion());
        assertTrue(PaimonCommitCodec.decode(12, nativeEmptyList).isEmpty());
        DataOutputSerializer output = new DataOutputSerializer(32);
        serializer.serializeList(List.of(), output);
        assertArrayEquals(nativeEmptyList, output.getCopyOfBuffer());
    }

    @Test
    void testExactPublicNativeVersion12Fixture() throws IOException {
        // Copied byte-for-byte from the pinned Apache paimon-cpp v0.3.0 source archive:
        // test/test_data/orc/pk_btree_source_meta.db/pk_btree_source_meta/commit_messages/commit_messages-01.
        // Its native TestCompatibleWithVersion12 deserializes and reserializes these exact bytes.
        try (InputStream fixture = getClass().getResourceAsStream("/paimon/public-v0.3.0-commit-v12.base64")) {
            org.junit.jupiter.api.Assertions.assertNotNull(fixture);
            byte[] single = Base64.getMimeDecoder().decode(new String(fixture.readAllBytes(), StandardCharsets.US_ASCII));
            assertEquals(1633, single.length);
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            var message = serializer.deserialize(12, single);
            assertArrayEquals(single, serializer.serialize(message));
            byte[] list = ByteBuffer.allocate(Integer.BYTES + single.length).putInt(1).put(single).array();
            assertEquals(1, PaimonCommitCodec.decode(12, list).size());
            assertArrayEquals(single, serializer.serialize(PaimonCommitCodec.decode(12, list).get(0)));
        }
    }

    @Test
    void testVersionIsNotGuessedFromPayload() {
        assertThrows(StarRocksConnectorException.class, () -> PaimonCommitCodec.decode(11, new byte[4]));
        assertThrows(StarRocksConnectorException.class, () -> PaimonCommitCodec.decode(13, new byte[4]));
    }

    @Test
    void testMalformedAndTrailingPayloadsAreRejected() {
        assertThrows(StarRocksConnectorException.class, () -> PaimonCommitCodec.decode(12, null));
        assertThrows(StarRocksConnectorException.class, () -> PaimonCommitCodec.decode(12, new byte[3]));
        assertThrows(StarRocksConnectorException.class,
                () -> PaimonCommitCodec.decode(12, ByteBuffer.allocate(4).putInt(-1).array()));
        assertThrows(StarRocksConnectorException.class,
                () -> PaimonCommitCodec.decode(12, ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array()));
        assertThrows(StarRocksConnectorException.class,
                () -> PaimonCommitCodec.decode(12, ByteBuffer.allocate(8).putInt(1).putInt(0).array()));
        assertThrows(StarRocksConnectorException.class, () -> PaimonCommitCodec.decode(12, new byte[5]));
    }
}
