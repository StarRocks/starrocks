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
import com.starrocks.thrift.TPaimonCommitInfo;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void testIndividuallyValidProducerPayloadsMayExceed64MiBTogether() throws IOException {
        int limit = 64 * 1024 * 1024;
        byte[] partitionBytes = new byte[24 * 1024 * 1024];
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition, partitionBytes.length + 1024);
        writer.writeBinary(0, partitionBytes, 0, partitionBytes.length);
        writer.complete();
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        DataOutputSerializer output = new DataOutputSerializer(partitionBytes.length + 1024);
        serializer.serializeList(List.of(new CommitMessageImpl(partition, 0, null,
                DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement())), output);
        byte[] payload = output.getCopyOfBuffer();
        assertTrue(payload.length < limit);
        assertTrue(payload.length * 3 > limit);

        PaimonSinkExtra operation = mock(PaimonSinkExtra.class);
        when(operation.getOperationId()).thenReturn("operation");
        when(operation.getTableIdentity()).thenReturn("table");
        when(operation.getLocation()).thenReturn("file:/paimon");
        when(operation.getPartitionColumns()).thenReturn(List.of("p"));
        when(operation.getBucketCount()).thenReturn(-1);

        List<TSinkCommitInfo> reports = List.of(
                producerReport("driver-1", serializer.getVersion(), payload),
                producerReport("driver-2", serializer.getVersion(), payload),
                producerReport("driver-3", serializer.getVersion(), payload));
        assertDoesNotThrow(() -> PaimonCommitCodec.collect(reports, operation));
    }

    private static TSinkCommitInfo producerReport(String producerId, int version, byte[] payload) {
        TPaimonCommitInfo info = new TPaimonCommitInfo();
        info.setOperation_id("operation");
        info.setTable_identity("table");
        info.setSchema_id(0);
        info.setProducer_id(producerId);
        info.setAttempt_id(0);
        info.setSerializer_version(version);
        info.setCommit_messages(payload);
        info.setPrepare_complete(true);
        info.setRow_count(0);
        return new TSinkCommitInfo().setPaimon_commit_info(info);
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
