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

package com.starrocks.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Splits a compressed MySQL protocol stream into compressed packets and restores their payloads, which are then
 * fed to {@link MysqlPackageDecoder}. See {@link MysqlCompression} for the packet layout.
 */
public class MysqlCompressedPacketDecoder {
    public record Packet(int sequenceId, ByteBuffer payload) {
    }

    private final MysqlCompression compression;
    private final Queue<Packet> queue = new ArrayDeque<>();
    private final ByteBuffer header = ByteBuffer.allocate(MysqlCompression.HEADER_LEN);
    private ByteBuffer payload = null;
    private int sequenceId;
    private int uncompressedLen;

    public MysqlCompressedPacketDecoder(MysqlCompression compression) {
        this.compression = compression;
    }

    public void consume(ByteBuffer src) throws IOException {
        while (src.hasRemaining()) {
            if (payload == null) {
                copy(src, header);
                if (header.hasRemaining()) {
                    return;
                }
                header.flip();
                int payloadLen = MysqlCompression.readInt3(header);
                sequenceId = header.get() & 0xFF;
                uncompressedLen = MysqlCompression.readInt3(header);
                header.clear();
                payload = ByteBuffer.allocate(payloadLen);
            }

            copy(src, payload);
            if (payload.hasRemaining()) {
                return;
            }
            byte[] data = compression.decodePayload(payload.array(), uncompressedLen);
            queue.add(new Packet(sequenceId, ByteBuffer.wrap(data)));
            payload = null;
        }
    }

    public Packet poll() {
        return queue.poll();
    }

    private static void copy(ByteBuffer src, ByteBuffer dst) {
        int toCopy = Math.min(src.remaining(), dst.remaining());
        if (toCopy <= 0) {
            return;
        }
        int oldLimit = src.limit();
        src.limit(src.position() + toCopy);
        dst.put(src);
        src.limit(oldLimit);
    }
}
