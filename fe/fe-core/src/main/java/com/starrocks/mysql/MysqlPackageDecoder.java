// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql;

import com.google.api.client.util.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class MysqlPackageDecoder {
    protected static final Logger LOG = LogManager.getLogger(MysqlPackageDecoder.class);
    protected static final int PACKET_HEADER_LEN = 4;
    protected static final int MAX_PHYSICAL_PACKET_LENGTH = 0xffffff;
    private final Queue<RequestPackage> queue = new ArrayDeque<>();
    private final List<RequestPackage> unfinishedPackage = Lists.newArrayList();

    private int sequenceId = 0;
    private ByteBuffer currentPayload = null;
    private ByteBuffer headerPackage = null;

    private static final class HeaderDecoder {
        private final int length;
        private final int seq;

        HeaderDecoder(ByteBuffer header) {
            header.flip();
            this.length = ((header.get() & 0xFF))
                    | ((header.get() & 0xFF) << 8)
                    | ((header.get() & 0xFF) << 16);
            this.seq = header.get() & 0xFF;
            header.clear();
        }

        int getLength() {
            return length;
        }

        int getSeq() {
            return seq;
        }
    }

    public void consume(ByteBuffer srcBuffer) {
        while (srcBuffer.hasRemaining()) {
            if (headerPackage == null) {
                headerPackage = ByteBuffer.allocate(PACKET_HEADER_LEN);
            }

            if (headerPackage.hasRemaining()) {
                copy(srcBuffer, headerPackage);
            }
            // can not get enough
            if (headerPackage.hasRemaining()) {
                return;
            }
            // start fill payload
            final HeaderDecoder headerDecoder = new HeaderDecoder(headerPackage);
            final int packageLen = headerDecoder.getLength();

            if (packageLen <= 0) {
                LOG.warn("invalid package length:{}", packageLen);
                throw new IllegalStateException("Invalid MySQL packet length: " + packageLen);
            }

            if (currentPayload == null) {
                currentPayload = ByteBuffer.allocate(packageLen);
            }
            if (sequenceId != headerDecoder.getSeq()) {
                LOG.warn("receive packet sequence id[{}] expected[{}]", headerDecoder.getSeq(), sequenceId);
                throw new IllegalStateException(
                        "receive packet sequence id[" + headerDecoder.getSeq() + "] expected[" + sequenceId + "]");
            }

            copy(srcBuffer, currentPayload);
            if (currentPayload.hasRemaining()) {
                return;
            }

            currentPayload.flip();
            final RequestPackage req = new RequestPackage(headerDecoder.getSeq(), currentPayload);
            unfinishedPackage.add(req);

            accSequenceId();

            if (packageLen != MAX_PHYSICAL_PACKET_LENGTH) {
                final RequestPackage mergedPackages = mergePackages(unfinishedPackage);
                queue.add(mergedPackages);
                unfinishedPackage.clear();
                sequenceId = 0;
            }

            currentPayload = null;
            headerPackage = null;
        }
    }

    public RequestPackage poll() {
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

    private static RequestPackage mergePackages(List<RequestPackage> parts) {
        if (parts.size() == 1) {
            return parts.get(0);
        }

        int totalSize = parts.stream()
                .mapToInt(p -> p.getByteBuffer().remaining())
                .sum();

        ByteBuffer merged = ByteBuffer.allocate(totalSize);
        for (RequestPackage p : parts) {
            merged.put(p.getByteBuffer().duplicate());
        }
        merged.flip();

        int seq = parts.get(parts.size() - 1).getPackageId();
        return new RequestPackage(seq, merged);
    }

    private void accSequenceId() {
        sequenceId = (sequenceId + 1) & 0xFF;
    }
}
