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

import org.junit.jupiter.api.Test;
import org.wildfly.common.Assert;

import java.nio.ByteBuffer;

public class MysqlPackageDecoderTest {
    @Test
    public void test() {
        final MysqlPackageDecoder decoder = new MysqlPackageDecoder();

        ByteBuffer buf = ByteBuffer.allocate(100);
        buf.put((byte) 9);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.flip();
        decoder.consume(buf);
        buf.compact();
        Assert.assertTrue(decoder.poll() == null);
        Assert.assertTrue(buf.remaining() == 100);
        buf.put((byte) 0);
        Assert.assertTrue(decoder.poll() == null);
        String bytes = "select 1;";
        buf.put(bytes.getBytes());
        buf.flip();
        decoder.consume(buf);
        final RequestPackage pkg = decoder.poll();
        Assert.assertTrue(pkg != null);
        Assert.assertTrue(pkg.getPackageId() == 0);
    }

    @Test
    public void testLargeRequest() {
        final MysqlPackageDecoder decoder = new MysqlPackageDecoder();

        int packageSize = 0xFFFFFF * 5 + 7;
        int leftSize = packageSize;
        int sequenceId = 0x00;
        while (leftSize > 0) {
            ByteBuffer buf = ByteBuffer.allocate(0xFFFFFF * 2);
            int current = Math.min(leftSize, 0xFFFFFF);
            buf.put((byte) (current & 0xFF));
            buf.put((byte) ((current >> 8) & 0xFF));
            buf.put((byte) ((current >> 16) & 0xFF));
            buf.put((byte) sequenceId++);
            final byte[] bytes = new byte[current];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) i;
            }
            buf.put(bytes);
            buf.flip();
            decoder.consume(buf);
            leftSize -= current;
        }
        final RequestPackage pkg = decoder.poll();
        Assert.assertTrue(pkg.getByteBuffer().remaining() == packageSize);
        int remain = pkg.getByteBuffer().remaining();
        final byte[] out = new byte[remain];
        pkg.getByteBuffer().get(out);
        for (int i = 0; i < out.length; i++) {
            if (out[i] != ((byte) i) % (0xFFFFFF + 1)) {
                break;
            }
            Assert.assertTrue(out[i] == ((byte) i) % 0xFFFFFF);
        }

    }
}
