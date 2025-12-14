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

package com.staros.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class ByteWriterTest {
    public class MyTestWriter implements Writable {
        private byte[] data;
        public MyTestWriter(byte[] data) {
            this.data = data;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.write(data);
        }
    }

    @Test
    public void testByteWriter() throws IOException {
        { // writer write
            ByteWriter w = new ByteWriter();
            byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
            MyTestWriter wd = new MyTestWriter(data);
            w.write(wd);
            byte[] data2 = w.getData();
            Assert.assertArrayEquals(data, data2);
        }

        { // writeX interface
            ByteWriter w = new ByteWriter();
            int a = 10;
            long b = 30;
            byte[] data;
            w.writeI(10); // 4 Bytes
            data = w.getData();
            Assert.assertEquals(data.length, 4);

            w.writeL(30); // 8 Bytes
            data = w.getData();
            Assert.assertEquals(data.length, 4 + 8);

            byte[] c = "abcdefgh".getBytes(); // 4 + 8 Bytes, with encoded length
            w.writeB(c);
            byte[] bytes = w.getData();
            Assert.assertEquals(bytes.length, 4 + 8 + 4 + 8);

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int aa = in.readInt();
            Assert.assertEquals(aa, a);
            long bb = in.readLong();
            Assert.assertEquals(bb, b);

            // Encoded length
            int len = in.readInt();
            Assert.assertEquals(len, 8);
            byte[] cc = new byte[in.available()];
            in.readFully(cc);
            Assert.assertArrayEquals(cc, c);
        }

        { // write a large data longer than default buffer size
            int len = 8192;
            byte[] data = new byte[len];
            ThreadLocalRandom.current().nextBytes(data);
            ByteWriter w = new ByteWriter();
            w.writeB(data);
            byte[] data2 = w.getData();
            Assert.assertEquals(data2.length, data.length + 4); // with additional 4 bytes for int length
            Assert.assertArrayEquals(Arrays.copyOfRange(data2, 4, data2.length), data);
        }
    }
}
