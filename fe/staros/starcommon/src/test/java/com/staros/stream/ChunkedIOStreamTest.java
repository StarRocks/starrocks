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

package com.staros.stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ChunkedIOStreamTest {
    private static final Logger LOG = LogManager.getLogger(ChunkedIOStreamTest.class);

    @Test
    public void testSingleChunkedStreamWriteAndRead() throws IOException {
        boolean expectedBool = true;
        short expectedShort = 255;
        int expectedInt = 10;
        long expectedLong = 100;
        double expectedDouble = 3.14;
        byte[] expectedBytes = "Java World".getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream baseStream = new ByteArrayOutputStream();
        {
            ChunkedOutputStream chunkedStream = new ChunkedOutputStream(baseStream);
            DataOutputStream dataStream = new DataOutputStream(chunkedStream);
            dataStream.writeBoolean(expectedBool);
            dataStream.writeShort(expectedShort);
            dataStream.writeInt(expectedInt);
            dataStream.writeLong(expectedLong);
            dataStream.writeDouble(expectedDouble);
            // write byte array's length first
            dataStream.writeInt(expectedBytes.length);
            dataStream.write(expectedBytes, 0, expectedBytes.length);
            dataStream.close();
        }
        byte[] finalBytes = baseStream.toByteArray();

        ByteArrayOutputStream compBaseStream = new ByteArrayOutputStream();
        {
            DataOutputStream compareStream = new DataOutputStream(compBaseStream);
            compareStream.writeBoolean(expectedBool);
            compareStream.writeShort(expectedShort);
            compareStream.writeInt(expectedInt);
            compareStream.writeLong(expectedLong);
            compareStream.writeDouble(expectedDouble);
            compareStream.writeInt(expectedBytes.length);
            compareStream.write(expectedBytes, 0, expectedBytes.length);
            compareStream.close();
        }
        byte[] cmpBytes = compBaseStream.toByteArray();

        Assert.assertNotEquals(finalBytes.length, cmpBytes.length);
        Assert.assertTrue(finalBytes.length > cmpBytes.length);

        { // use the finalBytes to construct a DataInputStream
            DataInputStream stream = new DataInputStream(new ChunkedInputStream(new ByteArrayInputStream(finalBytes)));
            Assert.assertEquals(expectedBool, stream.readBoolean());
            Assert.assertEquals(expectedShort, stream.readShort());
            Assert.assertEquals(expectedInt, stream.readInt());
            Assert.assertEquals(expectedLong, stream.readLong());
            // 3.14
            Assert.assertEquals(expectedDouble, stream.readDouble(), 0.001);
            int length = stream.readInt();
            Assert.assertEquals(expectedBytes.length, length);
            byte[] readBytes = new byte[length];
            stream.readFully(readBytes);
            Assert.assertArrayEquals(expectedBytes, readBytes);
            // reach EOF
            Assert.assertEquals(-1, stream.read());
            // still EOF
            Assert.assertEquals(-1, stream.read());
        }
    }

    @Test
    public void testChunkedIOStreamMultiplex() throws IOException {
        // test multiple streams on the same underlying ByteArrayStream
        // random string with 100~200 chars
        String data1 = RandomStringUtils.randomAlphabetic(10, 20);
        // random string with 200~300 chars
        String data2 = RandomStringUtils.randomAlphabetic(20, 30);
        // random string with 50~100 chars
        String data3 = RandomStringUtils.randomAlphabetic(5, 10);

        ByteArrayOutputStream baseOut = new ByteArrayOutputStream();
        try (ChunkedOutputStream stream = new ChunkedOutputStream(baseOut)) {
            // write data1 in a new chunk
            stream.write(data1.getBytes(StandardCharsets.UTF_8));
        }
        try (ChunkedOutputStream stream = new ChunkedOutputStream(baseOut)) {
            // write data2 in a new chunk
            stream.write(data2.getBytes(StandardCharsets.UTF_8));
        }
        try (ChunkedOutputStream stream = new ChunkedOutputStream(baseOut)) {
            // write data3 in a new chunk
            stream.write(data3.getBytes(StandardCharsets.UTF_8));
        }
        byte[] rawBytes = baseOut.toByteArray();
        ByteArrayInputStream inStream = new ByteArrayInputStream(rawBytes);

        try (ChunkedInputStream stream = new ChunkedInputStream(inStream)) {
            // data1 validation
            String myData = readFully(stream);
            Assert.assertEquals(data1, myData);
            // reaches the end
            Assert.assertEquals(-1, stream.read());
        }
        try (ChunkedInputStream stream = new ChunkedInputStream(inStream)) {
            // data2 validation
            String myData = readFully(stream);
            Assert.assertEquals(data2, myData);
            // reaches the end
            Assert.assertEquals(-1, stream.read());
        }
        try (ChunkedInputStream stream = new ChunkedInputStream(inStream)) {
            // data3 validation
            String myData = readFully(stream);
            Assert.assertEquals(data3, myData);
            // reaches the end
            Assert.assertEquals(-1, stream.read());
        }
        // the underlying input stream reaches the end as well
        Assert.assertEquals(-1, inStream.read());
    }

    private String readFully(InputStream in) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        int b;
        while ((b = in.read()) != -1) {
            buffer.put((byte) b);
        }
        buffer.flip();
        return new String(buffer.array(), buffer.position(), buffer.remaining(), StandardCharsets.UTF_8);
    }

    @Test
    public void testChunkedStreamExceptions() throws IOException {
        byte[] data = "testChunkedStreamExceptions".getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream baseStream = new ByteArrayOutputStream();
        { // ChunkedOutputStream exceptions
            try (OutputStream chunkOut = new ChunkedOutputStream(baseStream)) {
                Assert.assertThrows(NullPointerException.class, () -> chunkOut.write(null, 0, 1));
                Assert.assertThrows(IndexOutOfBoundsException.class, () -> chunkOut.write(new byte[5], -1, 10));
                // valid write
                chunkOut.write(data);
                // empty write, doesn't hurt anything
                chunkOut.write(data, 0, 0);
            }
        }

        byte[] rawBytes = baseStream.toByteArray();

        { // validate input
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            InputStream stream = new ChunkedInputStream(baseIn);
            Assert.assertFalse(stream.markSupported());

            // check exceptions
            Assert.assertThrows(NullPointerException.class, () -> stream.read(null, 0, 1));
            Assert.assertThrows(IndexOutOfBoundsException.class, () -> stream.read(new byte[5], 0, 10));

            byte[] copyData = new byte[data.length];
            int n = stream.read(copyData, 0, copyData.length);
            Assert.assertEquals(copyData.length, n);
            Assert.assertArrayEquals(data, copyData);

            Assert.assertEquals(-1, stream.read());
            Assert.assertEquals(-1, stream.read(copyData, 0, 1));
        }

        { // missing one byte at the end, can read data, but can't find end tag
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes, 0, rawBytes.length - 1);
            InputStream stream = new ChunkedInputStream(baseIn);
            byte[] copyData = new byte[data.length];
            int n = stream.read(copyData, 0, copyData.length);
            Assert.assertEquals(copyData.length, n);
            Assert.assertArrayEquals(data, copyData);

            Assert.assertThrows(EOFException.class, stream::read);
            Assert.assertThrows(EOFException.class, () -> stream.read(copyData, 0, 1));
        }
        { // missing 5 bytes at the end, CANNOT even decode one valid chunk from input stream
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes, 0, rawBytes.length - 5);
            InputStream stream = new ChunkedInputStream(baseIn);
            Assert.assertThrows(EOFException.class, stream::read);
        }
    }

    @Test
    public void testChunkedStreamOverhead() throws IOException {

        // normal write to ByteArrayOutputStream with any encoding.
        ByteArrayOutputStream baselineStream = new ByteArrayOutputStream();
        writeTestData(baselineStream);
        baselineStream.close();
        byte [] baselineBytes = baselineStream.toByteArray();

        { // write to chunked encoding stream without buffer
            ByteArrayOutputStream baseStream = new ByteArrayOutputStream();
            OutputStream stream = new ChunkedOutputStream(baseStream);
            writeTestData(stream);
            stream.close();
            baseStream.close();

            byte[] encodedBytes = baseStream.toByteArray();
            // BaselineSize: 43, EncodedSize: 58, Overhead: 34.88%
            logEncodingOverhead(baselineBytes.length, encodedBytes.length);
        }
        { // write to chunked encoding stream with buffer
            ByteArrayOutputStream baseStream = new ByteArrayOutputStream();
            OutputStream stream = new BufferedOutputStream(new ChunkedOutputStream(baseStream));
            writeTestData(stream);
            stream.close();
            baseStream.close();

            byte[] encodedBytes = baseStream.toByteArray();
            // BaselineSize: 43, EncodedSize: 45, Overhead: 4.65%
            logEncodingOverhead(baselineBytes.length, encodedBytes.length);
        }
    }

    private void writeTestData(OutputStream os) throws IOException {
        boolean dataBool = true;
        short dataShort = 255;
        int dataInt = 10;
        long dataLong = 100;
        double dataDouble = 3.14;
        byte[] dataBytes = "Java Hello World".getBytes(StandardCharsets.UTF_8);

        DataOutputStream dataOut = new DataOutputStream(os);
        dataOut.writeBoolean(dataBool);
        dataOut.writeShort(dataShort);
        dataOut.writeInt(dataInt);
        dataOut.writeLong(dataLong);
        dataOut.writeDouble(dataDouble);
        dataOut.writeInt(dataBytes.length);
        dataOut.write(dataBytes, 0, dataBytes.length);
    }

    private void logEncodingOverhead(int baseline, int actual) {
        String overhead = String.format("%.2f%%", 100.0 * (actual - baseline) / baseline);
        LOG.info("BaselineSize: {}, EncodedSize: {}, Overhead: {}", baseline, actual, overhead);
    }
}
