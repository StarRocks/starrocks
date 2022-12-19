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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/io/Text.java

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

package com.starrocks.common.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * This class stores text using standard UTF8 encoding. It provides methods to
 * serialize, deserialize, and compare texts at byte level.
 * <p>
 * In addition, it provides methods for string traversal without converting the
 * byte array to a string.
 * <p>
 * Also includes utilities for serializing/deserialing a string, coding/decoding
 * a string, checking if a byte array contains valid UTF8 code, calculating the
 * length of an encoded string.
 */
public class Text implements Writable {

    private static final Logger LOG = LogManager.getLogger(Text.class);
    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] bytes;
    private int length;

    public Text() {
        bytes = EMPTY_BYTES;
    }

    public Text(String string) {
        set(string);
    }

    public Text(Text utf8) {
        set(utf8);
    }

    public Text(byte[] utf8) {
        set(utf8);
    }

    // Returns the raw bytes; however, only data up to getLength() is valid.
    public byte[] getBytes() {
        return bytes;
    }

    // Returns the number of bytes in the byte array
    public int getLength() {
        return length;
    }

    public void setLength(int len) {
        if (len < 0) {
            return;
        }
        if (this.length < len) {
            setCapacity(len, true);
        }
        this.length = len;
    }

    /**
     * Returns the Unicode Scalar Value (32-bit integer value) for the character
     * at <code>position</code>. Note that this method avoids using the
     * converter or doing String instatiation
     *
     * @return the Unicode scalar value at position or -1 if the position is
     * invalid or points to a trailing byte
     */
    public int charAt(int position) {
        if (position > this.length) {
            return -1;
        }
        if (position < 0) {
            return -1;
        }

        ByteBuffer bb = (ByteBuffer) ByteBuffer.wrap(bytes).position(position);
        return bytesToCodePoint(bb.slice());
    }

    public int find(String what) {
        return find(what, 0);
    }

    /**
     * Finds any occurence of <code>what</code> in the backing buffer, starting
     * as position <code>start</code>. The starting position is measured in
     * bytes and the return value is in terms of byte position in the buffer.
     * The backing buffer is not converted to a string for this operation.
     *
     * @return byte position of the first occurence of the search string in the
     * UTF-8 buffer or -1 if not found
     */
    public int find(String what, int start) {
        try {
            ByteBuffer src = ByteBuffer.wrap(this.bytes, 0, this.length);
            ByteBuffer tgt = encode(what);
            byte b = tgt.get();
            src.position(start);

            while (src.hasRemaining()) {
                if (b == src.get()) { // matching first byte
                    src.mark(); // save position in loop
                    tgt.mark(); // save position in target
                    boolean found = true;
                    int pos = src.position() - 1;
                    while (tgt.hasRemaining()) {
                        if (!src.hasRemaining()) { // src expired first
                            tgt.reset();
                            src.reset();
                            found = false;
                            break;
                        }
                        if (!(tgt.get() == src.get())) {
                            tgt.reset();
                            src.reset();
                            found = false;
                            break; // no match
                        }
                    }
                    if (found) {
                        return pos;
                    }
                }
            }
            return -1; // not found
        } catch (CharacterCodingException e) {
            // can't get here
            LOG.warn(e);
            return -1;
        }
    }

    // Set to contain the contents of a string.
    public void set(String string) {
        try {
            ByteBuffer bb = encode(string, true);
            bytes = bb.array();
            length = bb.limit();
        } catch (CharacterCodingException e) {
            throw new RuntimeException("Should not have happened " + e);
        }
    }

    // Set to a utf8 byte array
    public void set(byte[] utf8) {
        set(utf8, 0, utf8.length);
    }

    // Copy a text.
    public void set(Text other) {
        set(other.getBytes(), 0, other.getLength());
    }

    /**
     * Set the Text to range of bytes
     *
     * @param utf8  the data to copy from
     * @param start the first position of the new string
     * @param len   the number of bytes of the new string
     */
    public void set(byte[] utf8, int start, int len) {
        setCapacity(len, false);
        System.arraycopy(utf8, start, bytes, 0, len);
        this.length = len;
    }

    /**
     * Append a range of bytes to the end of the given text
     *
     * @param utf8  the data to copy from
     * @param start the first position to append from utf8
     * @param len   the number of bytes to append
     */
    public void append(byte[] utf8, int start, int len) {
        setCapacity(length + len, true);
        System.arraycopy(utf8, start, bytes, length, len);
        length += len;
    }

    // Clear the string to empty.
    public void clear() {
        length = 0;
    }

    /*
     * Sets the capacity of this Text object to <em>at least</em>
     * <code>len</code> bytes. If the current buffer is longer, then the
     * capacity and existing content of the buffer are unchanged. If
     * <code>len</code> is larger than the current capacity, the Text object's
     * capacity is increased to match.
     *
     * @param len the number of bytes we need
     *
     * @param keepData should the old data be kept
     */
    public void setCapacity(int len, boolean keepData) {
        if (bytes == null || bytes.length < len) {
            byte[] newBytes = new byte[len];
            if (bytes != null && keepData) {
                System.arraycopy(bytes, 0, newBytes, 0, length);
            }
            bytes = newBytes;
        }
    }

    /**
     * Convert text back to string
     *
     * @see java.lang.Object#toString()
     */
    public String toString() {
        try {
            return decode(bytes, 0, length);
        } catch (CharacterCodingException e) {
            throw new RuntimeException("Should not have happened " + e);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int newLength = in.readInt();
        setCapacity(newLength, false);
        in.readFully(bytes, 0, newLength);
        length = newLength;
    }

    // Skips over one Text in the input.
    public static void skip(DataInput in) throws IOException {
        int length = in.readInt();
        skipFully(in, length);
    }

    public static void skipFully(DataInput in, int len) throws IOException {
        int total = 0;
        int cur;

        while ((total < len) && ((cur = in.skipBytes(len - total)) > 0)) {
            total += cur;
        }

        if (total < len) {
            throw new IOException("Not able to skip " + len
                    + " bytes, possibly " + "due to end of input.");
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(length);
        out.write(bytes, 0, length);
    }

    public boolean equals(Object o) {
        if (o instanceof Text) {
            return super.equals(o);
        }
        return false;
    }

    public int hashCode() {
        return super.hashCode();
    }

    public static String decode(byte[] utf8) throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8), true);
    }

    public static String decode(byte[] utf8, int start, int length)
            throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8, start, length), true);
    }

    /**
     * Converts the provided byte array to a String using the UTF-8 encoding. If
     * <code>replace</code> is true, then malformed input is replaced with the
     * substitution character, which is U+FFFD. Otherwise the method throws a
     * MalformedInputException.
     */
    private static String decode(ByteBuffer utf8, boolean replace)
            throws CharacterCodingException {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        if (replace) {
            decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        String str = decoder.decode(utf8).toString();
        // set decoder back to its default value: REPORT
        if (replace) {
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return str;
    }

    /**
     * Converts the provided String to bytes using the UTF-8 encoding. If the
     * input is malformed, invalid chars are replaced by a default value.
     *
     * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
     * ByteBuffer.limit()
     */

    public static ByteBuffer encode(String string)
            throws CharacterCodingException {
        return encode(string, true);
    }

    /**
     * Converts the provided String to bytes using the UTF-8 encoding. If
     * <code>replace</code> is true, then malformed input is replaced with the
     * substitution character, which is U+FFFD. Otherwise the method throws a
     * MalformedInputException.
     *
     * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
     * ByteBuffer.limit()
     */
    public static ByteBuffer encode(String string, boolean replace)
            throws CharacterCodingException {
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes = encoder
                .encode(CharBuffer.wrap(string.toCharArray()));
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPORT);
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return bytes;
    }

    /**
     * Read a UTF8 encoded string from in
     */
    public static String readString(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        return decode(bytes);
    }

    public static byte[] readBinary(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    /**
     * Write a UTF8 encoded string to out
     */
    public static int writeString(DataOutput out, String s) throws IOException {
        ByteBuffer bytes = encode(s);
        int length = bytes.limit();
        out.writeInt(length);
        out.write(bytes.array(), 0, length);
        return length;
    }

    public static int writeBinary(DataOutput out, byte[] bytes) throws IOException {
        int length = bytes.length;
        out.writeInt(length);
        out.write(bytes, 0, length);
        return length;
    }

    /**
     * Same as writeString(), but to a CheckedOutputStream
     */
    public static int writeStringWithChecksum(CheckedOutputStream cos, String s) throws IOException {
        ByteBuffer byteBuffer = encode(s);
        int length = byteBuffer.limit();
        byte[] bytes = ByteBuffer.allocate(4).putInt(length).array();
        cos.write(bytes);
        cos.write(byteBuffer.array(), 0, length);
        return length;
    }

    private static void readAndCheckEof(CheckedInputStream in, byte[] bytes, int expectLength) throws IOException {
        int readRet = in.read(bytes, 0, expectLength);
        if (readRet != expectLength) {
            throw new EOFException(String.format("reach EOF: read expect %d actual %d!", expectLength, readRet));
        }
    }

    /**
     * Same as readString(), but to a CheckedInputStream
     */
    public static String readStringWithChecksum(CheckedInputStream in) throws IOException {
        byte[] bytes = new byte[4];
        readAndCheckEof(in, bytes, 4);
        int length = ByteBuffer.wrap(bytes).getInt();
        bytes = new byte[length];
        readAndCheckEof(in, bytes, length);
        return decode(bytes);
    }

    /**
     * Magic numbers for UTF-8. These are the number of bytes that
     * <em>follow</em> a given lead byte. Trailing bytes have the value -1. The
     * values 4 and 5 are presented in this table, even though valid UTF-8
     * cannot include the five and six byte sequences.
     */
    static final int[] BYTES_FROM_UTF_8 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0,
            0,
            0,
            0,
            0,
            0,
            // trail bytes
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
            3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5};

    /**
     * Returns the next code point at the current position in the buffer. The
     * buffer's position will be incremented. Any mark set on this buffer will
     * be changed by this method!
     */
    public static int bytesToCodePoint(ByteBuffer bytes) {
        bytes.mark();
        byte b = bytes.get();
        bytes.reset();
        int extraBytesToRead = BYTES_FROM_UTF_8[(b & 0xFF)];
        if (extraBytesToRead < 0) {
            return -1; // trailing byte!
        }
        int ch = 0;

        switch (extraBytesToRead) {
            case 5:
                ch += (bytes.get() & 0xFF);
                ch <<= 6; /* remember, illegal UTF-8 */
            case 4:
                ch += (bytes.get() & 0xFF);
                ch <<= 6; /* remember, illegal UTF-8 */
            case 3:
                ch += (bytes.get() & 0xFF);
                ch <<= 6;
            case 2:
                ch += (bytes.get() & 0xFF);
                ch <<= 6;
            case 1:
                ch += (bytes.get() & 0xFF);
                ch <<= 6;
            case 0:
                ch += (bytes.get() & 0xFF);
        }
        ch -= OFFSETS_FROM_UTF_8[extraBytesToRead];

        return ch;
    }

    static final int[] OFFSETS_FROM_UTF_8 = {0x00000000, 0x00003080, 0x000E2080,
            0x03C82080, 0xFA082080, 0x82082080};
}
