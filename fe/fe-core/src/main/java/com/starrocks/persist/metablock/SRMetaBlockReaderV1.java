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

package com.starrocks.persist.metablock;

import com.google.gson.stream.JsonReader;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Load object from input stream as the following format.
 * <p>
 * +------------------+
 * |     header       | {"numJson": 10, "id": "1"}
 * +------------------+
 * |     Json 1       |
 * +------------------+
 * |     Json 2       |
 * +------------------+
 * |      ...         |
 * +------------------+
 * |     Json 10      |
 * +------------------+
 * |      footer      | {"checksum": xxx}
 * +------------------+
 * <p>
 *
 * the storage format of one json object is length:bytes,
 * `length` is a 4-byte int that stores the following byte size,
 * and `bytes` is the json str UTF8 encoded bytes.
 *
 * Usage see com.starrocks.persist.metablock.SRMetaBlockTest#testSimple()
 */
public class SRMetaBlockReaderV1 implements SRMetaBlockReader {
    private static final Logger LOG = LogManager.getLogger(SRMetaBlockReaderV1.class);
    private static final int MAX_JSON_PRINT_LENGTH = 1000;
    private final CheckedInputStream checkedInputStream;
    private SRMetaBlockHeader header;
    private int numJsonRead;

    public SRMetaBlockReaderV1(InputStream in) throws IOException {
        this.checkedInputStream = new CheckedInputStream(in, new CRC32());
        this.header = null;
        this.numJsonRead = 0;

        String s = Text.readStringWithChecksum(checkedInputStream);
        header = GsonUtils.GSON.fromJson(s, SRMetaBlockHeader.class);
    }

    @Override
    public SRMetaBlockHeader getHeader() {
        return header;
    }

    private byte[] readJsonBytes() throws IOException, SRMetaBlockEOFException {
        if (numJsonRead >= header.getNumJson()) {
            throw new SRMetaBlockEOFException(String.format(
                    "Read json more than expect: %d >= %d", numJsonRead, header.getNumJson()));
        }
        byte[] bytes = new byte[4];
        readAndCheckEof(checkedInputStream, bytes, 4);
        int length = ByteBuffer.wrap(bytes).getInt();
        bytes = new byte[length];
        readAndCheckEof(checkedInputStream, bytes, length);
        numJsonRead += 1;
        return bytes;
    }

    private void readAndCheckEof(CheckedInputStream in, byte[] bytes, int expectLength) throws IOException {
        int readRet = in.read(bytes, 0, expectLength);
        if (readRet != expectLength) {
            throw new EOFException(String.format("reach EOF: read expect %d actual %d!", expectLength, readRet));
        }
    }

    @Override
    public <T> T readJson(Class<T> returnClass) throws IOException, SRMetaBlockEOFException {
        byte[] bytes = readJsonBytes();
        try (JsonReader jsonReader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytes),
                StandardCharsets.UTF_8))) {
            return GsonUtils.GSON.fromJson(jsonReader, returnClass);
        }
    }

    @Override
    public int readInt() throws IOException, SRMetaBlockEOFException {
        return readJson(int.class);
    }

    @Override
    public long readLong() throws IOException, SRMetaBlockEOFException {
        return readJson(long.class);
    }

    @Override
    public Object readJson(Type returnType) throws IOException, SRMetaBlockEOFException {
        byte[] bytes = readJsonBytes();
        try (JsonReader jsonReader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytes),
                StandardCharsets.UTF_8))) {
            return GsonUtils.GSON.fromJson(jsonReader, returnType);
        }
    }

    @Override
    public void close() throws IOException, SRMetaBlockException {
        if (header == null) {
            LOG.warn("do nothing and quit.");
            return;
        }
        if (numJsonRead < header.getNumJson()) {
            // discard the rest of data for compatibility
            // normally it's because this FE has just rollback from a higher version that would produce more metadata
            int rest = header.getNumJson() - numJsonRead;
            LOG.warn("Meta block for {} read {} json < total {} json, will skip the rest {} json",
                    header.getSrMetaBlockID(), numJsonRead, header.getNumJson(), rest);
            for (int i = 0; i != rest; ++i) {
                String text = Text.readStringWithChecksum(checkedInputStream);
                LOG.warn("skip {}th json: {}", i,
                        text.substring(0, Math.min(MAX_JSON_PRINT_LENGTH, text.length())));
            }
        }

        // 1. calculate checksum
        // 2. read footer
        // 3. compare checksum
        // step 1 must before step 2 as the writer goes
        long checksum = checkedInputStream.getChecksum().getValue();
        String s = Text.readStringWithChecksum(checkedInputStream);
        SRMetaBlockFooter footer = GsonUtils.GSON.fromJson(s, SRMetaBlockFooter.class);
        if (checksum != footer.getChecksum()) {
            throw new SRMetaBlockException(String.format(
                    "Invalid meta block, checksum mismatch! expect %d actual %d", footer.getChecksum(), checksum));
        }
    }
}
