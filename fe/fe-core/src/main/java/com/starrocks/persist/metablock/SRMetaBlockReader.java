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

<<<<<<< HEAD
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Load object from input stream as the following format.
 * <p>
 * +------------------+
 * |     header       | {"numJson": 10, "name": "AuthenticationManager"}
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
 * Usage see com.starrocks.persist.metablock.SRMetaBlockTest#testSimple()
 */
public class SRMetaBlockReader {
    private static final Logger LOG = LogManager.getLogger(SRMetaBlockReader.class);
    private static final int MAX_JSON_PRINT_LENGTH = 1000;
    private final CheckedInputStream checkedInputStream;
    private SRMetaBlockHeader header;
    private int numJsonRead;
    // For backward compatibility reason
    private final String oldManagerClassName = "com.starrocks.privilege.PrivilegeManager";

    public SRMetaBlockReader(DataInputStream dis) throws IOException {
        this.checkedInputStream = new CheckedInputStream(dis, new CRC32());
        this.header = null;
        this.numJsonRead = 0;

        String s = Text.readStringWithChecksum(checkedInputStream);
        header = GsonUtils.GSON.fromJson(s, SRMetaBlockHeader.class);
    }

    @Deprecated
    public SRMetaBlockReader(DataInputStream dis, String name) throws IOException {
        this.checkedInputStream = new CheckedInputStream(dis, new CRC32());
        this.header = null;
        this.numJsonRead = 0;

        String s = Text.readStringWithChecksum(checkedInputStream);
        header = GsonUtils.GSON.fromJson(s, SRMetaBlockHeader.class);
    }

    public SRMetaBlockHeader getHeader() {
        return header;
    }

    private String readJsonText() throws IOException, SRMetaBlockEOFException {
        if (numJsonRead >= header.getNumJson()) {
            throw new SRMetaBlockEOFException(String.format(
                    "Read json more than expect: %d >= %d", numJsonRead, header.getNumJson()));
        }
        String s = Text.readStringWithChecksum(checkedInputStream);
        numJsonRead += 1;
        return s;
    }

    public <T> T readJson(Class<T> returnClass) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        return GsonUtils.GSON.fromJson(readJsonText(), returnClass);
    }

    public int readInt() throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        return readJson(int.class);
    }

    public Object readJson(Type returnType) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        return GsonUtils.GSON.fromJson(readJsonText(), returnType);
    }

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
=======
import java.io.IOException;
import java.lang.reflect.Type;

public interface SRMetaBlockReader {

    SRMetaBlockHeader getHeader();

    int readInt() throws IOException, SRMetaBlockEOFException;

    long readLong() throws IOException, SRMetaBlockEOFException;

    byte readByte() throws IOException, SRMetaBlockEOFException;

    short readShort() throws IOException, SRMetaBlockEOFException;

    double readDouble() throws IOException, SRMetaBlockEOFException;

    float readFloat() throws IOException, SRMetaBlockEOFException;

    char readChar() throws IOException, SRMetaBlockEOFException;

    boolean readBoolean() throws IOException, SRMetaBlockEOFException;

    String readString() throws IOException, SRMetaBlockEOFException;

    <T> T readJson(Type returnType) throws IOException, SRMetaBlockEOFException;

    <T> T readJson(Class<T> classType) throws IOException, SRMetaBlockEOFException;

    <T> void readCollection(Class<T> classType, CollectionConsumer<? super T> action) throws IOException, SRMetaBlockEOFException;

    <K, V> void readMap(Type keyType, Type valueType, MapEntryConsumer<? super K, ? super V> action)
            throws IOException, SRMetaBlockEOFException;

    void close() throws IOException, SRMetaBlockException;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}