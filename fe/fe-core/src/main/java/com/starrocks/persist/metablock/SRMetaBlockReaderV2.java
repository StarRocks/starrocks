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

import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Type;

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
 *
 * The format of json is json str, starting with "{" and ending with "}
 *
 */
public class SRMetaBlockReaderV2 implements SRMetaBlockReader {
    private static final Logger LOG = LogManager.getLogger(SRMetaBlockReaderV1.class);

    private final JsonReader jsonReader;
    private int numJsonRead;
    private SRMetaBlockHeader header;

    public SRMetaBlockReaderV2(JsonReader jsonReader) throws IOException {
        this.numJsonRead = 0;
        this.jsonReader = jsonReader;
        try {
            header = GsonUtils.GSON.fromJson(jsonReader, SRMetaBlockHeader.class);
        } catch (JsonSyntaxException e) {
            handleJsonSyntaxException(e);
        }
    }

    @Override
    public SRMetaBlockHeader getHeader() {
        return header;
    }

    @Override
    public <T> T readJson(Class<T> returnClass) throws IOException, SRMetaBlockEOFException {
        checkEOF();
        try {
            T t = GsonUtils.GSON.fromJson(jsonReader, returnClass);
            numJsonRead++;
            return t;
        } catch (JsonSyntaxException e) {
            handleJsonSyntaxException(e);
        }
        return null;
    }

    @Override
    public int readInt() throws IOException, SRMetaBlockEOFException {
        return readJson(IntObject.class).getValue();
    }

    @Override
    public long readLong() throws IOException, SRMetaBlockEOFException {
        return readJson(LongObject.class).getValue();
    }

    @Override
    public Object readJson(Type returnType) throws IOException, SRMetaBlockEOFException {
        checkEOF();
        try {
            Object obj = GsonUtils.GSON.fromJson(jsonReader, returnType);
            numJsonRead++;
            return obj;
        } catch (JsonSyntaxException e) {
            handleJsonSyntaxException(e);
        }
        return null;
    }

    private void checkEOF() throws SRMetaBlockEOFException {
        if (numJsonRead >= header.getNumJson()) {
            throw new SRMetaBlockEOFException(String.format(
                    "Read json more than expect: %d >= %d", numJsonRead, header.getNumJson()));
        }
    }


    @Override
    public void close() throws IOException {
        if (numJsonRead < header.getNumJson()) {
            // discard the rest of data for compatibility
            // normally it's because this FE has just rollback from a higher version that would produce more metadata
            int rest = header.getNumJson() - numJsonRead;
            LOG.warn("Meta block for {} read {} json < total {} json, will skip the rest {} json",
                    header.getSrMetaBlockID(), numJsonRead, header.getNumJson(), rest);
            jsonReader.setLenient(true);
            for (int i = 0; i != rest; ++i) {
                jsonReader.skipValue();
                LOG.warn("skip {}th json", i);
            }
        }
    }

    private void handleJsonSyntaxException(JsonSyntaxException e) throws IOException, JsonSyntaxException {
        if (jsonReader.peek() == JsonToken.END_DOCUMENT) {
            throw new EOFException(e.getMessage());
        } else {
            throw e;
        }
    }
}
