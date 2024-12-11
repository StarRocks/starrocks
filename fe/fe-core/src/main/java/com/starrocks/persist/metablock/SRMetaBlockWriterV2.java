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

import com.google.gson.stream.JsonWriter;
import com.starrocks.persist.gson.GsonUtils;

import java.io.IOException;

public class SRMetaBlockWriterV2 implements SRMetaBlockWriter {
    private final SRMetaBlockHeader header;
    private final JsonWriter jsonWriter;
    private int numJsonWritten;

    public SRMetaBlockWriterV2(JsonWriter jsonWriter, SRMetaBlockID id, int numJson) throws SRMetaBlockException {
        if (numJson <= 0) {
            throw new SRMetaBlockException(String.format("invalid numJson: %d", numJson));
        }
        this.header = new SRMetaBlockHeader(id, numJson);
        this.jsonWriter = jsonWriter;
        this.numJsonWritten = 0;
    }

    @Override
    public void writeJson(Object object) throws IOException, SRMetaBlockException {
        if (isBasicType(object)) {
            throw new SRMetaBlockException("can not write primitive type");
        }

        // always check if write more than expect
        if (numJsonWritten >= header.getNumJson()) {
            throw new SRMetaBlockException(String.format(
                    "About to write json more than expect %d, actual %d", header.getNumJson(), numJsonWritten));
        }
        if (numJsonWritten == 0) {
            // write header
            GsonUtils.GSON.toJson(header, header.getClass(), jsonWriter);
        }
        GsonUtils.GSON.toJson(object, object.getClass(), jsonWriter);
        numJsonWritten++;
    }

    @Override
    public void writeInt(int value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeLong(long value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeByte(byte value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeShort(short value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeDouble(double value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeFloat(float value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeChar(char value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeBoolean(boolean value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void writeString(String value) throws IOException, SRMetaBlockException {
        writeJson(new PrimitiveObject<>(value));
    }

    @Override
    public void close() throws IOException, SRMetaBlockException {
        // check if write as many json string as expect
        if (numJsonWritten != header.getNumJson()) {
            throw new SRMetaBlockException(String.format(
                    "Block json number mismatch: expect %d actual %d", header.getNumJson(), numJsonWritten));
        }
        jsonWriter.flush();
    }

    private boolean isBasicType(Object object) {
        return object instanceof Byte || object instanceof Short || object instanceof Integer || object instanceof Long
                || object instanceof Double || object instanceof Float
                || object instanceof String || object instanceof Character
                || object instanceof Boolean;
    }
}
