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

import com.google.common.reflect.TypeToken;
import com.google.gson.JsonSyntaxException;
import com.google.gson.internal.Primitives;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
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
    public int readInt() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Integer> object = readJson(new TypeToken<PrimitiveObject<Integer>>() {}.getType());
        return object.getValue();
    }

    @Override
    public long readLong() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Long> object = readJson(new TypeToken<PrimitiveObject<Long>>() {}.getType());
        return object.getValue();
    }

    @Override
    public byte readByte() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Byte> object = readJson(new TypeToken<PrimitiveObject<Byte>>() {}.getType());
        return object.getValue();
    }

    @Override
    public short readShort() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Short> object = readJson(new TypeToken<PrimitiveObject<Short>>() {}.getType());
        return object.getValue();
    }

    @Override
    public double readDouble() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Double> object = readJson(new TypeToken<PrimitiveObject<Double>>() {}.getType());
        return object.getValue();
    }

    @Override
    public float readFloat() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Float> object = readJson(new TypeToken<PrimitiveObject<Float>>() {}.getType());
        return object.getValue();
    }

    @Override
    public char readChar() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Character> object = readJson(new TypeToken<PrimitiveObject<Character>>() {}.getType());
        return object.getValue();
    }

    @Override
    public boolean readBoolean() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<Boolean> object = readJson(new TypeToken<PrimitiveObject<Boolean>>() {}.getType());
        return object.getValue();
    }

    @Override
    public String readString() throws IOException, SRMetaBlockEOFException {
        PrimitiveObject<String> object = readJson(new TypeToken<PrimitiveObject<String>>() {}.getType());
        return object.getValue();
    }

    @Override
    public <T> T readJson(Type returnType) throws IOException, SRMetaBlockEOFException {
        checkEOF();
        try {
            T t = GsonUtils.GSON.fromJson(jsonReader, returnType);
            numJsonRead++;
            return t;
        } catch (JsonSyntaxException e) {
            handleJsonSyntaxException(e);
        }
        return null;
    }

    @Override
    public <T> T readJson(Class<T> classOfT) throws IOException, SRMetaBlockEOFException {
        Object object = readJson((Type) classOfT);
        return Primitives.wrap(classOfT).cast(object);
    }

    private void checkEOF() throws SRMetaBlockEOFException {
        if (numJsonRead >= header.getNumJson()) {
            throw new SRMetaBlockEOFException(String.format(
                    "Read json more than expect: %d >= %d", numJsonRead, header.getNumJson()));
        }
    }

    @Override
    public <T> void readCollection(Class<T> classType, CollectionConsumer<? super T> action)
            throws IOException, SRMetaBlockEOFException {
        int size = readInt();
        while (size-- > 0) {
            T t = null;
            try {
                t = GsonUtils.GSON.fromJson(jsonReader, classType);
                numJsonRead++;
                action.accept(t);
            } catch (SubtypeNotFoundException e) {
                LOG.info("ignore_unknown_subtype is {}", Config.metadata_ignore_unknown_subtype);
                if (Config.metadata_ignore_unknown_subtype) {
                    LOG.warn("ignore unknown sub type: {}", e.getSubtype(), e);
                    numJsonRead++;
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public <K, V> void readMap(Type keyType, Type valueType,
                               MapEntryConsumer<? super K, ? super V> action)
            throws IOException, SRMetaBlockEOFException {
        int size = readInt();
        while (size-- > 0) {
            K k = null;
            boolean ignoreUnknownType = false;
            try {
                k = readMapElement(keyType);
                numJsonRead++;
            } catch (SubtypeNotFoundException e) {
                if (Config.metadata_ignore_unknown_subtype) {
                    LOG.warn("ignore unknown sub type: {}", e.getSubtype(), e);
                    numJsonRead++;
                    ignoreUnknownType = true;
                } else {
                    throw e;
                }
            }
            V v = null;
            try {
                v = readMapElement(valueType);
                numJsonRead++;
            } catch (SubtypeNotFoundException e) {
                if (Config.metadata_ignore_unknown_subtype) {
                    LOG.warn("ignore unknown sub type: {}", e.getSubtype(), e);
                    numJsonRead++;
                    ignoreUnknownType = true;
                } else {
                    throw e;
                }
            }

            if (!ignoreUnknownType) {
                action.accept(k, v);
            }
        }
    }

    private <T> T readMapElement(Type typeOfT) {
        if (typeOfT == byte.class || typeOfT == Byte.class) {
            PrimitiveObject<Byte> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Byte>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == short.class || typeOfT == Short.class) {
            PrimitiveObject<Short> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Short>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == int.class || typeOfT == Integer.class) {
            PrimitiveObject<Integer> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Integer>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == Long.class || typeOfT == long.class) {
            PrimitiveObject<Long> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Long>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == Double.class || typeOfT == double.class) {
            PrimitiveObject<Double> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Double>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == Float.class || typeOfT == float.class) {
            PrimitiveObject<Float> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Float>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == Character.class || typeOfT == char.class) {
            PrimitiveObject<Character> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Character>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == Boolean.class || typeOfT == boolean.class) {
            PrimitiveObject<Boolean> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<Boolean>>() {}.getType());
            return (T) object.getValue();
        }
        if (typeOfT == String.class) {
            PrimitiveObject<String> object = GsonUtils.GSON.fromJson(jsonReader,
                    new TypeToken<PrimitiveObject<String>>() {}.getType());
            return (T) object.getValue();
        }
        return GsonUtils.GSON.fromJson(jsonReader, typeOfT);
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
