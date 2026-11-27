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

package com.starrocks.catalog;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.JsonAdapter;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/*
 * StringVariant is for type CHAR, VARCHAR, BINARY, VARBINARY and HLL.
 * It holds the exact bytes of the value: the BE stores raw bytes, so a value may not be valid UTF-8.
 */
@JsonAdapter(StringVariant.GsonAdapter.class)
public class StringVariant extends Variant {

    private final byte[] bytes;

    public StringVariant(Type type, byte[] bytes) {
        this(type, bytes, false);
    }

    public StringVariant(Type type, String value) {
        this(type, value.getBytes(StandardCharsets.UTF_8), true);
    }

    // owned: the caller just created the array and keeps no reference, so it is used as is unless the CHAR rule cuts it.
    private StringVariant(Type type, byte[] bytes, boolean owned) {
        super(type);
        int length = valueLength(type, bytes);
        this.bytes = owned && length == bytes.length ? bytes : Arrays.copyOf(bytes, length);
    }

    // CHAR is defined only up to its first NUL in StarRocks: the BE strnlen-truncates a CHAR value on
    // every storage read and when parsing a routing boundary, so the bytes at/after the first 0x00 are
    // not part of the value. Canonicalize a CHAR variant the same way, so FE ordering and pre-split
    // boundaries match what the BE stores and routes. VARCHAR/BINARY/VARBINARY/HLL keep every byte.
    private static int valueLength(Type type, byte[] bytes) {
        if (type != null && type.getPrimitiveType() == PrimitiveType.CHAR) {
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == 0) {
                    return i;
                }
            }
        }
        return bytes.length;
    }

    @Override
    public byte[] getBytesValue() {
        return bytes.clone();
    }

    // Lossy for bytes that are not valid UTF-8: for display, messages and text conversions only.
    @Override
    public String getStringValue() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public long getLongValue() {
        return IntVariant.parseLong(getStringValue());
    }

    @Override
    protected int compareToImpl(Variant other) {
        // compare as unsigned bytes, same with DM,BE,StorageEngine
        byte[] otherBytes = other instanceof StringVariant ? ((StringVariant) other).bytes : other.getBytesValue();
        return Arrays.compareUnsigned(bytes, otherBytes);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof StringVariant)) {
            return false;
        }
        return Arrays.equals(bytes, ((StringVariant) object).bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    // Persisted as {"value": <text or {"bytes": "<base64>"}>, "type": <Type>}. A value that is valid UTF-8 is
    // written as a JSON string, exactly as before bytes were kept; any other value is written as an object, which a
    // reader expecting a string refuses. The two shapes are told apart by JSON type only.
    static class GsonAdapter implements JsonSerializer<StringVariant>, JsonDeserializer<StringVariant> {
        private static final String TYPE = "type";
        private static final String VALUE = "value";
        private static final String BYTES = "bytes";

        @Override
        public JsonElement serialize(StringVariant src, java.lang.reflect.Type typeOfSrc,
                                     JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            try {
                object.addProperty(VALUE, StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(src.bytes)).toString());
            } catch (CharacterCodingException e) {
                JsonObject encoded = new JsonObject();
                encoded.addProperty(BYTES, Base64.getEncoder().encodeToString(src.bytes));
                object.add(VALUE, encoded);
            }
            if (src.getType() != null) {
                object.add(TYPE, context.serialize(src.getType()));
            }
            return object;
        }

        @Override
        public StringVariant deserialize(JsonElement json, java.lang.reflect.Type typeOfT,
                                         JsonDeserializationContext context) throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            // The whole object is read first: "value" comes before "type", and the CHAR rule needs the type.
            JsonElement typeElement = object.get(TYPE);
            Type type = typeElement == null || typeElement.isJsonNull()
                    ? null : context.deserialize(typeElement, Type.class);
            JsonElement value = object.get(VALUE);
            if (value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
                return new StringVariant(type, value.getAsString());
            }
            if (value != null && value.isJsonObject()) {
                JsonElement bytes = value.getAsJsonObject().get(BYTES);
                if (bytes != null && bytes.isJsonPrimitive() && bytes.getAsJsonPrimitive().isString()) {
                    byte[] decoded;
                    try {
                        decoded = Base64.getDecoder().decode(bytes.getAsString());
                    } catch (IllegalArgumentException e) {
                        throw new JsonParseException("StringVariant bytes are not base64: " + json, e);
                    }
                    return new StringVariant(type, decoded, true);
                }
            }
            throw new JsonParseException("StringVariant record has no readable value: " + json);
        }
    }
}
