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
package com.starrocks.encryption;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.starrocks.proto.EncryptionKeyPB;

import java.lang.reflect.Type;
import java.util.Base64;

public class EncryptionKeyPBAdapter
        implements JsonSerializer<EncryptionKeyPB>, JsonDeserializer<EncryptionKeyPB> {

    // Define constants for repeated strings
    public static final String ID = "id";
    public static final String PARENT_ID = "parent_id";
    public static final String CREATE_TIME = "create_time";
    public static final String TYPE = "type";
    public static final String ALGORITHM = "algorithm";
    public static final String ENCRYPTED_KEY = "encrypted_key";
    public static final String PLAIN_KEY = "plain_key";

    @Override
    public JsonElement serialize(EncryptionKeyPB encryptionKeyPB, Type type,
                                 JsonSerializationContext jsonSerializationContext) {
        JsonObject jsonObject = new JsonObject();
        if (encryptionKeyPB.id != null) {
            jsonObject.addProperty(ID, encryptionKeyPB.id);
        }
        if (encryptionKeyPB.parentId != null) {
            jsonObject.addProperty(PARENT_ID, encryptionKeyPB.parentId);
        }
        if (encryptionKeyPB.createTime != null) {
            jsonObject.addProperty(CREATE_TIME, encryptionKeyPB.createTime);
        }
        if (encryptionKeyPB.type != null) {
            jsonObject.addProperty(TYPE, encryptionKeyPB.type.toString());
        }
        if (encryptionKeyPB.algorithm != null) {
            jsonObject.addProperty(ALGORITHM, encryptionKeyPB.algorithm.toString());
        }
        if (encryptionKeyPB.encryptedKey != null) {
            String base64Encoded = Base64.getEncoder().encodeToString(encryptionKeyPB.encryptedKey);
            jsonObject.addProperty(ENCRYPTED_KEY, base64Encoded);
        }
        if (encryptionKeyPB.plainKey != null) {
            String base64Encoded = Base64.getEncoder().encodeToString(encryptionKeyPB.plainKey);
            jsonObject.addProperty(PLAIN_KEY, base64Encoded);
        }
        return jsonObject;
    }

    @Override
    public EncryptionKeyPB deserialize(JsonElement jsonElement, Type type,
                                       JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        EncryptionKeyPB encryptionKeyPB = new EncryptionKeyPB();
        if (jsonObject.has(ID)) {
            encryptionKeyPB.id = jsonObject.get(ID).getAsLong();
        }
        if (jsonObject.has(PARENT_ID)) {
            encryptionKeyPB.parentId = jsonObject.get(PARENT_ID).getAsLong();
        }
        if (jsonObject.has(CREATE_TIME)) {
            encryptionKeyPB.createTime = jsonObject.get(CREATE_TIME).getAsLong();
        }
        if (jsonObject.has(TYPE)) {
            encryptionKeyPB.type =
                    com.starrocks.proto.EncryptionKeyTypePB.valueOf(jsonObject.get(TYPE).getAsString());
        }
        if (jsonObject.has(ALGORITHM)) {
            encryptionKeyPB.algorithm =
                    com.starrocks.proto.EncryptionAlgorithmPB.valueOf(jsonObject.get(ALGORITHM).getAsString());
        }
        if (jsonObject.has(ENCRYPTED_KEY)) {
            String base64Encoded = jsonObject.get(ENCRYPTED_KEY).getAsString();
            encryptionKeyPB.encryptedKey = Base64.getDecoder().decode(base64Encoded);
        }
        if (jsonObject.has(PLAIN_KEY)) {
            String base64Encoded = jsonObject.get(PLAIN_KEY).getAsString();
            encryptionKeyPB.plainKey = Base64.getDecoder().decode(base64Encoded);
        }
        return encryptionKeyPB;
    }
}
