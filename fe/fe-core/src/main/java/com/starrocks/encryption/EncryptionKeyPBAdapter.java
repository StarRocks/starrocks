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
    @Override
    public JsonElement serialize(EncryptionKeyPB encryptionKeyPB, Type type,
                                 JsonSerializationContext jsonSerializationContext) {
        JsonObject jsonObject = new JsonObject();
        if (encryptionKeyPB.id != null) {
            jsonObject.addProperty("id", encryptionKeyPB.id);
        }
        if (encryptionKeyPB.parentId != null) {
            jsonObject.addProperty("parent_id", encryptionKeyPB.parentId);
        }
        if (encryptionKeyPB.type != null) {
            jsonObject.addProperty("type", encryptionKeyPB.type.toString());
        }
        if (encryptionKeyPB.algorithm != null) {
            jsonObject.addProperty("algorithm", encryptionKeyPB.algorithm.toString());
        }
        if (encryptionKeyPB.encryptedKey != null) {
            String base64Encoded = Base64.getEncoder().encodeToString(encryptionKeyPB.encryptedKey);
            jsonObject.addProperty("encrypted_key", base64Encoded);
        }
        return jsonObject;
    }

    @Override
    public EncryptionKeyPB deserialize(JsonElement jsonElement, Type type,
                                       JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        EncryptionKeyPB encryptionKeyPB = new EncryptionKeyPB();
        if (jsonObject.has("id")) {
            encryptionKeyPB.id = jsonObject.get("id").getAsLong();
        }
        if (jsonObject.has("parent_id")) {
            encryptionKeyPB.parentId = jsonObject.get("parent_id").getAsLong();
        }
        if (jsonObject.has("type")) {
            encryptionKeyPB.type =
                    com.starrocks.proto.EncryptionKeyTypePB.valueOf(jsonObject.get("type").getAsString());
        }
        if (jsonObject.has("algorithm")) {
            encryptionKeyPB.algorithm =
                    com.starrocks.proto.EncryptionAlgorithmPB.valueOf(jsonObject.get("algorithm").getAsString());
        }
        if (jsonObject.has("encrypted_key")) {
            String base64Encoded = jsonObject.get("encrypted_key").getAsString();
            encryptionKeyPB.encryptedKey = Base64.getDecoder().decode(base64Encoded);
        }
        return encryptionKeyPB;
    }
}
