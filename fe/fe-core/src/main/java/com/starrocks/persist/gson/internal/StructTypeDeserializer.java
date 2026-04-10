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

package com.starrocks.persist.gson.internal;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;

import java.util.ArrayList;

// Todo: remove it after remove selectedFields
public class StructTypeDeserializer implements JsonDeserializer<StructType> {
    @Override
    public StructType deserialize(JsonElement jsonElement, java.lang.reflect.Type type,
                                  JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {
        JsonObject dumpJsonObject = jsonElement.getAsJsonObject();
        boolean isNamed = false;
        if (dumpJsonObject.get("named") != null) {
            isNamed = dumpJsonObject.get("named").getAsBoolean();
        }
        JsonArray fields = dumpJsonObject.getAsJsonArray("fields");
        ArrayList<StructField> structFields = new ArrayList<>(fields.size());
        for (JsonElement field : fields) {
            structFields.add(GsonUtils.GSON.fromJson(field, StructField.class));
        }
        return new StructType(structFields, isNamed);
    }
}
