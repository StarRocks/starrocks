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
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.concurrent.ArrayBlockingQueue;

public class ArrayBlockingQueueAdapter<E>
        implements JsonSerializer<ArrayBlockingQueue<E>>, JsonDeserializer<ArrayBlockingQueue<E>> {

    @Override
    public JsonElement serialize(ArrayBlockingQueue<E> queue, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("capacity", queue.size() + queue.remainingCapacity());
        JsonArray elementsArray = new JsonArray();
        for (E element : queue) {
            elementsArray.add(context.serialize(element));
        }
        jsonObject.add("elements", elementsArray);
        return jsonObject;
    }

    @Override
    public ArrayBlockingQueue<E> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        Type typeOfE = ((ParameterizedType) typeOfT).getActualTypeArguments()[0];
        JsonObject jsonObject = json.getAsJsonObject();
        int capacity = jsonObject.get("capacity").getAsInt();
        JsonArray elementsArray = jsonObject.get("elements").getAsJsonArray();
        ArrayBlockingQueue<E> queue = new ArrayBlockingQueue<>(capacity);
        for (JsonElement jsonElement : elementsArray) {
            E element = context.deserialize(jsonElement, typeOfE);
            queue.add(element);
        }
        return queue;
    }
}