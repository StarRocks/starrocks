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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.starrocks.common.Range;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/*
 * The json adapter for Range with generic type support.
 */
public class RangeAdapter<T extends Comparable<T>>
        implements JsonSerializer<Range<T>>, JsonDeserializer<Range<T>> {

    @Override
    public JsonElement serialize(Range<T> src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();

        // Serialize bounds using context to preserve generic type
        if (src.getLowerBound() != null) {
            jsonObject.add("lowerBound", context.serialize(src.getLowerBound()));
        }
        if (src.getUpperBound() != null) {
            jsonObject.add("upperBound", context.serialize(src.getUpperBound()));
        }
        if (src.isLowerBoundIncluded()) {
            jsonObject.add("lowerBoundIncluded", new JsonPrimitive(true));
        }
        if (src.isUpperBoundIncluded()) {
            jsonObject.add("upperBoundIncluded", new JsonPrimitive(true));
        }

        return jsonObject;
    }

    @Override
    public Range<T> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();

        // Extract generic type information
        Type elementType;
        if (typeOfT instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) typeOfT;
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            elementType = actualTypeArguments[0];
        } else {
            // Cannot deserialize Range without generic type information
            throw new JsonParseException("Cannot deserialize Range without generic type information.");
        }

        T lowerBound = null;
        JsonElement lowerBoundElement = jsonObject.get("lowerBound");
        if (lowerBoundElement != null) {
            lowerBound = context.deserialize(lowerBoundElement, elementType);
        }

        T upperBound = null;
        JsonElement upperBoundElement = jsonObject.get("upperBound");
        if (upperBoundElement != null) {
            upperBound = context.deserialize(upperBoundElement, elementType);
        }

        boolean lowerBoundIncluded = false;
        JsonElement lowerBoundIncludedElement = jsonObject.get("lowerBoundIncluded");
        if (lowerBoundIncludedElement != null) {
            lowerBoundIncluded = lowerBoundIncludedElement.getAsBoolean();
        }

        boolean upperBoundIncluded = false;
        JsonElement upperBoundIncludedElement = jsonObject.get("upperBoundIncluded");
        if (upperBoundIncludedElement != null) {
            upperBoundIncluded = upperBoundIncludedElement.getAsBoolean();
        }

        return Range.of(lowerBound, upperBound, lowerBoundIncluded, upperBoundIncluded);
    }
}

