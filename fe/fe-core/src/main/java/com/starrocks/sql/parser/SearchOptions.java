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

package com.starrocks.sql.parser;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Validated options for the built-in {@code search()} function. */
public final class SearchOptions {
    public enum DefaultOperator {
        AND,
        OR
    }

    public enum MultiFieldType {
        BEST_FIELDS,
        CROSS_FIELDS
    }

    private static final int MAX_OPTIONS_LENGTH = 4096;

    private final DefaultOperator defaultOperator;
    private final List<String> effectiveFields;
    private final MultiFieldType multiFieldType;

    private SearchOptions(DefaultOperator defaultOperator, List<String> effectiveFields,
                          MultiFieldType multiFieldType) {
        this.defaultOperator = defaultOperator;
        this.effectiveFields = ImmutableList.copyOf(effectiveFields);
        this.multiFieldType = multiFieldType;
    }

    public static SearchOptions defaults() {
        return new SearchOptions(DefaultOperator.OR, ImmutableList.of(), MultiFieldType.BEST_FIELDS);
    }

    public static SearchOptions parse(String json, NodePosition position) {
        if (json == null) {
            throw new ParsingException("search() options must not be null", position);
        }
        if (json.length() > MAX_OPTIONS_LENGTH) {
            throw new ParsingException("search() options is too long, at most " + MAX_OPTIONS_LENGTH
                    + " characters are supported", position);
        }

        JsonObject object = parseObject(json, position);

        String defaultField = null;
        DefaultOperator defaultOperator = DefaultOperator.OR;
        List<String> fields = null;
        MultiFieldType type = MultiFieldType.BEST_FIELDS;
        boolean typeSpecified = false;

        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            switch (entry.getKey()) {
                case "default_field":
                    defaultField = parseField(entry.getKey(), entry.getValue(), position);
                    break;
                case "default_operator":
                    defaultOperator = parseDefaultOperator(entry.getValue(), position);
                    break;
                case "fields":
                    fields = parseFields(entry.getValue(), position);
                    break;
                case "type":
                    type = parseType(entry.getValue(), position);
                    typeSpecified = true;
                    break;
                default:
                    throw new ParsingException("unknown search() option: '" + entry.getKey() + "'", position);
            }
        }

        if (defaultField != null && fields != null) {
            throw new ParsingException("search() options 'default_field' and 'fields' are mutually exclusive", position);
        }
        if (typeSpecified && fields == null) {
            throw new ParsingException("search() option 'type' requires a non-empty 'fields' option", position);
        }
        List<String> effectiveFields = defaultField == null
                ? (fields == null ? ImmutableList.of() : fields)
                : ImmutableList.of(defaultField);
        return new SearchOptions(defaultOperator, effectiveFields, type);
    }

    private static JsonObject parseObject(String json, NodePosition position) {
        Set<String> keys = new HashSet<>();
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.setLenient(false);
            if (reader.peek() != JsonToken.BEGIN_OBJECT) {
                throw new ParsingException("search() options must be a JSON object", position);
            }
            JsonObject object = new JsonObject();
            reader.beginObject();
            while (reader.hasNext()) {
                String key = reader.nextName();
                if (!keys.add(key)) {
                    throw new ParsingException("duplicate search() option: '" + key + "'", position);
                }
                object.add(key, Streams.parse(reader));
            }
            reader.endObject();
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw new ParsingException("search() options must contain exactly one JSON object", position);
            }
            return object;
        } catch (ParsingException exception) {
            throw exception;
        } catch (IOException | JsonParseException | IllegalStateException exception) {
            throw new ParsingException("search() options is not valid JSON: " + exception.getMessage(), position);
        }
    }

    private static DefaultOperator parseDefaultOperator(JsonElement value, NodePosition position) {
        String text = parseString("default_operator", value, position).toUpperCase(Locale.ROOT);
        try {
            return DefaultOperator.valueOf(text);
        } catch (IllegalArgumentException exception) {
            throw new ParsingException("search() option 'default_operator' must be \"and\" or \"or\"", position);
        }
    }

    private static MultiFieldType parseType(JsonElement value, NodePosition position) {
        String text = parseString("type", value, position).toUpperCase(Locale.ROOT);
        try {
            return MultiFieldType.valueOf(text);
        } catch (IllegalArgumentException exception) {
            throw new ParsingException(
                    "search() option 'type' must be \"best_fields\" or \"cross_fields\"", position);
        }
    }

    private static List<String> parseFields(JsonElement value, NodePosition position) {
        if (!value.isJsonArray()) {
            throw new ParsingException("search() option 'fields' must be a string array", position);
        }
        List<String> result = new ArrayList<>();
        Set<String> distinct = new HashSet<>();
        for (JsonElement element : value.getAsJsonArray()) {
            String field = parseField("fields", element, position);
            if (!distinct.add(field.toLowerCase(Locale.ROOT))) {
                throw new ParsingException("search() option 'fields' contains duplicate field '" + field + "'", position);
            }
            result.add(field);
        }
        if (result.isEmpty()) {
            throw new ParsingException("search() option 'fields' must not be an empty array", position);
        }
        return result;
    }

    private static String parseField(String key, JsonElement value, NodePosition position) {
        String field = parseString(key, value, position);
        if (!SearchDslAstBuilder.isValidFieldName(field)) {
            throw new ParsingException("search() option '" + key + "' contains invalid field '" + field + "'", position);
        }
        return field;
    }

    private static String parseString(String key, JsonElement value, NodePosition position) {
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new ParsingException("search() option '" + key + "' must be a string", position);
        }
        return value.getAsString();
    }

    public DefaultOperator getDefaultOperator() {
        return defaultOperator;
    }

    /**
     * Returns the canonical field selection used by the predicate builder. A configured
     * {@code default_field} is normalized to a one-element list during option parsing.
     */
    public List<String> getEffectiveFields() {
        return effectiveFields;
    }

    public MultiFieldType getMultiFieldType() {
        return multiFieldType;
    }
}
