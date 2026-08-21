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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.analyzer.SemanticException;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Immutable database-scoped Tantivy analyzer pipeline. */
public class TextAnalyzer implements Writable {
    public static final int SPEC_VERSION = 1;
    public static final int RUNTIME_ABI_VERSION = 1;
    public static final String BUILTIN_MODEL_VERSION = "starrocks-tantivy-3.5-v1";

    public static final int MAX_DEFINITION_BYTES = 64 * 1024;
    public static final int MAX_PIPELINE_COMPONENTS = 16;
    public static final int MAX_MAPPING_RULES = 256;
    public static final int MAX_MAPPING_RULE_BYTES = 1024;
    public static final int MAX_MAPPING_BYTES = 32 * 1024;
    public static final int MAX_STOPWORDS = 1024;
    public static final int MAX_STOPWORD_BYTES = 256;
    public static final int MAX_STOPWORDS_BYTES = 32 * 1024;
    public static final int MAX_TOKEN_BYTES = 32 * 1024;

    @SerializedName("id")
    private long id;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("name")
    private String name;
    @SerializedName("canonicalDefinition")
    private String canonicalDefinition;
    @SerializedName("digest")
    private String digest;
    @SerializedName("runtimeAbiVersion")
    private int runtimeAbiVersion;
    @SerializedName("createTime")
    private long createTime;
    @SerializedName("owner")
    private String owner;

    public TextAnalyzer() {
    }

    public TextAnalyzer(long id, long dbId, String name, String canonicalDefinition,
                        String digest, String owner) {
        this(id, dbId, name, canonicalDefinition, digest, RUNTIME_ABI_VERSION,
                System.currentTimeMillis(), owner);
    }

    public TextAnalyzer(long id, long dbId, String name, String canonicalDefinition,
                        String digest, int runtimeAbiVersion, long createTime, String owner) {
        this.id = id;
        this.dbId = dbId;
        this.name = name;
        this.canonicalDefinition = canonicalDefinition;
        this.digest = digest;
        this.runtimeAbiVersion = runtimeAbiVersion;
        this.createTime = createTime;
        this.owner = owner;
    }

    public static Definition canonicalize(String definition) {
        if (definition == null || definition.getBytes(StandardCharsets.UTF_8).length > MAX_DEFINITION_BYTES) {
            throw new SemanticException("TEXT ANALYZER definition must be non-null and at most "
                    + MAX_DEFINITION_BYTES + " bytes");
        }
        final JsonObject input;
        try {
            input = JsonParser.parseString(definition).getAsJsonObject();
        } catch (Exception e) {
            throw new SemanticException("Invalid TEXT ANALYZER definition: " + e.getMessage());
        }
        rejectUnknown(input, "analyzer", "spec_version", "runtime_abi_version", "builtin_model_version",
                "char_filter", "tokenizer", "token_filter", "resource_refs");

        int specVersion = optionalInt(input, "spec_version", SPEC_VERSION);
        int abiVersion = optionalInt(input, "runtime_abi_version", RUNTIME_ABI_VERSION);
        String modelVersion = optionalString(input, "builtin_model_version", BUILTIN_MODEL_VERSION);
        if (specVersion != SPEC_VERSION) {
            throw new SemanticException("Unsupported analyzer spec_version " + specVersion);
        }
        if (abiVersion != RUNTIME_ABI_VERSION) {
            throw new SemanticException("Unsupported analyzer runtime_abi_version " + abiVersion);
        }
        if (!BUILTIN_MODEL_VERSION.equals(modelVersion)) {
            throw new SemanticException("Unsupported analyzer builtin_model_version " + modelVersion);
        }

        JsonArray charFilters = input.has("char_filter") ? requireArray(input, "char_filter") : new JsonArray();
        JsonArray tokenFilters = input.has("token_filter") ? requireArray(input, "token_filter") : new JsonArray();
        if (charFilters.size() + tokenFilters.size() + 1 > MAX_PIPELINE_COMPONENTS) {
            throw new SemanticException("TEXT ANALYZER pipeline exceeds " + MAX_PIPELINE_COMPONENTS + " components");
        }
        if (!input.has("tokenizer") || !input.get("tokenizer").isJsonObject()) {
            throw new SemanticException("TEXT ANALYZER requires one tokenizer object");
        }
        JsonArray resources = input.has("resource_refs") ? requireArray(input, "resource_refs") : new JsonArray();
        if (resources.size() != 0) {
            throw new SemanticException("resource_refs are not supported in phase one");
        }

        JsonObject canonical = new JsonObject();
        canonical.addProperty("spec_version", SPEC_VERSION);
        canonical.addProperty("runtime_abi_version", RUNTIME_ABI_VERSION);
        canonical.addProperty("builtin_model_version", BUILTIN_MODEL_VERSION);
        canonical.add("char_filter", canonicalizeCharFilters(charFilters));
        canonical.add("tokenizer", canonicalizeTokenizer(input.getAsJsonObject("tokenizer")));
        canonical.add("token_filter", canonicalizeTokenFilters(tokenFilters));
        canonical.add("resource_refs", new JsonArray());
        String canonicalJson = canonical.toString();
        return new Definition(canonicalJson, sha256(canonicalJson));
    }

    private static JsonArray canonicalizeCharFilters(JsonArray filters) {
        JsonArray output = new JsonArray();
        for (JsonElement element : filters) {
            JsonObject filter = requireObject(element, "char_filter");
            String type = requireString(filter, "type").toLowerCase(Locale.ROOT);
            JsonObject value = new JsonObject();
            if ("unicode_normalize".equals(type)) {
                rejectUnknown(filter, "unicode_normalize", "type", "form");
                String form = requireString(filter, "form").toLowerCase(Locale.ROOT);
                if (!Set.of("nfc", "nfkc", "nfd", "nfkd").contains(form)) {
                    throw new SemanticException("Unsupported unicode normalization form " + form);
                }
                value.addProperty("type", type);
                value.addProperty("form", form);
            } else if ("mapping".equals(type) || "char_replace".equals(type)) {
                rejectUnknown(filter, "mapping", "type", "mappings");
                JsonArray mappings = requireArray(filter, "mappings");
                if (mappings.size() > MAX_MAPPING_RULES) {
                    throw new SemanticException("mapping contains more than " + MAX_MAPPING_RULES + " rules");
                }
                int totalBytes = 0;
                JsonArray canonicalMappings = new JsonArray();
                for (JsonElement mapping : mappings) {
                    if (!mapping.isJsonPrimitive() || !mapping.getAsJsonPrimitive().isString()) {
                        throw new SemanticException("mapping rules must be strings");
                    }
                    String rule = mapping.getAsString();
                    int bytes = rule.getBytes(StandardCharsets.UTF_8).length;
                    if (bytes > MAX_MAPPING_RULE_BYTES || !rule.contains("=>")
                            || rule.substring(0, rule.indexOf("=>")).trim().isEmpty()) {
                        throw new SemanticException("Invalid mapping rule: " + rule);
                    }
                    totalBytes += bytes;
                    canonicalMappings.add(rule);
                }
                if (totalBytes > MAX_MAPPING_BYTES) {
                    throw new SemanticException("mapping rules exceed " + MAX_MAPPING_BYTES + " bytes");
                }
                value.addProperty("type", "mapping");
                value.add("mappings", canonicalMappings);
            } else {
                throw new SemanticException("Unsupported char_filter type " + type);
            }
            output.add(value);
        }
        return output;
    }

    private static JsonObject canonicalizeTokenizer(JsonObject tokenizer) {
        String type = requireString(tokenizer, "type").toLowerCase(Locale.ROOT);
        if ("raw".equals(type)) {
            type = "none";
        } else if ("cjk".equals(type)) {
            type = "chinese";
        }
        JsonObject output = new JsonObject();
        switch (type) {
            case "none":
            case "english":
            case "standard":
            case "chinese":
                rejectUnknown(tokenizer, type, "type");
                output.addProperty("type", type);
                break;
            case "jieba":
                rejectUnknown(tokenizer, type, "type", "mode", "hmm");
                String mode = optionalString(tokenizer, "mode", "search").toLowerCase(Locale.ROOT);
                if (!Set.of("search", "default").contains(mode)) {
                    throw new SemanticException("Unsupported jieba mode " + mode);
                }
                output.addProperty("type", type);
                output.addProperty("mode", mode);
                output.addProperty("hmm", optionalBoolean(tokenizer, "hmm", true));
                break;
            case "ik":
                rejectUnknown(tokenizer, type, "type", "mode");
                String ikMode = optionalString(tokenizer, "mode", "search").toLowerCase(Locale.ROOT);
                if ("ik_smart".equals(ikMode)) {
                    ikMode = "search";
                } else if ("ik_max_word".equals(ikMode)) {
                    ikMode = "index";
                } else if (!Set.of("search", "index").contains(ikMode)) {
                    throw new SemanticException("Unsupported ik mode " + ikMode);
                }
                output.addProperty("type", type);
                output.addProperty("mode", ikMode);
                break;
            case "ngram":
                rejectUnknown(tokenizer, type, "type", "min_gram", "max_gram");
                int minGram = requireInt(tokenizer, "min_gram");
                int maxGram = requireInt(tokenizer, "max_gram");
                if (minGram < 1 || maxGram < minGram || maxGram > 32 || maxGram - minGram > 16) {
                    throw new SemanticException("ngram requires 1 <= min_gram <= max_gram <= 32 and gap <= 16");
                }
                output.addProperty("type", type);
                output.addProperty("min_gram", minGram);
                output.addProperty("max_gram", maxGram);
                break;
            default:
                throw new SemanticException("Unsupported tokenizer type " + type);
        }
        return output;
    }

    private static JsonArray canonicalizeTokenFilters(JsonArray filters) {
        JsonArray output = new JsonArray();
        for (JsonElement element : filters) {
            JsonObject filter = requireObject(element, "token_filter");
            String type = requireString(filter, "type").toLowerCase(Locale.ROOT);
            JsonObject value = new JsonObject();
            switch (type) {
                case "lowercase":
                case "remove_punctuation":
                    rejectUnknown(filter, type, "type");
                    value.addProperty("type", type);
                    break;
                case "stop":
                    rejectUnknown(filter, type, "type", "stopwords");
                    JsonArray stopwords = requireArray(filter, "stopwords");
                    if (stopwords.size() > MAX_STOPWORDS) {
                        throw new SemanticException("stop filter contains more than " + MAX_STOPWORDS + " stopwords");
                    }
                    int total = 0;
                    JsonArray canonicalStopwords = new JsonArray();
                    for (JsonElement stopword : stopwords) {
                        if (!stopword.isJsonPrimitive() || !stopword.getAsJsonPrimitive().isString()) {
                            throw new SemanticException("stopwords must be strings");
                        }
                        String word = stopword.getAsString();
                        int bytes = word.getBytes(StandardCharsets.UTF_8).length;
                        if (bytes > MAX_STOPWORD_BYTES) {
                            throw new SemanticException("stopword exceeds " + MAX_STOPWORD_BYTES + " bytes");
                        }
                        total += bytes;
                        canonicalStopwords.add(word);
                    }
                    if (total > MAX_STOPWORDS_BYTES) {
                        throw new SemanticException("stopwords exceed " + MAX_STOPWORDS_BYTES + " bytes");
                    }
                    value.addProperty("type", type);
                    value.add("stopwords", canonicalStopwords);
                    break;
                case "length":
                    rejectUnknown(filter, type, "type", "min", "max");
                    int min = optionalInt(filter, "min", 0);
                    int max = optionalInt(filter, "max", MAX_TOKEN_BYTES);
                    if (min < 0 || max < min || max > MAX_TOKEN_BYTES) {
                        throw new SemanticException("length requires 0 <= min <= max <= " + MAX_TOKEN_BYTES);
                    }
                    value.addProperty("type", type);
                    value.addProperty("min", min);
                    value.addProperty("max", max);
                    break;
                default:
                    throw new SemanticException("Unsupported token_filter type " + type);
            }
            output.add(value);
        }
        return output;
    }

    private static void rejectUnknown(JsonObject object, String component, String... supported) {
        Set<String> allowed = new HashSet<>(Arrays.asList(supported));
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            if (!allowed.contains(entry.getKey())) {
                throw new SemanticException("Unknown field '" + entry.getKey() + "' in " + component);
            }
        }
    }

    private static JsonObject requireObject(JsonElement element, String name) {
        if (element == null || !element.isJsonObject()) {
            throw new SemanticException(name + " entries must be objects");
        }
        return element.getAsJsonObject();
    }

    private static JsonArray requireArray(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || !element.isJsonArray()) {
            throw new SemanticException(name + " must be an array");
        }
        return element.getAsJsonArray();
    }

    private static String requireString(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || !element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
            throw new SemanticException(name + " must be a string");
        }
        return element.getAsString();
    }

    private static String optionalString(JsonObject object, String name, String defaultValue) {
        return object.has(name) ? requireString(object, name) : defaultValue;
    }

    private static int requireInt(JsonObject object, String name) {
        JsonElement element = object.get(name);
        try {
            if (element == null || !element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                throw new NumberFormatException();
            }
            BigDecimal value = element.getAsBigDecimal();
            return value.intValueExact();
        } catch (Exception e) {
            throw new SemanticException(name + " must be an integer");
        }
    }

    private static int optionalInt(JsonObject object, String name, int defaultValue) {
        return object.has(name) ? requireInt(object, name) : defaultValue;
    }

    private static boolean optionalBoolean(JsonObject object, String name, boolean defaultValue) {
        if (!object.has(name)) {
            return defaultValue;
        }
        JsonElement element = object.get(name);
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isBoolean()) {
            throw new SemanticException(name + " must be a boolean");
        }
        return element.getAsBoolean();
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                result.append(String.format("%02x", b & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public String getName() {
        return name;
    }

    public String getCanonicalDefinition() {
        return canonicalDefinition;
    }

    public String getDigest() {
        return digest;
    }

    public int getRuntimeAbiVersion() {
        return runtimeAbiVersion;
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getOwner() {
        return owner;
    }

    public static class Definition {
        private final String canonicalJson;
        private final String digest;

        Definition(String canonicalJson, String digest) {
            this.canonicalJson = canonicalJson;
            this.digest = digest;
        }

        public String getCanonicalJson() {
            return canonicalJson;
        }

        public String getDigest() {
            return digest;
        }
    }
}
