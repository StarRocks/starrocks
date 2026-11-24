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

package com.starrocks.persist.image;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Equivalence oracle for the image v3 migration: two objects are equivalent when the JSON trees
 * {@code GsonUtils.GSON} produces for them are equal. JSON object members are unordered, so map
 * iteration order does not matter; JSON arrays are ordered, so list order does.
 *
 * <p>Two kinds of exceptions can be declared as regular expressions over the JSON path
 * ({@code $.a.b[3].c}): paths to ignore (fields the v3 format deliberately does not carry) and
 * array paths whose order carries no meaning (Set-typed fields, maps with complex keys), which are
 * compared as multisets.
 */
public final class GsonEquivalence {
    private GsonEquivalence() {
    }

    public static void assertEquivalent(Object expected, Object actual) {
        assertEquivalent(expected, actual, Collections.emptySet(), Collections.emptySet());
    }

    public static void assertEquivalent(Object expected, Object actual, Set<String> ignoredPathPatterns,
                                        Set<String> unorderedPathPatterns) {
        JsonElement expectedJson = GsonUtils.GSON.toJsonTree(expected);
        JsonElement actualJson = GsonUtils.GSON.toJsonTree(actual);
        Rules rules = new Rules(ignoredPathPatterns, unorderedPathPatterns);
        String difference = firstDifference("$", expectedJson, actualJson, rules);
        Assertions.assertNull(difference, () -> "GSON views differ: " + difference);
    }

    /** Returns a description of the first difference, or null when the trees are equal. */
    static String firstDifference(String path, JsonElement expected, JsonElement actual) {
        return firstDifference(path, expected, actual, new Rules(Collections.emptySet(), Collections.emptySet()));
    }

    private static String firstDifference(String path, JsonElement expected, JsonElement actual, Rules rules) {
        if (expected.isJsonObject() && actual.isJsonObject()) {
            JsonObject e = expected.getAsJsonObject();
            JsonObject a = actual.getAsJsonObject();
            TreeSet<String> keys = new TreeSet<>(e.keySet());
            keys.addAll(a.keySet());
            for (String key : keys) {
                String memberPath = path + "." + key;
                if (rules.isIgnored(memberPath)) {
                    continue;
                }
                if (!e.has(key)) {
                    return memberPath + " only present in actual: " + a.get(key);
                }
                if (!a.has(key)) {
                    return memberPath + " only present in expected: " + e.get(key);
                }
                String diff = firstDifference(memberPath, e.get(key), a.get(key), rules);
                if (diff != null) {
                    return diff;
                }
            }
            return null;
        }
        if (expected.isJsonArray() && actual.isJsonArray()) {
            JsonArray e = expected.getAsJsonArray();
            JsonArray a = actual.getAsJsonArray();
            if (e.size() != a.size()) {
                return path + " has " + e.size() + " elements in expected but " + a.size() + " in actual";
            }
            if (rules.isUnordered(path)) {
                List<String> expectedElements = canonical(e);
                List<String> actualElements = canonical(a);
                return expectedElements.equals(actualElements) ? null
                        : path + " differs as a set: expected " + expectedElements + " but was " + actualElements;
            }
            for (int i = 0; i < e.size(); i++) {
                String diff = firstDifference(path + "[" + i + "]", e.get(i), a.get(i), rules);
                if (diff != null) {
                    return diff;
                }
            }
            return null;
        }
        return expected.equals(actual) ? null : path + ": expected " + expected + " but was " + actual;
    }

    private static List<String> canonical(JsonArray array) {
        List<String> elements = new ArrayList<>(array.size());
        for (JsonElement element : array) {
            elements.add(element.toString());
        }
        Collections.sort(elements);
        return elements;
    }

    /** Convenience for tests that build JSON by hand. */
    static JsonObject object(Map<String, JsonElement> members) {
        JsonObject object = new JsonObject();
        members.forEach(object::add);
        return object;
    }

    private static final class Rules {
        private final List<Pattern> ignored;
        private final List<Pattern> unordered;

        Rules(Set<String> ignoredPathPatterns, Set<String> unorderedPathPatterns) {
            ignored = ignoredPathPatterns.stream().map(Pattern::compile).collect(Collectors.toList());
            unordered = unorderedPathPatterns.stream().map(Pattern::compile).collect(Collectors.toList());
        }

        boolean isIgnored(String path) {
            return ignored.stream().anyMatch(pattern -> pattern.matcher(path).matches());
        }

        boolean isUnordered(String path) {
            return unordered.stream().anyMatch(pattern -> pattern.matcher(path).matches());
        }
    }
}
