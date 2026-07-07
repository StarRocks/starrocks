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

package com.starrocks.context;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * Helpers for parsing JsonElement values out of internal-table SQL result rows.
 *
 * <p>StarRocks' BOOLEAN column type renders as a JSON number primitive (1/0) on the result wire
 * — not as a JSON boolean primitive. Gson's {@link JsonElement#getAsBoolean()} routes such numeric
 * primitives through {@code Boolean.parseBoolean(getAsString())}, which returns false for "1" and
 * "0" alike. The result is silent boolean corruption: {@code deleted=1} in the versions table
 * shows up as {@code false} in the REST response.
 *
 * <p>{@link #parseBool(JsonElement)} accepts JSON booleans, JSON numbers (non-zero = true), and
 * JSON strings ("1"/"true" case-insensitive = true) so internal-table reads round-trip cleanly
 * regardless of which form the engine chose.
 */
public final class ContextJsonUtil {

    private ContextJsonUtil() {
    }

    public static boolean parseBool(JsonElement el) {
        if (el == null || el.isJsonNull()) {
            return false;
        }
        if (!el.isJsonPrimitive()) {
            return false;
        }
        JsonPrimitive p = el.getAsJsonPrimitive();
        if (p.isBoolean()) {
            return p.getAsBoolean();
        }
        if (p.isNumber()) {
            return p.getAsLong() != 0L;
        }
        if (p.isString()) {
            String s = p.getAsString().trim();
            if (s.isEmpty()) {
                return false;
            }
            if ("1".equals(s) || "true".equalsIgnoreCase(s)) {
                return true;
            }
            if ("0".equals(s) || "false".equalsIgnoreCase(s)) {
                return false;
            }
        }
        return false;
    }
}
