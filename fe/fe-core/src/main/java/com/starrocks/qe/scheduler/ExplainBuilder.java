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

package com.starrocks.qe.scheduler;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The utility class to help build scheduler explain.
 */
public class ExplainBuilder {
    private final StringBuilder builder = new StringBuilder();

    private int level = 0;

    public String build() {
        return builder.toString();
    }

    /**
     * Add key and the nested value to the builder.
     * @param key The key.
     * @param consumer The function to generate the nested value.
     */
    public ExplainBuilder addValue(String key, Runnable consumer) {
        addPrefix();
        builder.append(key).append('\n');

        level++;
        consumer.run();
        level--;

        return this;
    }

    /**
     * Add key and string value to the builder.
     * @param key The key.
     * @param value The value which will be added by {@link Object#toString()}.
     */
    public ExplainBuilder addValue(String key, Object value) {
        addPrefix();
        builder.append(key).append(": ").append(value).append('\n');
        return this;
    }
    /**
     * Add key and multiple string values to the builder.
     * @param key The key.
     * @param values All the values will be joined by "{@code ,}".
     */
    public <T> ExplainBuilder addValue(String key, List<T> values) {
        String stringValue = values.stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));
        return addValue(key, stringValue);
    }

    /**
     * Add the array item to the builder.
     * @param index The order of the item in the array.
     * @param value The value which will be added by {@link Object#toString()}.
     */
    public ExplainBuilder addOrderedItem(int index, Object value) {
        addPrefix();
        builder.append(index).append(". ").append(value).append('\n');
        return this;
    }

    private void addPrefix() {
        for (int i = 0; i < level * 2; i++) {
            builder.append(' ');
        }
    }
}
