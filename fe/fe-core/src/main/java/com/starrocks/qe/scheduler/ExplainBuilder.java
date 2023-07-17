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

import com.google.common.base.Preconditions;

import java.util.Collection;

public class ExplainBuilder {
    private static final int NUM_INLINE_ITEMS_PER_ROW = 10;

    private final StringBuilder builder = new StringBuilder();

    private int level;

    public ExplainBuilder(int level) {
        Preconditions.checkArgument(level >= 0, "explain level should be non-negative");
        this.level = level;
    }

    public String build() {
        return builder.toString();
    }

    public ExplainBuilder addMultipleChildren(String key, Runnable consumer) {
        addPrefix();
        builder.append(key).append('\n');

        level++;
        consumer.run();
        level--;

        return this;
    }

    public ExplainBuilder addSimpleKVItem(String key, Object value) {
        addPrefix();
        builder.append(key).append(": ").append(value).append('\n');
        return this;
    }

    public ExplainBuilder addOrderedItem(int index, Object value) {
        addPrefix();
        builder.append(index).append(". ").append(value).append('\n');
        return this;
    }

    public <T> ExplainBuilder addInlineItems(Collection<T> items) {
        int i = 0;
        addPrefix();
        for (T item : items) {
            builder.append(item);

            i++;
            if (i != items.size()) {
                if (i % NUM_INLINE_ITEMS_PER_ROW == 0) {
                    builder.append('\n');
                    addPrefix();
                } else {
                    builder.append(',');
                }
            }
        }
        builder.append("\n");

        return this;
    }

    private void addPrefix() {
        for (int i = 0; i < level * 2; i++) {
            builder.append(' ');
        }
    }

}
