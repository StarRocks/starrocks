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

import com.starrocks.type.Type;

/**
 * Definition of a virtual column including its metadata.
 * Virtual columns are computed at query runtime and not persisted in storage.
 */
public class VirtualColumnDefinition {
    private final String name;
    private final Type type;
    private final String description;
    private final boolean enabled;
    
    // Lazy-initialized column instance
    private volatile Column column = null;

    public VirtualColumnDefinition(String name, Type type, String description) {
        this(name, type, description, true);
    }

    public VirtualColumnDefinition(String name, Type type, String description, boolean enabled) {
        this.name = name;
        this.type = type;
        this.description = description;
        this.enabled = enabled;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Get or create the Column instance for this virtual column definition.
     * Uses double-checked locking for thread-safe lazy initialization.
     * @return The Column instance
     */
    public Column getColumn() {
        if (column == null) {
            synchronized (this) {
                if (column == null) {
                    Column col = new Column(name, type);
                    col.setIsVirtual(true);
                    column = col;
                }
            }
        }
        return column;
    }

    @Override
    public String toString() {
        return String.format("VirtualColumn(%s, %s, enabled=%s)", name, type, enabled);
    }
}
