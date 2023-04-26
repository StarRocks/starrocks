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

package com.starrocks.sql.ast;

import com.google.common.collect.Maps;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import java.util.Objects;

public class Property implements ParseNode {
    private final String key;
    private final String value;

    private final NodePosition pos;

    public Property(String key, String value) {
        this(key, value, NodePosition.ZERO);
    }

    public Property(String key, String value, NodePosition pos) {
        this.pos = pos;
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Map<String, String> getMap() {
        Map<String, String> map = Maps.newHashMap();
        map.put(key, value);
        return map;
    }

    public boolean containsKey(String key) {
        return key.equals(this.key);
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    // only compare keys in properties
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Property that = (Property) o;
        return Objects.equals(this.key, that.key);
    }
}
