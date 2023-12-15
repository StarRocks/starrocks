// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
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
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.Map;
import java.util.StringJoiner;

public class PropertySet implements ParseNode {

    private final Map<String, Property> propertyMap = Maps.newHashMap();

    private final NodePosition pos;

    public PropertySet(Collection<Property> properties, NodePosition pos) {
        this.pos = pos;
        if (CollectionUtils.isNotEmpty(properties)) {
            for (Property property : properties) {
                propertyMap.put(property.getKey(), property);
            }
        }
    }


    public boolean containsProperty(String name) {
        return propertyMap.containsKey(name);
    }

    public Collection<Property> getPropertySet() {
        return propertyMap.values();
    }

    public Map<String, Property> getPropertyMap() {
        return propertyMap;
    }

    public String getPropertyValue(String name) {
        return propertyMap.get(name).getValue();
    }

    public Property removeProperty(String name) {
        return propertyMap.remove(name);
    }

    public String getPropertyNames() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (Map.Entry<String, Property> entry : propertyMap.entrySet()) {
            joiner.add(entry.getKey());
        }
        return joiner.toString();
    }


    public boolean isEmpty() {
        return propertyMap.isEmpty();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
