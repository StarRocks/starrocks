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

package com.starrocks.privilege.cauthz;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * CauthzAccessResourceImpl is the base class that is used to 
 * represent a resource. A resource is a set of key-value pairs.
 * For example, a table resource could be represented as:
 * { "database": "db1", "table": "tbl1" }
 */
public class CauthzAccessResourceImpl {
    private final Map<String, String> resourceMap;

    public CauthzAccessResourceImpl() {
        this.resourceMap = new HashMap<>();
    }

    public CauthzAccessResourceImpl(Map<String, String> resourceMap) {
        this.resourceMap = new HashMap<>(resourceMap);
    }

    public Map<String, String> getResourceMap() {
        return Collections.unmodifiableMap(resourceMap);
    }

    public void setValue(String key, String value) {
        resourceMap.put(key, value);
    }

    public String getValue(String key) {
        return resourceMap.get(key);
    }

    public Set<String> getKeys() {
        return resourceMap.keySet();
    }

    public boolean containsKey(String key) {
        return resourceMap.containsKey(key);
    }

    public String toString() {
        return "CauthzAccessResourceImpl{" +
                "resourceMap=" + resourceMap +
                '}';
    }
}