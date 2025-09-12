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

package com.starrocks.persist;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Persistence-only DTO for broker properties.
 * Avoids using AST class {@code com.starrocks.sql.ast.BrokerDesc} in metadata serialization.
 */
public class BrokerPropertiesPersistInfo {

    @SerializedName("n")
    private String name;
    @SerializedName("p")
    private Map<String, String> properties;

    // for deserialization
    public BrokerPropertiesPersistInfo() {
    }

    public BrokerPropertiesPersistInfo(Map<String, String> properties) {
        this.properties = properties;
    }

    public BrokerPropertiesPersistInfo(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    public boolean hasBroker() {
        return !Strings.isNullOrEmpty(name);
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}


