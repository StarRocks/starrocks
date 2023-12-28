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

package com.starrocks.connector.odps;

import com.aliyun.odps.utils.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OdpsProperties {
    public static final String ACCESS_ID = "odps.access.id";
    public static final String ACCESS_KEY = "odps.access.key";
    public static final String ENDPOINT = "odps.endpoint";
    public static final String PROJECT = "odps.project";
    public static final String TUNNEL_ENDPOINT = "odps.tunnel.endpoint";
    public static final String TUNNEL_QUOTA = "odps.tunnel.quota";
    public static final String SPLIT_POLICY = "odps.split.policy";
    public static final String SPLIT_ROW_COUNT = "odps.split.row.count";

    public static final String ENABLE_TABLE_CACHE = "odps.cache.table.enable";
    public static final String TABLE_CACHE_EXPIRE_TIME = "odps.cache.table.expire";
    public static final String TABLE_CACHE_SIZE = "odps.cache.table.size";
    public static final String ENABLE_PARTITION_CACHE = "odps.cache.partition.enable";
    public static final String PARTITION_CACHE_EXPIRE_TIME = "odps.cache.partition.expire";
    public static final String PARTITION_CACHE_SIZE = "odps.cache.partition.size";
    public static final String ENABLE_TABLE_NAME_CACHE = "odps.cache.table-name.enable";
    public static final String TABLE_NAME_CACHE_EXPIRE_TIME = "odps.cache.table-name.expire";
    public static final String PROJECT_CACHE_SIZE = "odps.cache.table-name.size";

    public static final String ROW_OFFSET = "row_offset";
    public static final String SIZE = "size";
    private static final long DEFAULT_SPLIT_ROW_COUNT = 4 * 1024 * 1024L;
    private final Map<String, String> properties;
    private static final Map<String, String> DEFAULT_VALUES = new HashMap<>();
    private static final Set<String> REQUIRED_PROPERTIES = new HashSet<>();

    static {
        newProperty(ACCESS_ID).isRequired();
        newProperty(ACCESS_KEY).isRequired();
        newProperty(ENDPOINT).isRequired();
        newProperty(PROJECT).isRequired();
        newProperty(SPLIT_POLICY).withDefaultValue(SIZE);
        newProperty(SPLIT_ROW_COUNT).withDefaultValue(String.valueOf(DEFAULT_SPLIT_ROW_COUNT));
        newProperty(TUNNEL_ENDPOINT).noDefaultValue();
        newProperty(TUNNEL_QUOTA).noDefaultValue();

        newProperty(ENABLE_TABLE_CACHE).withDefaultValue(true);
        newProperty(TABLE_CACHE_EXPIRE_TIME).withDefaultValue(86400);
        newProperty(TABLE_CACHE_SIZE).withDefaultValue(1000);
        newProperty(ENABLE_PARTITION_CACHE).withDefaultValue(true);
        newProperty(PARTITION_CACHE_EXPIRE_TIME).withDefaultValue(86400);
        newProperty(PARTITION_CACHE_SIZE).withDefaultValue(1000);
        newProperty(ENABLE_TABLE_NAME_CACHE).withDefaultValue(false);
        newProperty(TABLE_NAME_CACHE_EXPIRE_TIME).withDefaultValue(86400);
        newProperty(PROJECT_CACHE_SIZE).withDefaultValue(1000);
    }

    public OdpsProperties(Map<String, String> properties) {
        this.properties = properties;
        validate();
    }

    public String get(String key) {
        return properties.getOrDefault(key, DEFAULT_VALUES.get(key));
    }

    static Property newProperty(String key) {
        return new Property(key);
    }

    private void validate() {
        for (String value : REQUIRED_PROPERTIES) {
            if (StringUtils.isNullOrEmpty(properties.get(value))) {
                throw new IllegalArgumentException("Missing " + value + " in properties");
            }
        }
    }

    static class Property {
        private final String key;

        Property(String key) {
            this.key = key;
        }

        Property withDefaultValue(Object defaultValue) {
            DEFAULT_VALUES.put(key, defaultValue.toString());
            return this;
        }

        Property noDefaultValue() {
            DEFAULT_VALUES.put(key, "");
            return this;
        }

        Property isRequired() {
            REQUIRED_PROPERTIES.add(key);
            return this;
        }
    }

}
