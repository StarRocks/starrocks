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

import java.util.Map;
import java.util.Set;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsProperties {
    public static final String ACCESS_ID = "odps.access.id";
    public static final String ACCESS_KEY = "odps.access.key";
    public static final String ENDPOINT = "odps.endpoint";
    public static final String PROJECT = "odps.project";
    public static final String TUNNEL_ENDPOINT = "odps.tunnel.endpoint";
    public static final String TUNNEL_QUOTA = "odps.tunnel.quota";

    static {
        defaultValues = new java.util.HashMap<>();
        requiredProperties = new java.util.HashSet<>();

        newProperty(ACCESS_ID).isRequired();
        newProperty(ACCESS_KEY).isRequired();
        newProperty(ENDPOINT).isRequired();
        newProperty(PROJECT).isRequired();
        newProperty(TUNNEL_ENDPOINT);
        newProperty(TUNNEL_QUOTA);
    }

    private Map<String, String> properties;

    private static Map<String, String> defaultValues;
    private static Set<String> requiredProperties;

    public OdpsProperties(Map<String, String> properties) {
        this.properties = properties;
        validate();
    }

    public String get(String key) {
        return properties.getOrDefault(key, defaultValues.get(key));
    }

    static Property newProperty(String key) {
        return new Property(key);
    }

    private void validate() {
        for (String value : requiredProperties) {
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

        Property withDefaultValue(String defaultValue) {
            defaultValues.put(key, defaultValue);
            return this;
        }

        Property isRequired() {
            requiredProperties.add(key);
            return this;
        }
    }

}
