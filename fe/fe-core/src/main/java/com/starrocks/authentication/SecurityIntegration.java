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


package com.starrocks.authentication;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Security integration specified in `Config.authentication_chain`.
 * Authentication for this integration is provided by corresponding `getAuthenticationProvider()`.
 */
public abstract class SecurityIntegration {
    public static final String SECURITY_INTEGRATION_TYPE_LDAP = "ldap";
    public static final String SECURITY_INTEGRATION_PROPERTY_TYPE_KEY = "type";
    @SerializedName(value = "n")
    protected String name;
    /**
     * Properties describe this integration.
     */
    @SerializedName(value = "m")
    protected Map<String, String> propertyMap;

    SecurityIntegration(String name, Map<String, String> propertyMap) {
        this.name = name;
        this.propertyMap = propertyMap;
    }

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }

    public String getComment() {
        return propertyMap.getOrDefault("comment", "");
    }

    public String getType() {
        return propertyMap.get(SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
    }

    public abstract AuthenticationProvider getAuthenticationProvider() throws AuthenticationException;

    public Map<String, String> getPropertyMapWithMasking() {
        return propertyMap;
    }
}
