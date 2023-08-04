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


package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;

/**
 * A role mapping specifies the mappings for attribute of users, like group, org etc. in a security integration
 * to the role in StarRocks, so that when an external user login, we can decide what roles we should assign to it
 * based on the mapping.
 */
public class RoleMapping {
    public static final String ROLE_MAPPING_PROPERTY_INTEGRATION_NAME_KEY = "integration_name";
    public static final String ROLE_MAPPING_PROPERTY_ROLE_NAME_KEY = "role";

    @SerializedName(value = "n")
    protected String name;
    /**
     * name of the security integration where this mapping is defined on
     */
    @SerializedName("in")
    protected String integrationName;
    @SerializedName("itp")
    protected String integrationType;
    /**
     * name of the role to which the user's attribute will map
     */
    @SerializedName("r")
    protected String roleName;

    public RoleMapping(String name, Map<String, String> propertyMap, String integrationType) {
        this.name = name;
        this.roleName = propertyMap.get(ROLE_MAPPING_PROPERTY_ROLE_NAME_KEY);
        this.integrationName = propertyMap.get(ROLE_MAPPING_PROPERTY_INTEGRATION_NAME_KEY);
        this.integrationType = integrationType;
    }

    public String getIntegrationName() {
        return integrationName;
    }

    public String getName() {
        return name;
    }

    public String getRoleName() {
        return roleName;
    }

    public String getIntegrationType() {
        return integrationType;
    }

    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(ROLE_MAPPING_PROPERTY_INTEGRATION_NAME_KEY, integrationName);
        properties.put(ROLE_MAPPING_PROPERTY_ROLE_NAME_KEY, roleName);

        return properties;
    }

    @Override
    public String toString() {
        return "name: " + name + ", integration name: " + integrationName + ", role name: " + roleName;
    }
}
