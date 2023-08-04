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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Define a mapping that maps groups of users in ldap to role in StarRocks.
 */
public class LDAPRoleMapping extends RoleMapping {
    public static final String ROLE_MAPPING_PROPERTY_GROUP_LIST_KEY = "ldap_group_list";

    /**
     * the dn of groups in ldap
     */
    @SerializedName("gl")
    private Set<String> groupSet;

    public LDAPRoleMapping(String name, Map<String, String> propertyMap, String integrationType) {
        super(name, propertyMap, integrationType);
        this.groupSet = Arrays.stream(propertyMap.get(ROLE_MAPPING_PROPERTY_GROUP_LIST_KEY).split(";"))
                .map(String::trim)
                .collect(Collectors.toSet());
    }

    public Set<String> getGroupSet() {
        return groupSet;
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = super.getProperties();
        properties.put(ROLE_MAPPING_PROPERTY_GROUP_LIST_KEY, String.join(";", groupSet));

        return properties;
    }

    @Override
    public String toString() {
        return "name: " + name + ", integration name: " + integrationName +
                ", role name: " + roleName + ", mapped groups: " + groupSet;
    }
}
