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

package com.starrocks.privilege;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum ObjectType {
    TABLE(1, "TABLES", ImmutableList.of(
            PrivilegeType.DELETE,
            PrivilegeType.DROP,
            PrivilegeType.INSERT,
            PrivilegeType.SELECT,
            PrivilegeType.ALTER,
            PrivilegeType.EXPORT,
            PrivilegeType.UPDATE)),

    DATABASE(2, "DATABASES", ImmutableList.of(
            PrivilegeType.CREATE_TABLE,
            PrivilegeType.DROP,
            PrivilegeType.ALTER,
            PrivilegeType.CREATE_VIEW,
            PrivilegeType.CREATE_FUNCTION,
            PrivilegeType.CREATE_MATERIALIZED_VIEW)),

    SYSTEM(3, null, ImmutableList.of(
            PrivilegeType.GRANT,
            PrivilegeType.NODE,
            PrivilegeType.CREATE_RESOURCE,
            PrivilegeType.PLUGIN,
            PrivilegeType.FILE,
            PrivilegeType.BLACKLIST,
            PrivilegeType.OPERATE,
            PrivilegeType.CREATE_EXTERNAL_CATALOG,
            PrivilegeType.REPOSITORY,
            PrivilegeType.CREATE_RESOURCE_GROUP,
            PrivilegeType.CREATE_GLOBAL_FUNCTION)),

    USER(4, "USERS", ImmutableList.of(
            PrivilegeType.IMPERSONATE)),

    RESOURCE(5, "RESOURCES", ImmutableList.of(
            PrivilegeType.USAGE,
            PrivilegeType.ALTER,
            PrivilegeType.DROP)),

    VIEW(6, "VIEWS", ImmutableList.of(
            PrivilegeType.SELECT,
            PrivilegeType.ALTER,
            PrivilegeType.DROP)),

    CATALOG(7, "CATALOGS", ImmutableList.of(
            PrivilegeType.USAGE,
            PrivilegeType.CREATE_DATABASE,
            PrivilegeType.DROP,
            PrivilegeType.ALTER)),

    MATERIALIZED_VIEW(8, "MATERIALIZED_VIEWS", ImmutableList.of(
            PrivilegeType.ALTER,
            PrivilegeType.REFRESH,
            PrivilegeType.DROP,
            PrivilegeType.SELECT)),

    FUNCTION(9, "FUNCTIONS", ImmutableList.of(
            PrivilegeType.USAGE,
            PrivilegeType.DROP)),

    RESOURCE_GROUP(10, "RESOURCE_GROUPS", ImmutableList.of(
            PrivilegeType.ALTER,
            PrivilegeType.DROP)),

    GLOBAL_FUNCTION(11, "GLOBAL_FUNCTIONS", ImmutableList.of(
            PrivilegeType.USAGE,
            PrivilegeType.DROP));

    private final int id;
    /**
     * Action name(ALTER, DROP etc.) -> {@link Action} object
     */
    private final Map<String, Action> actionMap;

    // used in ALL statement, e.g. grant select on all tables in all databases
    private final String plural;

    ObjectType(int id, String plural, List<PrivilegeType> privilegeTypes) {
        this.id = id;
        this.plural = plural;

        Map<String, Action> ret = new HashMap<>();
        for (PrivilegeType privilegeType : privilegeTypes) {
            ret.put(privilegeType.name(), new Action((short) privilegeType.ordinal(), privilegeType.name()));
        }
        this.actionMap = ret;
    }

    public Map<String, Action> getActionMap() {
        return actionMap;
    }

    public String getPlural() {
        return plural;
    }

    public int getId() {
        return id;
    }

    public boolean isAvailablePrivType(String privilegeType) {
        return actionMap.containsKey(privilegeType);
    }
}
