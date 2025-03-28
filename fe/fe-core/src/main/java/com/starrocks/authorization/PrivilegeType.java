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

package com.starrocks.authorization;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PrivilegeType {
    protected final int id;
    protected final String name;

    protected PrivilegeType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String name() {
        if (VALID_PRIVILEGE_TYPE.contains(this)) {
            return name;
        } else if (this.equals(ANY)) {
            return "ANY";
        } else {
            return "UNKNOWN";
        }
    }

    // ANY is not in VALID_PRIVILEGE_TYPE, ANY is not a Privilege Type that users can use directly
    public static final PrivilegeType ANY = new PrivilegeType(0, "ANY");

    public static final PrivilegeType GRANT = new PrivilegeType(1, "GRANT");
    public static final PrivilegeType NODE = new PrivilegeType(2, "NODE");
    public static final PrivilegeType OPERATE = new PrivilegeType(3, "OPERATE");
    public static final PrivilegeType DELETE = new PrivilegeType(4, "DELETE");
    public static final PrivilegeType DROP = new PrivilegeType(5, "DROP");
    public static final PrivilegeType INSERT = new PrivilegeType(6, "INSERT");
    public static final PrivilegeType SELECT = new PrivilegeType(7, "SELECT");
    public static final PrivilegeType ALTER = new PrivilegeType(8, "ALTER");
    public static final PrivilegeType EXPORT = new PrivilegeType(9, "EXPORT");
    public static final PrivilegeType UPDATE = new PrivilegeType(10, "UPDATE");
    public static final PrivilegeType USAGE = new PrivilegeType(11, "USAGE");
    public static final PrivilegeType PLUGIN = new PrivilegeType(12, "PLUGIN");
    public static final PrivilegeType FILE = new PrivilegeType(13, "FILE");
    public static final PrivilegeType BLACKLIST = new PrivilegeType(14, "BLACKLIST");
    public static final PrivilegeType REPOSITORY = new PrivilegeType(15, "REPOSITORY");
    public static final PrivilegeType REFRESH = new PrivilegeType(16, "REFRESH");
    public static final PrivilegeType IMPERSONATE = new PrivilegeType(17, "IMPERSONATE");
    public static final PrivilegeType CREATE_DATABASE = new PrivilegeType(18, "CREATE DATABASE");
    public static final PrivilegeType CREATE_TABLE = new PrivilegeType(19, "CREATE TABLE");
    public static final PrivilegeType CREATE_VIEW = new PrivilegeType(20, "CREATE VIEW");
    public static final PrivilegeType CREATE_FUNCTION = new PrivilegeType(21, "CREATE FUNCTION");
    public static final PrivilegeType CREATE_GLOBAL_FUNCTION = new PrivilegeType(22, "CREATE GLOBAL FUNCTION");
    public static final PrivilegeType CREATE_MATERIALIZED_VIEW = new PrivilegeType(23, "CREATE MATERIALIZED VIEW");
    public static final PrivilegeType CREATE_RESOURCE = new PrivilegeType(24, "CREATE RESOURCE");
    public static final PrivilegeType CREATE_RESOURCE_GROUP = new PrivilegeType(25, "CREATE RESOURCE GROUP");
    public static final PrivilegeType CREATE_EXTERNAL_CATALOG = new PrivilegeType(26, "CREATE EXTERNAL CATALOG");
    public static final PrivilegeType CREATE_STORAGE_VOLUME = new PrivilegeType(27, "CREATE STORAGE VOLUME");
    public static final PrivilegeType CREATE_PIPE = new PrivilegeType(28, "CREATE PIPE");

    /**
     * NOTICE: PrivilegeType cannot use a value exceeding 20000, please follow the above sequence number
     */
    public static final PrivilegeType CREATE_WAREHOUSE = new PrivilegeType(20004, "CREATE WAREHOUSE");

    public static final Set<PrivilegeType> VALID_PRIVILEGE_TYPE = new ImmutableSet.Builder<PrivilegeType>().add(
            GRANT,
            NODE,
            OPERATE,
            DELETE,
            DROP,
            INSERT,
            SELECT,
            ALTER,
            EXPORT,
            UPDATE,
            USAGE,
            PLUGIN,
            FILE,
            BLACKLIST,
            REPOSITORY,
            REFRESH,
            IMPERSONATE,
            CREATE_DATABASE,
            CREATE_TABLE,
            CREATE_VIEW,
            CREATE_FUNCTION,
            CREATE_GLOBAL_FUNCTION,
            CREATE_MATERIALIZED_VIEW,
            CREATE_RESOURCE,
            CREATE_RESOURCE_GROUP,
            CREATE_EXTERNAL_CATALOG,
            CREATE_STORAGE_VOLUME,
            CREATE_PIPE,
            CREATE_WAREHOUSE
    ).build();

    public static final Map<String, PrivilegeType> NAME_TO_PRIVILEGE = VALID_PRIVILEGE_TYPE.stream().collect(Collectors.toMap(
            PrivilegeType::name, Function.identity()));

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrivilegeType that = (PrivilegeType) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
