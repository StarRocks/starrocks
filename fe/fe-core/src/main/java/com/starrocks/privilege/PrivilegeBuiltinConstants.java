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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class  PrivilegeBuiltinConstants {

    public static final String ROOT_ROLE_NAME = "root";
    public static final long ROOT_ROLE_ID = -1;

    public static final String DB_ADMIN_ROLE_NAME = "db_admin";
    public static final long DB_ADMIN_ROLE_ID = -2;

    public static final String CLUSTER_ADMIN_ROLE_NAME = "cluster_admin";
    public static final long CLUSTER_ADMIN_ROLE_ID = -3;

    public static final String USER_ADMIN_ROLE_NAME = "user_admin";
    public static final long USER_ADMIN_ROLE_ID = -4;

    public static final String PUBLIC_ROLE_NAME = "public";
    public static final long PUBLIC_ROLE_ID = -5;

    public static final Set<String> BUILT_IN_ROLE_NAMES =
            new HashSet<>(Arrays.asList(ROOT_ROLE_NAME, DB_ADMIN_ROLE_NAME, CLUSTER_ADMIN_ROLE_NAME,
                    USER_ADMIN_ROLE_NAME, PUBLIC_ROLE_NAME));

    public static final Set<Long> IMMUTABLE_BUILT_IN_ROLE_IDS = new HashSet<>(Arrays.asList(
            ROOT_ROLE_ID, DB_ADMIN_ROLE_ID, CLUSTER_ADMIN_ROLE_ID, USER_ADMIN_ROLE_ID));

    public static final Set<String> IMMUTABLE_BUILT_IN_ROLE_NAMES = new HashSet<>(Arrays.asList(
            ROOT_ROLE_NAME, DB_ADMIN_ROLE_NAME, CLUSTER_ADMIN_ROLE_NAME, USER_ADMIN_ROLE_NAME));

    public static final long ALL_CATALOGS_ID = -1; // -1 represent all catalogs

    public static final String ALL_DATABASES_UUID = "ALL_DATABASES_UUID"; // represent all databases
    public static final long ALL_DATABASE_ID = -2; // -2 represent all databases

    public static final String ALL_TABLES_UUID = "ALL_TABLES_UUID"; // represent all tables

    public static final long ALL_FUNCTIONS_ID = -3; // -3 represent all functions
    public static final long GLOBAL_FUNCTION_DEFAULT_DATABASE_ID = -4; // global function default database id

    public static final long ALL_RESOURCE_GROUP_ID = -1; // -1 represent all resource group
}
