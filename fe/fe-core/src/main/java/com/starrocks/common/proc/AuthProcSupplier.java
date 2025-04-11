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

package com.starrocks.common.proc;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authorization.ActionSet;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeEntry;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthProcSupplier {
    private static final Logger LOG = LogManager.getLogger(AuthProcSupplier.class);
    private final AuthenticationMgr authenticationMgr;
    private final AuthorizationMgr authorizationMgr;

    private static final ObjectType[] AUTH_INFO_OBJECT_TYPES = {
            ObjectType.SYSTEM,
            ObjectType.CATALOG,
            ObjectType.DATABASE,
            ObjectType.TABLE,
            ObjectType.RESOURCE
    };

    private static final Map<ObjectType, String> OBJECT_TYPE_TO_PRIVS_NAME = Map.of(
            ObjectType.SYSTEM, "GlobalPrivs",
            ObjectType.CATALOG, "CatalogPrivs",
            ObjectType.DATABASE, "DatabasePrivs",
            ObjectType.TABLE, "TablePrivs",
            ObjectType.RESOURCE, "ResourcePrivs"
    );

    private static final PrivilegeType[] COMMON_PRIVILEGE_TYPES = {
            PrivilegeType.SELECT,
            PrivilegeType.INSERT,
            PrivilegeType.UPDATE,
            PrivilegeType.DELETE,
            PrivilegeType.ALTER,
            PrivilegeType.CREATE_TABLE,
            PrivilegeType.DROP,
            PrivilegeType.USAGE,
            PrivilegeType.CREATE_DATABASE,
            PrivilegeType.GRANT
    };

    public AuthProcSupplier(AuthenticationMgr authenticationMgr, AuthorizationMgr authorizationMgr) {
        this.authenticationMgr = authenticationMgr;
        this.authorizationMgr = authorizationMgr;
    }

    public List<List<String>> getAuthInfo() {
        List<List<String>> rows = new ArrayList<>();
        Map<UserIdentity, UserAuthenticationInfo> userMap = authenticationMgr.getUserToAuthenticationInfo();

        for (Map.Entry<UserIdentity, UserAuthenticationInfo> entry : userMap.entrySet()) {
            UserIdentity userIdentity = entry.getKey();
            UserAuthenticationInfo authInfo = entry.getValue();

            if (authInfo == null) {
                continue;
            }

            List<String> row = new ArrayList<>();
            row.add(userIdentity.toString());
            // Password column shown as ********
            row.add("********");
            row.add(authInfo.getAuthPlugin());

            try {
                row.add(getRoleNamesForUser(userIdentity).toString());

                for (ObjectType objectType : AUTH_INFO_OBJECT_TYPES) {
                    row.add(getPrivilegesString(userIdentity, objectType));
                }
            } catch (Exception e) {
                LOG.error("Unexpected error while processing auth info for user {}: {}", userIdentity, e.getMessage(), e);
                // Add empty values for all fields in case of exception
                row.add("[]");
                for (int i = 0; i < AUTH_INFO_OBJECT_TYPES.length; i++) {
                    row.add("[]");
                }
            }

            rows.add(row);
        }

        return rows;
    }

    private List<String> getRoleNamesForUser(UserIdentity userIdentity) {
        try {
            return authorizationMgr.getRoleNamesByUser(userIdentity);
        } catch (PrivilegeException e) {
            LOG.error("Error fetching roles for user {}: {}", userIdentity, e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Gets a formatted string representation of the privileges a user has for a specific object type.
     * For example: 
     * - For a table: ["db1.table1(SELECT,INSERT)", "db2.table2(SELECT)"]
     * - For a database: ["db1(CREATE_TABLE,DROP)", "db2(CREATE_TABLE)"]
     * - For all tables: ["ALL TABLES(SELECT,INSERT)"]
     */
    private String getPrivilegesString(UserIdentity userIdentity, ObjectType objectType) {
        try {
            Map<ObjectType, List<PrivilegeEntry>> privilegeMap =
                    authorizationMgr.getTypeToPrivilegeEntryListByUser(userIdentity);
            List<PrivilegeEntry> privilegeEntries = privilegeMap.get(objectType);

            if (privilegeEntries == null || privilegeEntries.isEmpty()) {
                return "[]";
            }

            List<String> formattedEntries = privilegeEntries.stream()
                    .map(entry -> {
                        if (entry == null) {
                            return "null";
                        }

                        StringBuilder sb = new StringBuilder();
                        // Get object name (table name, database name, etc.)
                        if (entry.getObject() != null) {
                            sb.append(entry.getObject().toString());
                        } else {
                            sb.append("ALL");
                        }

                        // Get privilege types (SELECT, INSERT, etc.)
                        if (entry.getActionSet() != null) {
                            sb.append("(");

                            List<String> privTypes = extractPrivilegeTypes(entry.getActionSet());
                            if (!privTypes.isEmpty()) {
                                sb.append(String.join(",", privTypes));
                            } else {
                                sb.append("privilege");
                            }

                            if (entry.isWithGrantOption()) {
                                sb.append(" WITH GRANT OPTION");
                            }

                            sb.append(")");
                        }

                        return sb.toString();
                    })
                    .toList();

            return formattedEntries.toString();
        } catch (Exception e) {
            LOG.error("Unexpected error retrieving privileges for user {} and objectType {}: {}",
                    userIdentity, objectType, e.getMessage(), e);
            return "[]";
        }
    }

    private List<String> extractPrivilegeTypes(ActionSet actionSet) {
        List<String> privTypes = new ArrayList<>();

        try {
            for (PrivilegeType privType : COMMON_PRIVILEGE_TYPES) {
                if (actionSet.contains(privType)) {
                    privTypes.add(privType.toString());
                }
            }
        } catch (Exception e) {
            LOG.error("Error extracting privilege types from actionSet: {}", e.getMessage(), e);
        }

        return privTypes;
    }

    public List<List<String>> getUserPropertyInfo(UserIdentity userIdentity) {
        List<List<String>> rows = new ArrayList<>();
        UserAuthenticationInfo authInfo =
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);

        if (authInfo == null) {
            return rows;
        }

        addProperty(rows, "UserIdentity", userIdentity.toString());
        addProperty(rows, "AuthPlugin", authInfo.getAuthPlugin());

        try {
            List<String> roleNames = getRoleNamesForUser(userIdentity);
            addProperty(rows, "Roles", roleNames.toString());

            for (ObjectType objectType : AUTH_INFO_OBJECT_TYPES) {
                String propertyName = OBJECT_TYPE_TO_PRIVS_NAME.get(objectType);
                addProperty(rows, propertyName, getPrivilegesString(userIdentity, objectType));
            }

            Set<Long> defaultRoleIds = authorizationMgr.getDefaultRoleIdsByUser(userIdentity);
            List<String> defaultRoleNames = authorizationMgr.getRoleNamesByRoleIds(defaultRoleIds);
            addProperty(rows, "DefaultRoles", defaultRoleNames.toString());
        } catch (Exception e) {
            LOG.error("Unexpected error while getting user property info for {}: {}", userIdentity, e.getMessage(), e);
        }

        return rows;
    }

    private void addProperty(List<List<String>> rows, String property, String value) {
        List<String> row = new ArrayList<>();
        row.add(property);
        row.add(value != null ? value : "");
        rows.add(row);
    }
} 