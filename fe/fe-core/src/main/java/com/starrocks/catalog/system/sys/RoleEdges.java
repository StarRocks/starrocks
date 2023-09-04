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
package com.starrocks.catalog.system.sys;

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.RolePrivilegeCollectionV2;
import com.starrocks.privilege.UserPrivilegeCollectionV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TGetRoleEdgesItem;
import com.starrocks.thrift.TGetRoleEdgesRequest;
import com.starrocks.thrift.TGetRoleEdgesResponse;
import com.starrocks.thrift.TSchemaTableType;

import java.util.List;
import java.util.Set;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class RoleEdges {
    public static SystemTable create() {
        return new SystemTable(SystemId.ROLE_EDGES_ID, "role_edges", Table.TableType.SCHEMA,
                builder()
                        .column("FROM_ROLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TO_ROLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TO_USER", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_ROLE_EDGES);
    }

    public static TGetRoleEdgesResponse getRoleEdges(TGetRoleEdgesRequest request) {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        TGetRoleEdgesResponse tGetRoleEdgesResponse = new TGetRoleEdgesResponse();

        List<String> allRoles = authorizationManager.getAllRoles();
        for (String roleName : allRoles) {
            RolePrivilegeCollectionV2 rolePrivilegeCollection =
                    authorizationManager.getRolePrivilegeCollection(roleName);
            if (rolePrivilegeCollection == null) {
                continue;
            }
            Set<Long> parentRoleIds = rolePrivilegeCollection.getParentRoleIds();
            for (Long parentRoleId : parentRoleIds) {
                RolePrivilegeCollectionV2 parentRoleCollection =
                        authorizationManager.getRolePrivilegeCollection(parentRoleId);
                if (parentRoleCollection == null) {
                    continue;
                }
                TGetRoleEdgesItem tGetRoleEdgesItem = new TGetRoleEdgesItem();
                tGetRoleEdgesItem.setFrom_role(parentRoleCollection.getName());
                tGetRoleEdgesItem.setTo_role(roleName);
                tGetRoleEdgesResponse.addToRole_edges(tGetRoleEdgesItem);
            }
        }

        Set<UserIdentity> allUsers = authorizationManager.getAllUserIdentities();
        for (UserIdentity userIdentity : allUsers) {
            UserPrivilegeCollectionV2 userPrivilegeCollection =
                    authorizationManager.getUserPrivilegeCollection(userIdentity);
            if (userPrivilegeCollection == null) {
                continue;
            }
            Set<Long> parentRoleIds = userPrivilegeCollection.getAllRoles();
            for (Long parentRoleId : parentRoleIds) {
                RolePrivilegeCollectionV2 parentRoleCollection =
                        authorizationManager.getRolePrivilegeCollection(parentRoleId);
                if (parentRoleCollection == null) {
                    continue;
                }
                TGetRoleEdgesItem tGetRoleEdgesItem = new TGetRoleEdgesItem();
                tGetRoleEdgesItem.setFrom_role(parentRoleCollection.getName());
                tGetRoleEdgesItem.setTo_user(userIdentity.toString());
                tGetRoleEdgesResponse.addToRole_edges(tGetRoleEdgesItem);
            }
        }

        return tGetRoleEdgesResponse;
    }
}
