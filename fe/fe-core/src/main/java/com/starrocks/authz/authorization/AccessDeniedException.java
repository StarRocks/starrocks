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

package com.starrocks.authz.authorization;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReportException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AccessDeniedException extends Exception {

    public AccessDeniedException() {
    }

    public AccessDeniedException(String message) {
        super(message);
    }

    public static void reportAccessDenied(String catalog, UserIdentity userIdentity, Set<Long> roleIds, String privilegeType,
                                          String objectType, String object) {
        if (catalog == null) {
            catalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        }

        if (Authorizer.getInstance().getAccessControlOrDefault(catalog) instanceof
                ExternalAccessController) {
            ErrorReportException.report(ErrorCode.ERR_ACCESS_DENIED_FOR_EXTERNAL_ACCESS_CONTROLLER,
                    privilegeType, objectType, object == null ? "" : " " + object);
        } else {
            AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            List<String> activatedRoles = new ArrayList<>();
            if (roleIds != null) {
                for (Long roleId : roleIds) {
                    RolePrivilegeCollectionV2 roleCollection = authorizationMgr.getRolePrivilegeCollection(roleId);
                    if (roleCollection != null) {
                        activatedRoles.add(roleCollection.getName());
                    }
                }
            }

            List<String> inactivatedRoles = new ArrayList<>();
            try {
                inactivatedRoles = authorizationMgr.getRoleNamesByUser(userIdentity);
            } catch (PrivilegeException e) {
                //ignore exception
            }
            inactivatedRoles.removeAll(activatedRoles);

            ErrorReportException.report(ErrorCode.ERR_ACCESS_DENIED, privilegeType, objectType,
                    object == null ? "" : " " + object,
                    activatedRoles.isEmpty() ? "NONE" : activatedRoles, inactivatedRoles.isEmpty() ? "NONE" : inactivatedRoles);
        }
    }
}