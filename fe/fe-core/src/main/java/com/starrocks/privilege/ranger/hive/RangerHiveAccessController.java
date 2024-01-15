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

package com.starrocks.privilege.ranger.hive;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.RangerAccessController;
import com.starrocks.privilege.ranger.RangerStarRocksAccessRequest;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerHiveAccessController extends RangerAccessController {
    public RangerHiveAccessController(String serviceName) {
        super("hive", serviceName);
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.DATABASE, Lists.newArrayList(db));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.DATABASE, Lists.newArrayList(db));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.TABLE,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.TABLE,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    public HiveAccessType convertToAccessType(PrivilegeType privilegeType) {
        if (privilegeType == PrivilegeType.SELECT) {
            return HiveAccessType.SELECT;
        } else if (privilegeType == PrivilegeType.INSERT) {
            return HiveAccessType.UPDATE;
        } else if (privilegeType == PrivilegeType.CREATE_DATABASE
                || privilegeType == PrivilegeType.CREATE_TABLE) {
            return HiveAccessType.CREATE;
        } else if (privilegeType == PrivilegeType.DROP) {
            return HiveAccessType.DROP;
        } else {
            return HiveAccessType.NONE;
        }
    }

    private void hasPermission(RangerHiveResource resource, UserIdentity user, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            HiveAccessType hiveAccessType = convertToAccessType(privilegeType);
            accessType = hiveAccessType.name().toLowerCase(ENGLISH);
        }

        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(resource, user, accessType);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);

        if (result == null || !result.getIsAllowed()) {
            throw new AccessDeniedException();
        }
    }
}
