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

package com.starrocks.epack.authorization.ranger.starrocks;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.RangerStarRocksAccessRequest;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.epack.authorization.AccessControllerEPack;
import com.starrocks.epack.authorization.DbUID;
import com.starrocks.epack.authorization.Policy;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.authorization.ranger.RangerKerberosAuth;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import java.util.Map;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerStarRocksAccessControllerEPack extends RangerStarRocksAccessController implements AccessControllerEPack {
    @Override
    protected RangerPluginConfig buildRangerPluginContext(String serviceType, String serviceName) {
        return RangerKerberosAuth.buildKerberosRangerPluginContext(serviceType, serviceName);
    }

    @Override
    public void checkPolicyAction(ConnectContext context, PolicyType policyType, String catalogName,
                                  String db, String policy, PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResourceEPack resource =
                RangerStarRocksResourceEPack.makePolicyResource(policyType, catalogName, db, policy);
        hasPermission(resource, context.getCurrentUserIdentity(), context.getGroups(), privilegeType);
    }

    @Override
    public void checkAnyActionOnPolicy(ConnectContext context, PolicyType policyType, String catalogName,
                                       String db, String policy) throws AccessDeniedException {
        RangerStarRocksResourceEPack resource =
                RangerStarRocksResourceEPack.makePolicyResource(policyType, catalogName, db, policy);
        hasPermission(resource, context.getCurrentUserIdentity(), context.getGroups(), PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyPolicy(ConnectContext context, PolicyType policyType, String catalogName,
                                          String db) throws AccessDeniedException {
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Map<String, Policy> policyMap = securityPolicyMgr
                .getOrCreateNamePolicyMapByDBUID(DbUID.generate(catalogName, db), policyType);
        RangerStarRocksResourceEPack resource;
        for (Policy policy : policyMap.values()) {
            resource = RangerStarRocksResourceEPack.makePolicyResource(policyType, catalogName, db, policy.getName());
            try {
                hasPermission(resource, context.getCurrentUserIdentity(), context.getGroups(), PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }

        throw new AccessDeniedException();
    }

    @Override
    public void checkWarehouseAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResourceEPack resource = RangerStarRocksResourceEPack.makeWarehouseResource(name);
        hasPermission(resource, context.getCurrentUserIdentity(), context.getGroups(), privilegeType);
    }

    @Override
    public void checkAnyActionOnWarehouse(ConnectContext context, String name) throws AccessDeniedException {
        RangerStarRocksResourceEPack resource = RangerStarRocksResourceEPack.makeWarehouseResource(name);
        hasPermission(resource, context.getCurrentUserIdentity(), context.getGroups(), PrivilegeType.ANY);
    }

    @Override
    public void checkFailoverGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResourceEPack resource = RangerStarRocksResourceEPack.makeFailoverGroup(name);
        hasPermission(resource, context.getCurrentUserIdentity(), context.getGroups(), privilegeType);
    }

    @Override
    public void checkAnyActionOnFailoverGroup(ConnectContext context, String name)
            throws AccessDeniedException {
        RangerStarRocksResourceEPack resource = RangerStarRocksResourceEPack.makeFailoverGroup(name);
        hasPermission(resource, context.getCurrentUserIdentity(), context.getGroups(), PrivilegeType.ANY);
    }

    private void hasPermission(RangerStarRocksResourceEPack resource, UserIdentity user, Set<String> groups,
                               PrivilegeType privilegeType)
            throws AccessDeniedException {
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            accessType = privilegeType.name().toLowerCase(ENGLISH);
        }

        RangerStarRocksAccessRequest request =
                RangerStarRocksAccessRequest.createAccessRequest(resource, user, groups, accessType);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);

        if (result == null || !result.getIsAllowed()) {
            throw new AccessDeniedException();
        }
    }
}
