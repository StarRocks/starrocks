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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivObjNotFoundException;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeCollectionV2;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class PrivilegeActionsEPack {
    private static final Logger LOG = LogManager.getLogger(PrivilegeActions.class);

    public static boolean checkPolicyAction(ConnectContext context, PolicyType policyType,
                                            String catalogName, String db, String policy, PrivilegeType privilegeType) {
        UserIdentity userIdentity = context.getCurrentUserIdentity();
        Set<Long> roleIds = context.getCurrentRoleIds();
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);

        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ?
                ObjectTypeEPack.MASKING_POLICY : ObjectTypeEPack.ROW_ACCESS_POLICY;
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(userIdentity, roleIds);
            PEntryObject object = PolicyPEntryObject.generate(GlobalStateMgr.getCurrentState(), policyType, objectTokens);

            return manager.getProvider().check(objectType, privilegeType, object, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on {} {}, message: {}",
                    privilegeType, objectType.name().replace("_", " "),
                    Joiner.on(".").join(objectTokens), e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on {} {}",
                    privilegeType, objectType.name().replace("_", " "),
                    Joiner.on(".").join(objectTokens), e);
            return false;
        }
    }

    public static boolean checkAnyActionOnPolicy(ConnectContext context, PolicyType policyType,
                                                 String catalogName, String db, String policy) {
        UserIdentity currentUser = context.getCurrentUserIdentity();
        Set<Long> roleIds = context.getCurrentRoleIds();
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);

        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ?
                ObjectTypeEPack.MASKING_POLICY : ObjectTypeEPack.ROW_ACCESS_POLICY;
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject pEntryObject = PolicyPEntryObject.generate(GlobalStateMgr.getCurrentState(), policyType, objectTokens);
            return manager.getProvider().searchAnyActionOnObject(objectType, pEntryObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on {} {}, message: {}",
                    objectType.name(), Joiner.on(".").join(objectTokens), e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on {} {}",
                    objectType.name(), Joiner.on(".").join(objectTokens), e);
            return false;
        }
    }
}
