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

import com.google.common.collect.Lists;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class PrivilegeActionsEPack extends PrivilegeActions {
    private static final Logger LOG = LogManager.getLogger(PrivilegeActions.class);

    public static boolean checkPolicyAction(ConnectContext context, PolicyType policyType,
                                            String catalogName, String db, String policy,
                                            PrivilegeType privilegeType) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ?
                ObjectTypeEPack.MASKING_POLICY : ObjectTypeEPack.ROW_ACCESS_POLICY;
        return checkObjectTypeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                privilegeType, objectType, objectTokens);
    }

    public static boolean checkAnyActionOnPolicy(ConnectContext context, PolicyType policyType,
                                                 String catalogName, String db, String policy) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ?
                ObjectTypeEPack.MASKING_POLICY : ObjectTypeEPack.ROW_ACCESS_POLICY;
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                objectType, objectTokens);
    }

    public static boolean checkWarehouseAction(ConnectContext connectContext, String name,
                                               PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectTypeEPack.WAREHOUSE, Collections.singletonList(name));
    }

    public static boolean checkAnyActionOnWarehouse(ConnectContext context, String name) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                ObjectTypeEPack.WAREHOUSE, Collections.singletonList(name));
    }
}
