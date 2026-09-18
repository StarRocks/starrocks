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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class RangerStarRocksAccessControllerEPackTest {

    @BeforeAll
    public static void beforeClass() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }

            // Ranger denies every request: only the root short-circuit can let a check through.
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };
    }

    private static ConnectContext contextOf(UserIdentity user) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(user);
        context.setGroups(Set.of());
        return context;
    }

    @Test
    public void testRootBypassesRangerOnWarehouse() {
        RangerStarRocksAccessControllerEPack controller = new RangerStarRocksAccessControllerEPack();

        // RESUME/ALTER WAREHOUSE issued by root must not be blocked by Ranger,
        // even when no Ranger policy grants root anything on the warehouse resource.
        ConnectContext root = contextOf(UserIdentity.ROOT);
        Assertions.assertDoesNotThrow(() ->
                controller.checkWarehouseAction(root, "default_warehouse", PrivilegeType.ALTER));
        Assertions.assertDoesNotThrow(() ->
                controller.checkAnyActionOnWarehouse(root, "default_warehouse"));

        // Everyone else still goes through Ranger and is denied.
        ConnectContext alice = contextOf(new UserIdentity("alice", "%"));
        Assertions.assertThrows(AccessDeniedException.class, () ->
                controller.checkWarehouseAction(alice, "default_warehouse", PrivilegeType.ALTER));
        Assertions.assertThrows(AccessDeniedException.class, () ->
                controller.checkAnyActionOnWarehouse(alice, "default_warehouse"));
    }
}
