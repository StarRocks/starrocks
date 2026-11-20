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

package com.starrocks.datacache;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class DataCacheSelectSubContextTest {

    @Test
    public void testSubContextKeepsCallerRolesAndGroups() {
        // The sub task runs its INSERT with its own privilege check, so it must see the caller's
        // groups: a role granted to a group, or a Ranger policy written for a group, would otherwise
        // be missing there although the CACHE SELECT itself passed the check.
        ConnectContext caller = new ConnectContext();
        caller.setCurrentUserIdentity(new UserIdentity("cache_user", "%"));
        caller.setCurrentRoleIds(new HashSet<>(Set.of(10L)));
        caller.setGroups(new HashSet<>(Set.of("analysts")));
        caller.setQueryId(UUID.randomUUID());

        ConnectContext sub = DataCacheSelectExecutor.buildCacheSelectConnectContext(
                Mockito.mock(DataCacheSelectStatement.class), caller, true);

        Assertions.assertEquals(caller.getCurrentUserIdentity(), sub.getCurrentUserIdentity());
        Assertions.assertEquals(Set.of(10L), sub.getCurrentRoleIds());
        Assertions.assertEquals(Set.of("analysts"), sub.getGroups());
    }
}
