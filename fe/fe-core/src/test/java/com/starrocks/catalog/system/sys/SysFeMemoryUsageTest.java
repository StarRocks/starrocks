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

import com.google.common.collect.ImmutableMap;
import com.starrocks.memory.MemoryStat;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeMemoryReq;
import mockit.Expectations;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SysFeMemoryUsageTest {

    @Test
    public void testListFeMemoryUsage() throws TException, AccessDeniedException {
        TFeMemoryReq req = new TFeMemoryReq();
        TAuthInfo auth = new TAuthInfo();
        auth.setUser("root");
        auth.setUser_ip("127.0.0.1");
        req.setAuth_info(auth);

        AccessControlProvider accessControlProvider = Authorizer.getInstance();
        new Expectations(accessControlProvider) {
            {
                Authorizer.checkSystemAction((UserIdentity) any, (Set<Long>) any, (PrivilegeType) any);
                result = null;
                minTimes = 0;
            }
        };
        Map<String, Map<String, MemoryStat>> memoryUsage = MemoryUsageTracker.MEMORY_USAGE;
        MemoryStat memoryStat = new MemoryStat();
        memoryUsage.put("test", ImmutableMap.of("test", memoryStat));

        var res = SysFeMemoryUsage.listFeMemoryUsage(req);
        assertTrue(StringUtils.isNotEmpty(res.toString()));
    }

}