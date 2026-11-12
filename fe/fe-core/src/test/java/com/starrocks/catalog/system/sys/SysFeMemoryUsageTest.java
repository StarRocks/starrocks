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
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.memory.MemoryStat;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeMemoryReq;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                Authorizer.checkSystemAction((ConnectContext) any, (PrivilegeType) any);
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

    @Test
    public void testListFeMemoryUsageCollectsInsteadOfReadingWhatIsAlreadyThere()
            throws TException, AccessDeniedException {
        // The test above plants an entry in MEMORY_USAGE before asking, so it passes whether or
        // not the table can produce anything by itself. This one plants nothing.
        //
        // Nothing fills MEMORY_USAGE on a timer: the periodic task calls estimateCount() and logs
        // it, and estimateSize() -- the half that writes the map -- moved to the collect path when
        // /api/memory_usage was added. That endpoint answers 127.0.0.1 only, so on a server nobody
        // has curled from, reading the map answers an empty table.
        TFeMemoryReq req = new TFeMemoryReq();
        TAuthInfo auth = new TAuthInfo();
        auth.setUser("root");
        auth.setUser_ip("127.0.0.1");
        req.setAuth_info(auth);

        AccessControlProvider accessControlProvider = Authorizer.getInstance();
        new Expectations(accessControlProvider) {
            {
                Authorizer.checkSystemAction((ConnectContext) any, (PrivilegeType) any);
                result = null;
                minTimes = 0;
            }
        };

        MemoryUsageTracker.registerMemoryTracker("SysFeMemoryUsageTestModule", new MemoryTrackable() {
            @Override
            public long estimateSize() {
                return 4096;
            }

            @Override
            public Map<String, Long> estimateCount() {
                return ImmutableMap.of("Widget", 7L);
            }
        });
        MemoryUsageTracker.MEMORY_USAGE.clear();

        var res = SysFeMemoryUsage.listFeMemoryUsage(req);

        assertNotNull(res.getItems(), "the table must collect when it has nothing to read");
        var item = res.getItems().stream()
                .filter(each -> "SysFeMemoryUsageTestModule".equals(each.getModule_name()))
                .findFirst()
                .orElse(null);
        assertNotNull(item, "registered tracker should appear without anyone calling the endpoint");
        assertEquals(4096, item.getCurrent_consumption());
        assertTrue(item.getCounter_info().contains("Widget"), item.getCounter_info());
    }

    @Test
    public void testCollectionIsSharedRatherThanRepeatedPerQuery()
            throws TException, AccessDeniedException {
        // A collection walks every database under its READ lock (LocalMetastore#estimateSize) and
        // every entry of the iceberg file caches, so one per scan would have every session that
        // reads this table taking its own pass over every database lock, contending with DDL.
        // Callers inside the window read the snapshot the last one left.
        TFeMemoryReq req = new TFeMemoryReq();
        TAuthInfo auth = new TAuthInfo();
        auth.setUser("root");
        auth.setUser_ip("127.0.0.1");
        req.setAuth_info(auth);

        AccessControlProvider accessControlProvider = Authorizer.getInstance();
        new Expectations(accessControlProvider) {
            {
                Authorizer.checkSystemAction((ConnectContext) any, (PrivilegeType) any);
                result = null;
                minTimes = 0;
            }
        };

        AtomicInteger estimates = new AtomicInteger();
        MemoryUsageTracker.registerMemoryTracker("SysFeMemoryUsageCountingModule", new MemoryTrackable() {
            @Override
            public long estimateSize() {
                estimates.incrementAndGet();
                return 1;
            }

            @Override
            public Map<String, Long> estimateCount() {
                return ImmutableMap.of("Widget", 1L);
            }
        });
        MemoryUsageTracker.MEMORY_USAGE.clear();

        SysFeMemoryUsage.listFeMemoryUsage(req);
        int afterFirst = estimates.get();
        assertTrue(afterFirst > 0, "an empty snapshot must collect");

        SysFeMemoryUsage.listFeMemoryUsage(req);
        SysFeMemoryUsage.listFeMemoryUsage(req);
        assertEquals(afterFirst, estimates.get(), "queries inside the window must reuse the snapshot");
    }

    @Test
    public void testListFeMemoryUsageAccessDeniedSurfacesMessage() {
        TFeMemoryReq req = new TFeMemoryReq();
        TAuthInfo auth = new TAuthInfo();
        auth.setUser("nopriv");
        auth.setUser_ip("127.0.0.1");
        req.setAuth_info(auth);

        new MockUp<Authorizer>() {
            @Mock
            public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
                    throws AccessDeniedException {
                throw new AccessDeniedException();
            }
        };

        Exception ex = assertThrows(Exception.class, () -> SysFeMemoryUsage.listFeMemoryUsage(req));
        assertNotNull(ex.getMessage(), "AccessDenied message should not be null");
        assertTrue(ex.getMessage().contains(
                        "Access denied; you need (at least one of) the OPERATE privilege(s) on SYSTEM for this operation."),
                "Should match canonical format: " + ex.getMessage());
    }

}