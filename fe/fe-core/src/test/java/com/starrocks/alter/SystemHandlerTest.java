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


package com.starrocks.alter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.DecommissionBackendClause;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.Backend;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SystemHandlerTest {

    private SystemHandler systemHandler;
    private GlobalStateMgr globalStateMgr;
    private static FakeEditLog fakeEditLog;
    private static FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() throws Exception {
        fakeEditLog = new FakeEditLog();
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = GlobalStateMgrTestUtil.createTestState();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        systemHandler = new SystemHandler();
    }

    @Test
    public void testModifyBackendAddressLogic() {
        assertThrows(RuntimeException.class, () -> {
            ModifyBackendClause clause = new ModifyBackendClause("127.0.0.1", "sandbox-fqdn");
            List<AlterClause> clauses = new ArrayList<>();
            clauses.add(clause);
            systemHandler.process(clauses, null, null);
        });
    }

    @Test
    public void testModifyFrontendAddressLogic() {
        assertThrows(NullPointerException.class, () -> {
            ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("127.0.0.1", "sandbox-fqdn");
            List<AlterClause> clauses = new ArrayList<>();
            clauses.add(clause);
            systemHandler.process(clauses, null, null);
        });
    }

    @Test
    public void testDecommissionInvalidBackend() {
        List<String> hostAndPorts = Lists.newArrayList("192.168.1.11:1234");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        Throwable exception = assertThrows(RuntimeException.class, () -> {
            systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
        });
        assertThat(exception.getMessage(), containsString("Backend does not exist"));
    }

    @Test
    public void testDecommissionBackendsReplicasRequirement() {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        Throwable exception = assertThrows(RuntimeException.class, () -> {
            systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
        });
        assertThat(exception.getMessage(), containsString("It will cause insufficient BE number"));
    }

    @Test
    public void testDecommissionBackendsSpaceRequirement() {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        DiskInfo diskInfo = new DiskInfo("/data");
        diskInfo.setAvailableCapacityB(100);
        diskInfo.setDataUsedCapacityB(900);
        diskInfo.setTotalCapacityB(1000);
        Map<String, DiskInfo> diskInfoMap = Maps.newHashMap();
        diskInfoMap.put("/data", diskInfo);

        for (Backend backend : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            backend.setDisks(ImmutableMap.copyOf(diskInfoMap));
        }

        Throwable exception = assertThrows(RuntimeException.class, () -> {
            systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
        });
        assertThat(exception.getMessage(), containsString("It will cause insufficient disk space"));
    }

    @Test
    public void testDecommissionBackends() throws StarRocksException {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        Backend backend4 = new Backend(100, "host4", 123);
        backend4.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend4);

        DiskInfo diskInfo = new DiskInfo("/data");
        diskInfo.setAvailableCapacityB(900);
        diskInfo.setDataUsedCapacityB(100);
        diskInfo.setTotalCapacityB(1000);
        Map<String, DiskInfo> diskInfoMap = Maps.newHashMap();
        diskInfoMap.put("/data", diskInfo);

        for (Backend backend : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            backend.setDisks(ImmutableMap.copyOf(diskInfoMap));
        }

        systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
    }
}
