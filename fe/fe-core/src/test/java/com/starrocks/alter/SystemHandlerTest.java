// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.alter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.DecommissionBackendClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.Backend;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SystemHandlerTest {

    private SystemHandler systemHandler;
    private GlobalStateMgr globalStateMgr;
    private static FakeEditLog fakeEditLog;
    private static FakeGlobalStateMgr fakeGlobalStateMgr;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        fakeEditLog = new FakeEditLog();
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = GlobalStateMgrTestUtil.createTestState();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        systemHandler = new SystemHandler();
    }

    @Test(expected = DdlException.class)
    public void testModifyBackendAddressLogic() throws UserException {
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.1", "sandbox-fqdn");
        List<AlterClause> clauses = new ArrayList<>();
        clauses.add(clause);
        systemHandler.process(clauses, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testModifyFrontendAddressLogic() throws UserException {
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("127.0.0.1", "sandbox-fqdn");
        List<AlterClause> clauses = new ArrayList<>();
        clauses.add(clause);
        systemHandler.process(clauses, null, null);
    }

    @Test
    public void testDecommissionInvalidBackend() throws UserException {
        List<String> hostAndPorts = Lists.newArrayList("192.168.1.11:1234");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        expectedException.expect(DdlException.class);
        expectedException.expectMessage("Backend does not exist");
        systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
    }

    @Test
    public void testDecommissionBackendsReplicasRequirement() throws UserException {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        expectedException.expect(DdlException.class);
        expectedException.expectMessage("It will cause insufficient BE number");
        systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
    }

    @Test
    public void testDecommissionBackendsSpaceRequirement() throws UserException {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        DiskInfo diskInfo = new DiskInfo("/data");
        diskInfo.setAvailableCapacityB(100);
        diskInfo.setDataUsedCapacityB(900);
        diskInfo.setTotalCapacityB(1000);
        Map<String, DiskInfo> diskInfoMap = Maps.newHashMap();
        diskInfoMap.put("/data", diskInfo);

        for (Backend backend : GlobalStateMgr.getCurrentSystemInfo().getBackends()) {
            backend.setDisks(ImmutableMap.copyOf(diskInfoMap));
        }

        expectedException.expect(DdlException.class);
        expectedException.expectMessage("It will cause insufficient disk space");
        systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
    }

    @Test
    public void testDecommissionBackends() throws UserException {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        Backend backend4 = new Backend(100, "host4", 123);
        backend4.setAlive(true);
        GlobalStateMgr.getCurrentSystemInfo().addBackend(backend4);

        DiskInfo diskInfo = new DiskInfo("/data");
        diskInfo.setAvailableCapacityB(900);
        diskInfo.setDataUsedCapacityB(100);
        diskInfo.setTotalCapacityB(1000);
        Map<String, DiskInfo> diskInfoMap = Maps.newHashMap();
        diskInfoMap.put("/data", diskInfo);

        for (Backend backend : GlobalStateMgr.getCurrentSystemInfo().getBackends()) {
            backend.setDisks(ImmutableMap.copyOf(diskInfoMap));
        }

        systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
    }
}
