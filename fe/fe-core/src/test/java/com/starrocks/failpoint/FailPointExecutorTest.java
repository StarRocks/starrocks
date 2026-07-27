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

package com.starrocks.failpoint;

import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.proto.FailPointTriggerModeType;
import com.starrocks.proto.PFailPointInfo;
import com.starrocks.proto.PFailPointTriggerMode;
import com.starrocks.proto.PListFailPointResponse;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.proto.PUpdateFailPointStatusResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PListFailPointRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowFailPointStatement;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class FailPointExecutorTest {
    private SystemInfoService service;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() {
        service = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();
        Deencapsulation.setField(nodeMgr, "systemInfo", service);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };
    }

    private static ComputeNode aliveCn(long id, String host, int bePort, int brpcPort) {
        ComputeNode cn = new ComputeNode(id, host, 9050);
        cn.setBePort(bePort);
        cn.setBrpcPort(brpcPort);
        cn.setAlive(true);
        return cn;
    }

    private static Backend aliveBe(long id, String host, int bePort, int brpcPort) {
        Backend be = new Backend(id, host, 9050);
        be.setBePort(bePort);
        be.setBrpcPort(brpcPort);
        be.setAlive(true);
        return be;
    }

    @Test
    public void testResolveAllAliveBackendAndComputeNodes() throws Exception {
        service.addBackend(aliveBe(10001, "127.0.0.1", 9060, 8060));
        service.addComputeNode(aliveCn(10002, "127.0.0.2", 9060, 8060));

        ComputeNode deadCn = aliveCn(10003, "127.0.0.3", 9060, 8060);
        deadCn.setAlive(false);
        service.addComputeNode(deadCn);

        List<ComputeNode> nodes = FailPointExecutor.resolveNodes(service, null);
        Assertions.assertEquals(2, nodes.size());
        Assertions.assertEquals(
                Arrays.asList(10001L, 10002L),
                nodes.stream().map(ComputeNode::getId).sorted().collect(Collectors.toList()));
    }

    @Test
    public void testResolveExplicitComputeNodeAddr() throws Exception {
        service.addComputeNode(aliveCn(10002, "10.0.0.2", 9060, 8060));

        List<ComputeNode> nodes = FailPointExecutor.resolveNodes(service,
                Collections.singletonList("10.0.0.2:9060"));
        Assertions.assertEquals(1, nodes.size());
        Assertions.assertEquals(10002L, nodes.get(0).getId());
    }

    @Test
    public void testResolveEmptyClusterThrows() {
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> FailPointExecutor.resolveNodes(service, null));
        Assertions.assertTrue(e.getMessage().contains("No alive backends or compute nodes"));
    }

    @Test
    public void testResolveUnknownAddrThrows() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> FailPointExecutor.resolveNodes(service,
                        Collections.singletonList("10.0.0.9:9060")));
        Assertions.assertTrue(e.getMessage().contains("cannot find backend or compute node"));
    }

    @Test
    public void testResolveInvalidAddrThrows() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> FailPointExecutor.resolveNodes(service, Collections.singletonList("bad-addr")));
        Assertions.assertTrue(e.getMessage().contains("invalid backend addr"));
    }

    @Test
    public void testUpdateBackendFailPointSuccess() throws Exception {
        service.addComputeNode(aliveCn(10002, "10.0.0.2", 9060, 8060));

        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PUpdateFailPointStatusResponse> updateFailPointStatusAsync(
                    TNetworkAddress address, PUpdateFailPointStatusRequest request) {
                PUpdateFailPointStatusResponse resp = new PUpdateFailPointStatusResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = TStatusCode.OK.getValue();
                return CompletableFuture.completedFuture(resp);
            }
        };

        UpdateFailPointStatusStatement stmt = new UpdateFailPointStatusStatement(
                "fp_test", true, Collections.singletonList("10.0.0.2:9060"), NodePosition.ZERO);
        new FailPointExecutor(stmt).execute();
    }

    @Test
    public void testUpdateBackendFailPointRpcError() {
        service.addComputeNode(aliveCn(10002, "10.0.0.2", 9060, 8060));

        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PUpdateFailPointStatusResponse> updateFailPointStatusAsync(
                    TNetworkAddress address, PUpdateFailPointStatusRequest request) throws RpcException {
                throw new RpcException("rpc failed");
            }
        };

        UpdateFailPointStatusStatement stmt = new UpdateFailPointStatusStatement(
                "fp_test", true, Collections.singletonList("10.0.0.2:9060"), NodePosition.ZERO);
        Assertions.assertThrows(DdlException.class, () -> new FailPointExecutor(stmt).execute());
    }

    @Test
    public void testUpdateBackendFailPointStatusError() {
        service.addComputeNode(aliveCn(10002, "10.0.0.2", 9060, 8060));

        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PUpdateFailPointStatusResponse> updateFailPointStatusAsync(
                    TNetworkAddress address, PUpdateFailPointStatusRequest request) {
                PUpdateFailPointStatusResponse resp = new PUpdateFailPointStatusResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                resp.status.errorMsgs = Collections.singletonList("boom");
                return CompletableFuture.completedFuture(resp);
            }
        };

        UpdateFailPointStatusStatement stmt = new UpdateFailPointStatusStatement(
                "fp_test", true, Collections.singletonList("10.0.0.2:9060"), NodePosition.ZERO);
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> new FailPointExecutor(stmt).execute());
        Assertions.assertTrue(e.getMessage().contains("update failPoint status failed"));
    }

    @Test
    public void testShowFailPointsOnComputeNode() throws Exception {
        service.addComputeNode(aliveCn(10002, "10.0.0.2", 9060, 8060));

        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PListFailPointResponse> listFailPointAsync(
                    TNetworkAddress address, PListFailPointRequest request) {
                PFailPointTriggerMode mode = new PFailPointTriggerMode();
                mode.mode = FailPointTriggerModeType.ENABLE;
                PFailPointInfo info = new PFailPointInfo();
                info.name = "fp_cn";
                info.triggerMode = mode;

                PListFailPointResponse resp = new PListFailPointResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = TStatusCode.OK.getValue();
                resp.failPoints = Collections.singletonList(info);
                return CompletableFuture.completedFuture(resp);
            }
        };

        ShowFailPointStatement stmt = new ShowFailPointStatement(
                null, Collections.singletonList("10.0.0.2:9060"), NodePosition.ZERO);
        ShowResultSet result = ShowExecutor.ShowExecutorVisitor.getInstance()
                .visit(stmt, new ConnectContext());
        Assertions.assertTrue(result.next());
        Assertions.assertEquals("fp_cn", result.getString(0));
        Assertions.assertEquals("10.0.0.2:9060", result.getString(3));
    }

    @Test
    public void testShowFailPointsEmptyClusterThrows() {
        ShowFailPointStatement stmt = new ShowFailPointStatement(null, null, NodePosition.ZERO);
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> ShowExecutor.ShowExecutorVisitor.getInstance()
                        .visit(stmt, new ConnectContext()));
        Assertions.assertTrue(e.getMessage().contains("No alive backends or compute nodes"));
    }
}
