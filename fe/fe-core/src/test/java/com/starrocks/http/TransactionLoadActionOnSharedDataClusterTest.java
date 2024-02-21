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

package com.starrocks.http;

import com.starrocks.common.DdlException;
import com.starrocks.http.rest.TransactionLoadAction;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

public class TransactionLoadActionOnSharedDataClusterTest extends TransactionLoadActionTest {

    private static HttpServer beServer;
    private static int TEST_HTTP_PORT = 0;

    @Override
    protected void doSetUp() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        ComputeNode computeNode = new ComputeNode(1234, "localhost", 8040);
        computeNode.setBePort(9300);
        computeNode.setAlive(true);
        computeNode.setHttpPort(TEST_HTTP_PORT);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(computeNode);
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        new MockUp<TransactionLoadAction>() {

            @Mock
            public void redirectTo(BaseRequest request,
                                   BaseResponse response,
                                   TNetworkAddress addr) throws DdlException {
                TransactionResult result = new TransactionResult();
                result.setOKMsg("mock redirect to BE");
                response.setContentType(JSON.toString());
                response.appendContent(result.toJson());
                writeResponse(request, response);
            }

        };
    }

    /**
     * we need close be server after junit test
     */
    @AfterClass
    public static void close() {
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .dropComputeNode(new ComputeNode(1234, "localhost", HTTP_PORT));
        beServer.shutDown();
    }

    @BeforeClass
    public static void initBeServer() throws Exception {
        TEST_HTTP_PORT = detectUsableSocketPort();
        beServer = new HttpServer(TEST_HTTP_PORT);
        BaseAction ac = new BaseAction(beServer.getController()) {

            @Override
            public void execute(BaseRequest request, BaseResponse response) {
                TransactionResult resp = new TransactionResult();
                response.appendContent(resp.toJson());
                writeResponse(request, response, HttpResponseStatus.OK);
            }
        };

        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/begin", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/prepare", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/commit", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/rollback", ac);
        beServer.start();
        // must ensure the http server started before any unit test
        while (!beServer.isStarted()) {
            Thread.sleep(500);
        }
        assertTrue(beServer.isStarted());
    }
}
