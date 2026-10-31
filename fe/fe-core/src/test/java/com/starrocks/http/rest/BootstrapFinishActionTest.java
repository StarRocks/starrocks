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

package com.starrocks.http.rest;

import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BootstrapFinishActionTest {

    @Test
    public void testReportsLakeIndexPayloadRebuild(@Mocked GlobalStateMgr globalStateMgr,
                                                   @Mocked NodeMgr nodeMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isReady();
                result = true;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                nodeMgr.getToken();
                result = "cluster-token";
            }
        };
        BaseRequest request = mock(BaseRequest.class);
        when(request.getSingleParameter("token")).thenReturn("cluster-token");
        BaseResponse response = new BaseResponse();

        new BootstrapFinishAction(null) {
            @Override
            public void sendResult(BaseRequest request, BaseResponse response) {
                // Leave the reply in the response for the test to read.
            }
        }.executeWithoutPassword(request, response);

        BootstrapFinishAction.BootstrapResult result =
                BootstrapFinishAction.BootstrapResult.fromJson(response.getContent().toString());
        assertEquals(ActionStatus.OK, result.status);
        // The leader's heartbeat to this FE reads it: the FE rebuilds the payload of a lake index fast-path job it
        // takes over.
        assertTrue(result.rebuildsLakeIndexPayload());
    }
}
