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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.common.DdlException;
import com.starrocks.http.BaseRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the LoadAction internal-trust-token bypass branch.
 *
 * <p>The bypass is gated by three conditions; each fall-through branch is
 * exercised here so the rejected_records sync daemon's auth path stays
 * regression-tested as the surrounding code evolves. The "success" branch
 * (token + table both match) drives executeWithoutPassword which depends
 * on the full HTTP server stack; that path is left to the integration
 * harness in LoadActionTest, while these unit tests focus on the
 * "do-not-bypass" decisions where a leaked-token attack is most likely
 * to slip through.
 */
public class LoadActionInternalTokenTest {

    private LoadAction newAction() {
        // ActionController is only used during registerHandler, which we
        // never call here -- pass null so the test doesn't have to spin
        // up the HTTP framework.
        return new LoadAction(null);
    }

    private BaseRequest stubRequest(String tokenHeader, String dbName, String tableName) {
        BaseRequest req = mock(BaseRequest.class);
        HttpRequest httpReq = mock(HttpRequest.class);
        HttpHeaders headers = new DefaultHttpHeaders();
        if (tokenHeader != null) {
            headers.add(LoadAction.INTERNAL_TOKEN_HEADER, tokenHeader);
        }
        when(req.getRequest()).thenReturn(httpReq);
        when(httpReq.headers()).thenReturn(headers);
        when(req.getSingleParameter("db")).thenReturn(dbName);
        when(req.getSingleParameter("table")).thenReturn(tableName);
        return req;
    }

    @Test
    public void noTokenHeaderFallsThrough() throws DdlException, AccessDeniedException {
        BaseRequest req = stubRequest(null, "_statistics_", "rejected_records");
        assertFalse(newAction().tryInternalTokenBypass(req, null));
    }

    @Test
    public void emptyTokenHeaderFallsThrough() throws DdlException, AccessDeniedException {
        BaseRequest req = stubRequest("", "_statistics_", "rejected_records");
        assertFalse(newAction().tryInternalTokenBypass(req, null));
    }

    @Test
    public void wrongDatabaseFallsThrough(@Mocked GlobalStateMgr ignored) throws DdlException, AccessDeniedException {
        // The token gate must reject any non-rejected_records target even
        // when the token itself would have been valid -- a leaked token
        // cannot be reused to write some other table.
        BaseRequest req = stubRequest("any-token", "user_db", "rejected_records");
        assertFalse(newAction().tryInternalTokenBypass(req, null));
    }

    @Test
    public void wrongTableFallsThrough(@Mocked GlobalStateMgr ignored) throws DdlException, AccessDeniedException {
        BaseRequest req = stubRequest("any-token", "_statistics_", "rejected_records_foo");
        assertFalse(newAction().tryInternalTokenBypass(req, null));
    }

    @Test
    public void tokenMismatchFallsThrough(@Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked NodeMgr nodeMgr) throws DdlException, AccessDeniedException {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                nodeMgr.getToken();
                result = "expected-token";
            }
        };
        BaseRequest req = stubRequest("attacker-supplied-token", "_statistics_", "rejected_records");
        assertFalse(newAction().tryInternalTokenBypass(req, null));
    }

    @Test
    public void emptyFeTokenFallsThrough(@Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked NodeMgr nodeMgr) throws DdlException, AccessDeniedException {
        // Defensive: pre-bootstrap clusters where NodeMgr.getToken() is
        // null/empty must never accept any inbound token as valid.
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                nodeMgr.getToken();
                result = "";
            }
        };
        BaseRequest req = stubRequest("any-token", "_statistics_", "rejected_records");
        assertFalse(newAction().tryInternalTokenBypass(req, null));
    }
}
