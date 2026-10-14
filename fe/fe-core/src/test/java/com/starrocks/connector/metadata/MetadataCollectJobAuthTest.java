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

package com.starrocks.connector.metadata;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TResultSinkType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards the REST auth-token propagation contract of {@link MetadataCollectJob}.
 *
 * <p>Iceberg scan planning switches to a remote/distributed path for tables whose
 * manifests exceed the local-planning threshold (or when {@code plan_mode=distributed}).
 * That path runs an internal metadata-collection query via a {@link MetadataCollectJob}.
 * If the job is initialized without the originating {@link ConnectContext}, its context
 * carries no REST auth token, and the metadata query's REST calls (e.g. {@code tableExists})
 * are rejected under OIDC/JWT security — failing deterministically regardless of data,
 * only for tables large enough to trigger remote planning.
 *
 * <p>These tests assert that {@link MetadataCollectJob#init(ConnectContext)} forwards the
 * auth token while {@link MetadataCollectJob#init(SessionVariable)} does not, so a
 * regression that reverts the caller to the session-variable overload is caught here.
 */
public class MetadataCollectJobAuthTest {

    private static final String AUTH_TOKEN = "test-jwt-abc123";

    /**
     * Test-only job that stubs out {@code buildConnectContext} so the test stays hermetic
     * (no GlobalStateMgr / warehouse resolution) and exercises only the token-forwarding
     * logic in the {@code init(...)} overloads.
     */
    private static class FakeMetadataCollectJob extends MetadataCollectJob {
        FakeMetadataCollectJob() {
            super("cat", "db", "tbl", TResultSinkType.METADATA_ICEBERG);
        }

        @Override
        protected String buildCollectMetadataSQL() {
            return "SELECT 1";
        }

        @Override
        protected ConnectContext buildConnectContext(SessionVariable originSessionVariable) {
            // Bare context, no token — mirrors the real builder's starting point without
            // pulling in GlobalStateMgr/warehouse wiring.
            return new ConnectContext();
        }
    }

    @Test
    public void testInitWithConnectContextForwardsAuthToken() {
        ConnectContext origin = new ConnectContext();
        origin.setAuthToken(AUTH_TOKEN);

        FakeMetadataCollectJob job = new FakeMetadataCollectJob();
        job.init(origin);

        Assertions.assertNotNull(job.getContext());
        Assertions.assertEquals(AUTH_TOKEN, job.getContext().getAuthToken(),
                "init(ConnectContext) must forward the originating session's REST auth token "
                        + "so remote/distributed metadata planning can authenticate to the REST catalog");
    }

    @Test
    public void testInitWithConnectContextNullTokenIsSafe() {
        ConnectContext origin = new ConnectContext();
        // No token set (e.g. non-JWT security).
        FakeMetadataCollectJob job = new FakeMetadataCollectJob();
        job.init(origin);

        Assertions.assertNotNull(job.getContext());
        Assertions.assertNull(job.getContext().getAuthToken());
    }

    @Test
    public void testInitWithSessionVariableDoesNotCarryToken() {
        // The legacy overload builds a context from the session variable only; it cannot
        // carry an auth token. This documents why the remote-planning caller must use the
        // ConnectContext overload instead.
        FakeMetadataCollectJob job = new FakeMetadataCollectJob();
        job.init(new SessionVariable());

        Assertions.assertNotNull(job.getContext());
        Assertions.assertNull(job.getContext().getAuthToken());
    }
}
