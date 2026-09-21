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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationQuerySessionsTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    /**
     * Recorded per test rather than in a setup method: the fail-closed cases throw before they ever reach
     * the cluster id, and a recorded expectation that is never invoked is itself a failure.
     */
    private void stubClusterId() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterId();
                result = 12345;
            }
        };
    }

    @AfterEach
    public void clearThreadLocal() {
        ConnectContext.remove();
    }

    private static ConnectContext contextWithQueryId(UUID queryId) {
        ConnectContext context = new ConnectContext();
        context.setQueryId(queryId);
        return context;
    }

    @Test
    public void testUsesTheQueryIdFromTheGivenContext() {
        stubClusterId();
        UUID queryId = UUID.randomUUID();
        LakeFormationQuerySession session = LakeFormationQuerySessions.of(contextWithQueryId(queryId));
        assertEquals(queryId.toString(), session.queryId());
        assertEquals("12345", session.clusterId());
    }

    /**
     * MetadataMgr picks the metadata instance - and therefore which memo this lands in - from the thread
     * local context alone, so the query id has to come from the same place. Taking it from the passed-in
     * context would let the memo belong to one query while CloudTrail names another.
     */
    @Test
    public void testThreadLocalQueryIdWinsOverTheGivenContext() {
        stubClusterId();
        UUID threadLocalQueryId = UUID.randomUUID();
        contextWithQueryId(threadLocalQueryId).setThreadLocalInfo();

        // A different query id on the passed-in context must not win.
        ConnectContext given = contextWithQueryId(UUID.randomUUID());
        assertEquals(threadLocalQueryId.toString(), LakeFormationQuerySessions.of(given).queryId());
    }

    @Test
    public void testUsesTheGivenContextWhenThereIsNoAmbientQuery() {
        stubClusterId();
        UUID queryId = UUID.randomUUID();
        assertEquals(queryId.toString(),
                LakeFormationQuerySessions.of(contextWithQueryId(queryId)).queryId());
    }

    /**
     * Must be the typed exception, not an NPE: the resolution memo catches everything and would remember an
     * NPE as this table's authorization failure, keeping it dead for the rest of the query.
     */
    @Test
    public void testFailsClosedWhenNoContextAnywhereHasAQueryId() {
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationQuerySessions.of(new ConnectContext()));
        assertTrue(e.getMessage().contains("without a query context"), e.getMessage());
    }

    @Test
    public void testFailsClosedWhenTheContextIsNull() {
        assertThrows(LakeFormationTableAccessException.class, () -> LakeFormationQuerySessions.of(null));
    }

    /** The audit event formats the query id the same way, and the two have to be joinable. */
    @Test
    public void testQueryIdIsFormattedLikeTheAuditEvent() {
        stubClusterId();
        UUID queryId = UUID.randomUUID();
        String formatted = LakeFormationQuerySessions.of(contextWithQueryId(queryId)).queryId();
        assertEquals(queryId.toString(), formatted);
        assertTrue(formatted.contains("-"), formatted);
        assertFalse(formatted.contains(":"), formatted);
    }

    @Test
    public void testQueryStartTimeComesFromTheContext() {
        stubClusterId();
        ConnectContext context = contextWithQueryId(UUID.randomUUID());
        assertEquals(context.getStartTimeInstant(), LakeFormationQuerySessions.of(context).queryStartTime());
    }
}
