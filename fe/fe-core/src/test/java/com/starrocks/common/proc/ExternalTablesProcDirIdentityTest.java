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

package com.starrocks.common.proc;

import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * These three call sites used to build a fresh empty ConnectContext, which reached the connector without the
 * caller's identity or session variables. That is a gap once a connector authorizes per user, and it applies
 * to every external catalog, not only Lake Formation - so both halves are pinned here: the caller's context
 * is passed through when there is one, and the code still works when there is not.
 *
 * The assertions are on the context object itself rather than on any returned data, because passing the
 * right identity along is the entire behaviour under test.
 */
public class ExternalTablesProcDirIdentityTest {

    private static final String CATALOG = "hive_catalog";
    private static final String DB = "db";

    @Mocked
    private GlobalStateMgr globalStateMgr;

    /**
     * Forced into the cascade explicitly: with GlobalStateMgr mocked, getMetadataMgr() would otherwise
     * answer with a cascaded mock of its own, and nothing recorded against this one would ever be reached.
     */
    @Mocked
    private MetadataMgr metadataMgr;

    @BeforeEach
    public void routeMetadataMgr() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static ConnectContext callerOnThisThread() {
        ConnectContext caller = new ConnectContext();
        caller.setQueryId(UUID.randomUUID());
        caller.setThreadLocalInfo();
        return caller;
    }

    @Test
    public void testLookupPassesTheCallersContextThrough() throws Exception {
        ConnectContext caller = callerOnThisThread();

        new ExternalTablesProcDir(CATALOG, DB).lookup("t");

        List<ConnectContext> dbContexts = new ArrayList<>();
        List<ConnectContext> tableContexts = new ArrayList<>();
        new Verifications() {
            {
                metadataMgr.getDb(withCapture(dbContexts), anyString, anyString);
                metadataMgr.getTable(withCapture(tableContexts), anyString, anyString, anyString);
            }
        };
        assertSame(caller, dbContexts.get(0), "getDb must receive the caller's context");
        assertSame(caller, tableContexts.get(0), "getTable must receive the caller's context");
    }

    @Test
    public void testFetchResultPassesTheCallersContextThrough() throws Exception {
        ConnectContext caller = callerOnThisThread();

        new ExternalTablesProcDir(CATALOG, DB).fetchResult();

        List<ConnectContext> contexts = new ArrayList<>();
        new Verifications() {
            {
                metadataMgr.listTableNames(withCapture(contexts), anyString, anyString);
            }
        };
        assertSame(caller, contexts.get(0));
    }

    /**
     * The half that must not regress. Thrift handlers and background tasks reach this code with no ambient
     * context at all, and it has to keep working there - just without an identity to pass on. This applies
     * to every external catalog, so getting it wrong would be a regression well beyond Lake Formation.
     */
    @Test
    public void testStillWorksWithNoAmbientContext() throws Exception {
        ConnectContext.remove();

        new ExternalTablesProcDir(CATALOG, DB).fetchResult();

        List<ConnectContext> contexts = new ArrayList<>();
        new Verifications() {
            {
                metadataMgr.listTableNames(withCapture(contexts), anyString, anyString);
            }
        };
        assertNotNull(contexts.get(0), "a fresh context is expected, never null");
    }
}
