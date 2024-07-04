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

package com.starrocks.scheduler;

import com.google.common.collect.Sets;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.lifecycle.TunespaceIngester;
import com.starrocks.sql.automv.util.TieredMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MVLifeCycleAutoKeeper extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(MVLifeCycleAutoKeeper.class);

    @Override
    protected void runAfterCatalogReady() {
        // reset if the interval has been changed
        setInterval(10L * 1000L);

        if (!Config.enable_mv_lifecycle_auto_keeper || FeConstants.runningUnitTest) {
            return;
        }

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();

        try {
            process(ctx);
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of MVActiveChecker", e);
        } finally {
            ctx.cleanup();
        }
    }

    public void process(ConnectContext ctx) throws Throwable {
        // starrocks_audit_db__.starrocks_audit_tbl__
        String auditDb = "starrocks_audit_db__";
        String auditTbl = "starrocks_audit_tbl__";
        String tsDb = "automv_ts_db__";
        String tsTbl = "automv_ts_tbl_";
        String mvDb = "automv_db";

        TunespaceIngester ingester = TunespaceIngester.of(ctx, auditDb, auditTbl, tsDb, tsTbl, mvDb);
        ingester.prepare();
        ingester.ingest();

        Set<String> legacyDigests = ingester.listLegacyMVs().stream()
                .map(MVName::getDigest).collect(Collectors.toSet());
        Predicate<String> needNotToCreate = mvName -> MVName.parse(mvName)
                .map(MVName::getDigest)
                .map(legacyDigests::contains)
                .orElse(false);

        TieredMap<String, String> mvMap = ingester.recommendMVs();
        TieredMap<String, String> newMvMap = mvMap.entrySet()
                .stream().filter(e -> !needNotToCreate.test(e.getKey()))
                .collect(TieredMap.toMap());

        ingester.createMVs(newMvMap);
    }
}
