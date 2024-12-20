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
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.lifecycle.MVLifecycleManager;
import com.starrocks.sql.automv.lifecycle.TunespaceIngester;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class MVLifecycleAutoKeeper extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(MVLifecycleAutoKeeper.class);
    private final MVLifecycleManager mvLifecycleManager = new MVLifecycleManager();

    private long lastCreateTime = -1;

    public MVLifecycleAutoKeeper() {
    }

    public MVLifecycleManager getMVLifecycleManager() {
        return mvLifecycleManager;
    }

    @Override
    protected void runAfterCatalogReady() {
        // reset if the interval has been changed
        setInterval(10L * 1000L);

        if (!GlobalVariable.isEnableAutoMVLifecycleKeeper() || FeConstants.runningUnitTest) {
            return;
        }

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setSessionVariable(GlobalStateMgr.getCurrentState().getVariableMgr().newSessionVariable());
        ctx.getSessionVariable().setAutoMVDecayAcceleratedQueries(true);
        ctx.setThreadLocalInfo();

        try {
            Supplier<Boolean> shouldCreateMVs = () -> {
                long now = System.currentTimeMillis();
                long interval = GlobalVariable.getAutoMVLifecycleMVRecommendationInterval();
                if (lastCreateTime == -1 || Util.timeDiff(now, lastCreateTime) > interval) {
                    lastCreateTime = System.currentTimeMillis();
                    return true;
                } else {
                    return false;
                }
            };
            process(ctx, shouldCreateMVs);
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of MVActiveChecker", e);
        } finally {
            ctx.cleanup();
        }
    }

    private void createMVs(TunespaceIngester ingester) throws Throwable {
        ingester.ingest(mvLifecycleManager);

        List<Pair<MVName, MaterializedViewPlus>> legacyMVs = ingester.listLegacyMVs();
        mvLifecycleManager.associateMVWithLifecycle(legacyMVs);

        // TODO(by satanson): At present, two MVs of the identical digest are considered as
        //  duplicate MVs, however, the digest is computed from the MV's backbone query, so it does not
        //  contain partition, distribution, short key and etc. information. in reality, users
        //  maybe create multiple MVs of the identical digest each of which has different short key indexes.
        //  in the future, multiple MVs of the identical digest would be taken into consideration.
        Predicate<String> needNotToCreate = mvName -> MVName.parse(mvName)
                .map(MVName::getDigest)
                .map(mvLifecycleManager::contains)
                .orElse(false);

        TieredMap<String, String> mvMap = ingester.recommendMVs();
        TieredMap<String, String> newMvMap = mvMap.entrySet()
                .stream().filter(e -> !needNotToCreate.test(e.getKey()))
                .collect(TieredMap.toMap());

        ingester.createMVs(newMvMap);
    }

    public void process(ConnectContext ctx, Supplier<Boolean> shouldCreateMV) throws Throwable {
        // starrocks_audit_db__.starrocks_audit_tbl__
        String auditDb = "starrocks_audit_db__";
        String auditTbl = "starrocks_audit_tbl__";
        String tsDb = "automv_ts_db__";
        String tsTbl = "automv_ts_tbl__";
        String mvDb = "automv_db";

        TunespaceIngester ingester =
                TunespaceIngester.of(ctx, mvLifecycleManager, auditDb, auditTbl, tsDb, tsTbl, mvDb);
        ingester.prepare();
        if (shouldCreateMV.get()) {
            createMVs(ingester);
        }
        mvLifecycleManager.associateMVWithLifecycle(ingester.listLegacyMVs());
        ingester.collectMVHitRatio();
        mvLifecycleManager.scanMVLifecycles();
    }
}
