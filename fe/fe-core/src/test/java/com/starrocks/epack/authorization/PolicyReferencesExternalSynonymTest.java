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
package com.starrocks.epack.authorization;

import com.starrocks.epack.catalog.system.starrocks.PolicyReferences;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TGetPolicyReferenceItem;
import com.starrocks.thrift.TGetPolicyReferenceResponse;
import com.starrocks.thrift.TGetPolicyReferencesRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Verifies that sys.policy_references handles an external table that was dropped and recreated
 * under the same name:
 * <ul>
 *   <li>a live external table renders its full table UID (catalog.db.table.createTime) as
 *       REF_OBJECT_NAME so same-name tables stay distinguishable;</li>
 *   <li>a stale entry whose stored UID (old create time) no longer matches the live table's UID
 *       is reported as dangling with [NULL]:&lt;uid&gt; markers, without throwing.</li>
 * </ul>
 */
public class PolicyReferencesExternalSynonymTest extends ConnectorPlanTestBase {

    private static final String CATALOG = "hive0";
    private static final String DB = "tpch";
    private static final String TABLE = "supplier";

    @Test
    public void testRecreatedExternalSynonymIsDangling() {
        SecurityPolicyMgr mgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();

        // The UID that matches the live external table (correct create time).
        TableUID liveUID = TableUID.generate(connectContext, CATALOG, DB, TABLE);
        // A stale UID for the same name but a different create time, as if the table had been
        // dropped and recreated. getTableNameFromUUID still resolves it to the live table by name,
        // but its stored UID differs from the live table's UID.
        TableUID staleUID = new TableUID(liveUID.getCatalogId(), liveUID.getDatabaseUUID(),
                liveUID.getTableUUID() + "0");
        Assertions.assertNotEquals(liveUID, staleUID);

        PolicyAppliedContext liveCtx = new PolicyAppliedContext();
        liveCtx.addRowAccessPolicy(new RowAccessPolicyContext(900001L, Collections.emptyList()));
        PolicyAppliedContext staleCtx = new PolicyAppliedContext();
        staleCtx.addRowAccessPolicy(new RowAccessPolicyContext(900002L, Collections.emptyList()));

        mgr.getPolicyContextMap().put(liveUID, liveCtx);
        mgr.getPolicyContextMap().put(staleUID, staleCtx);
        try {
            TGetPolicyReferenceResponse resp = PolicyReferences.getPolicyReference(new TGetPolicyReferencesRequest());

            List<TGetPolicyReferenceItem> items = itemsForTable(resp, TABLE);
            Assertions.assertEquals(2, items.size(), "expected one live and one stale row");

            List<TGetPolicyReferenceItem> dangling = items.stream()
                    .filter(i -> i.getRef_object_name().startsWith("[NULL]:"))
                    .collect(Collectors.toList());
            List<TGetPolicyReferenceItem> live = items.stream()
                    .filter(i -> !i.getRef_object_name().startsWith("[NULL]:"))
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, dangling.size(), "stale synonym must be reported as dangling");
            Assertions.assertEquals("[NULL]:" + staleUID.getTableUUID(), dangling.get(0).getRef_object_name());

            Assertions.assertEquals(1, live.size(), "live external table must remain visible");
            Assertions.assertEquals(liveUID.getTableUUID(), live.get(0).getRef_object_name(),
                    "live external table object name must be the full table UID");
            Assertions.assertEquals(CATALOG, live.get(0).getRef_catalog());
            Assertions.assertEquals(DB, live.get(0).getRef_database());
        } finally {
            mgr.getPolicyContextMap().remove(liveUID);
            mgr.getPolicyContextMap().remove(staleUID);
        }
    }

    private static List<TGetPolicyReferenceItem> itemsForTable(TGetPolicyReferenceResponse resp, String table) {
        if (resp.getPolicy_reference() == null) {
            return Collections.emptyList();
        }
        return resp.getPolicy_reference().stream()
                .filter(i -> i.getRef_object_name() != null && i.getRef_object_name().contains(table))
                .collect(Collectors.toList());
    }
}
