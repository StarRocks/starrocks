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
package com.starrocks.epack.policy;

import com.starrocks.epack.catalog.system.starrocks.PolicyReferences;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.thrift.TGetPolicyReferenceItem;
import com.starrocks.thrift.TGetPolicyReferenceResponse;
import com.starrocks.thrift.TGetPolicyReferencesRequest;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * Verifies that sys.policy_references shows [NULL]:&lt;uuid&gt; markers for dangling entries
 * when the referenced table is dropped, without requiring ADMIN CLEAN.
 */
public class PolicyReferencesDanglingDisplayTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.useDatabase("test");
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testMaskingPolicyDanglingShowsDeletedMarker() throws Exception {
        DDLStmtExecutor.execute(
                analyzeSuccess("CREATE MASKING POLICY mp_display AS (v INT) RETURNS INT -> v + 1"),
                starRocksAssert.getCtx());

        starRocksAssert.withTable(
                "CREATE TABLE test.t_mp_display (k INT, v INT)" +
                " DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1" +
                " PROPERTIES ('replication_num'='1')");

        DDLStmtExecutor.execute(
                analyzeSuccess("ALTER TABLE test.t_mp_display MODIFY COLUMN v" +
                        " SET MASKING POLICY mp_display USING (v)"),
                starRocksAssert.getCtx());

        // Drop the table to create a dangling entry
        starRocksAssert.dropTable("t_mp_display");

        // sys.policy_references must NOT throw; the dangling row must appear with [NULL]:<uuid> markers
        TGetPolicyReferenceResponse resp = PolicyReferences.getPolicyReference(new TGetPolicyReferencesRequest());
        List<TGetPolicyReferenceItem> items = itemsByPolicy(resp, "mp_display");
        Assert.assertEquals(1, items.size());
        Assert.assertTrue(
                "ref_object_name must start with [NULL]:",
                items.get(0).getRef_object_name().startsWith("[NULL]:"));
        Assert.assertTrue(
                "ref_column must start with [NULL]:",
                items.get(0).getRef_column().startsWith("[NULL]:"));

        DDLStmtExecutor.execute(analyzeSuccess("DROP MASKING POLICY mp_display"), starRocksAssert.getCtx());
    }

    @Test
    public void testRowAccessPolicyDanglingShowsDeletedMarker() throws Exception {
        DDLStmtExecutor.execute(
                analyzeSuccess("CREATE ROW ACCESS POLICY rap_display AS (region VARCHAR) RETURNS BOOLEAN -> TRUE"),
                starRocksAssert.getCtx());

        starRocksAssert.withTable(
                "CREATE TABLE test.t_rap_display (region VARCHAR(20), v INT)" +
                " DUPLICATE KEY(region) DISTRIBUTED BY HASH(region) BUCKETS 1" +
                " PROPERTIES ('replication_num'='1')");

        DDLStmtExecutor.execute(
                analyzeSuccess("ALTER TABLE test.t_rap_display ADD ROW ACCESS POLICY rap_display ON (region)"),
                starRocksAssert.getCtx());

        // Drop the table to create a dangling entry
        starRocksAssert.dropTable("t_rap_display");

        // sys.policy_references must NOT throw; dangling row must appear with [NULL]:<uuid> table name
        TGetPolicyReferenceResponse resp = PolicyReferences.getPolicyReference(new TGetPolicyReferencesRequest());
        List<TGetPolicyReferenceItem> items = itemsByPolicy(resp, "rap_display");
        Assert.assertEquals(1, items.size());
        Assert.assertTrue(
                "ref_object_name must start with [NULL]:",
                items.get(0).getRef_object_name().startsWith("[NULL]:"));

        DDLStmtExecutor.execute(analyzeSuccess("DROP ROW ACCESS POLICY rap_display"), starRocksAssert.getCtx());
    }

    private static List<TGetPolicyReferenceItem> itemsByPolicy(TGetPolicyReferenceResponse resp, String policyName) {
        if (resp.getPolicy_reference() == null) {
            return Collections.emptyList();
        }
        return resp.getPolicy_reference().stream()
                .filter(item -> policyName.equals(item.getPolicy_name()))
                .collect(Collectors.toList());
    }
}
