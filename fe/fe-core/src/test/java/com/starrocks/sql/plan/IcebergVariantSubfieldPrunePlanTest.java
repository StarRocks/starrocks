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

package com.starrocks.sql.plan;

import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.thrift.TPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class IcebergVariantSubfieldPrunePlanTest extends ConnectorPlanTestBase {

    @Test
    public void testVariantColumnAccessPathInExplainAndThrift() throws Exception {
        connectContext.getSessionVariable().setCboPruneSubfield(true);

        String sql = "select get_variant_int(v, '$.a.b'), get_variant_string(v, '$.profile.department') " +
                "from iceberg0.unpartitioned_db.variant_t0";

        String plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/v/a/b(bigint(20)), /v/profile/department(varchar)]");

        ExecPlan execPlan = getExecPlan(sql);
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        Assertions.assertEquals(1, scanNodes.size());
        Assertions.assertInstanceOf(IcebergScanNode.class, scanNodes.get(0));

        IcebergScanNode scanNode = (IcebergScanNode) scanNodes.get(0);
        Assertions.assertNotNull(scanNode.getColumnAccessPaths());
        Assertions.assertFalse(scanNode.getColumnAccessPaths().isEmpty());
        Assertions.assertTrue(scanNode.getColumnAccessPaths().stream()
                .anyMatch(path -> "/v/a/b(bigint(20)), /v/profile/department(varchar)".equals(path.explain())));

        TPlan thrift = scanNode.treeToThrift();
        Assertions.assertTrue(thrift.getNodes().get(0).getHdfs_scan_node().isSetColumn_access_paths());
        Assertions.assertTrue(thrift.getNodes().get(0).getHdfs_scan_node().getColumn_access_pathsSize() >= 1);
    }
}
