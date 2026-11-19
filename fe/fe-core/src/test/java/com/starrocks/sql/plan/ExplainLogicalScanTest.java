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

import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

// EXPLAIN LOGICAL must print scans that have no dedicated printer in Explain.OperatorPrinter
// (Paimon, Delta Lake, Iceberg metadata tables, ...) instead of recursing until the stack overflows.
public class ExplainLogicalScanTest extends ConnectorPlanTestBase {

    private String getLogicalExplain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second
                .getExplainString(StatementBase.ExplainLevel.LOGICAL);
    }

    @Test
    public void testPaimonScan() throws Exception {
        String plan = getLogicalExplain("SELECT COUNT(*) FROM paimon0.pmn_db1.partitioned_table");
        assertContains(plan, "- PAIMON-SCAN [partitioned_table]");
    }

    @Test
    public void testPaimonSystemTableScan() throws Exception {
        String plan = getLogicalExplain("SELECT * FROM paimon0.pmn_db1.partitioned_table$snapshots");
        assertContains(plan, "SCAN [partitioned_table$snapshots]");
    }

    @Test
    public void testScanWithPrinterUnchanged() throws Exception {
        String plan = getLogicalExplain("SELECT * FROM iceberg0.unpartitioned_db.t0");
        assertContains(plan, "- ICEBERG-SCAN [t0]");
    }
}
