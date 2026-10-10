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

import com.starrocks.qe.GlobalVariable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ReduceCastVarcharPlanTest extends PlanTestBase {

    private final boolean previousLengthInheritance =
            GlobalVariable.isEnableReduceCastVarcharLengthInheritance();
    private final boolean previousExprSync =
            GlobalVariable.isEnableReduceCastVarcharExprSyncType();

    @AfterEach
    public void tearDown() {
        GlobalVariable.setEnableReduceCastVarcharLengthInheritance(previousLengthInheritance);
        GlobalVariable.setEnableReduceCastVarcharExprSyncType(previousExprSync);
    }

    @Test
    public void testOutputSlotKeepsOriginalLengthWhenInheritanceDisabled() throws Exception {
        GlobalVariable.setEnableReduceCastVarcharLengthInheritance(false);
        GlobalVariable.setEnableReduceCastVarcharExprSyncType(false);

        String sql = "select cast(t1a as varchar(10)) as c from test_all_type";
        String descTbl = getDescTbl(sql);

        assertContains(descTbl, "TScalarType(type:VARCHAR, len:20)");
    }

    @Test
    public void testOutputSlotStillKeepsOriginalLengthWithoutExprSync() throws Exception {
        GlobalVariable.setEnableReduceCastVarcharLengthInheritance(true);
        GlobalVariable.setEnableReduceCastVarcharExprSyncType(false);

        String sql = "select cast(t1a as varchar(10)) as c from test_all_type";
        String descTbl = getDescTbl(sql);

        assertContains(descTbl, "TScalarType(type:VARCHAR, len:20)");
    }

    @Test
    public void testFullOuterJoinUsingKeepsJoinKeyLength() throws Exception {
        GlobalVariable.setEnableReduceCastVarcharLengthInheritance(true);
        GlobalVariable.setEnableReduceCastVarcharExprSyncType(true);

        starRocksAssert.withTables(List.of(
                        "CREATE TABLE foj_left (id INT, region VARCHAR(64)) DUPLICATE KEY(id) " +
                                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')",
                        "CREATE TABLE foj_right (id INT, region VARCHAR(255)) DUPLICATE KEY(id) " +
                                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"),
                () -> {
                    String sql = "select region from foj_left full outer join foj_right using(region)";
                    String plan = getVerboseExplain(sql);
                    // The COALESCE the transformer synthesizes must not retype the join key.
                    assertContains(plan, "equal join conjunct: [2: region, VARCHAR(64), true] = " +
                            "[4: region, VARCHAR(255), true]");
                });
    }

    @Test
    public void testOutputSlotInheritsCastLengthWithExprSync() throws Exception {
        GlobalVariable.setEnableReduceCastVarcharLengthInheritance(true);
        GlobalVariable.setEnableReduceCastVarcharExprSyncType(true);

        String sql = "select cast(t1a as varchar(10)) as c from test_all_type";
        String descTbl = getDescTbl(sql);

        assertContains(descTbl, "TScalarType(type:VARCHAR, len:10)");
    }
}
