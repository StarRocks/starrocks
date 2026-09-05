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
    public void testOutputSlotInheritsCastLengthWithExprSync() throws Exception {
        GlobalVariable.setEnableReduceCastVarcharLengthInheritance(true);
        GlobalVariable.setEnableReduceCastVarcharExprSyncType(true);

        String sql = "select cast(t1a as varchar(10)) as c from test_all_type";
        String descTbl = getDescTbl(sql);

        assertContains(descTbl, "TScalarType(type:VARCHAR, len:10)");
    }
}
