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


package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ModifyInferenceTest extends PlanTestBase {

    private ModifyInference.ModifyOp planAndInferenceKey(String sql) throws Exception {
        ExecPlan plan = getExecPlan(sql);
        OptExpression physicalPlan = plan.getPhysicalPlan();
        return ModifyInference.infer(physicalPlan);
    }

    private void assertInferenceModify(String sql, ModifyInference.ModifyOp expected) throws Exception {
        ModifyInference.ModifyOp modify = planAndInferenceKey(sql);
        Assertions.assertEquals(expected, modify);
    }

    private void assertInferenceNotSupported(String sql) throws Exception {
        Assertions.assertThrows(org.apache.commons.lang3.NotImplementedException.class, () -> planAndInferenceKey(sql));
    }

    @Test
    public void testScan() throws Exception {
        assertInferenceModify("select * from t0", ModifyInference.ModifyOp.INSERT_ONLY);
        assertInferenceModify("select v1 from t0", ModifyInference.ModifyOp.INSERT_ONLY);
    }

    @Test
    public void testJoin() throws Exception {
        assertInferenceNotSupported("select * from t0 join t1 on t0.v1 = t1.v4");
        assertInferenceNotSupported("select * from tprimary join t1 on pk = t1.v4");
    }

    @Test
    public void testAgg() throws Exception {
        assertInferenceNotSupported("select v1, count(*) from t0 group by v1");
        assertInferenceModify("select pk, count(*) from tprimary group by pk", ModifyInference.ModifyOp.ALL);
        assertInferenceNotSupported("select v1, count(*) from t0 join t1 on t0.v1=t1.v4 group by v1");
        assertInferenceNotSupported("select v4, count(*) from tprimary join t1 on pk=t1.v4 group by v4");
    }
}
