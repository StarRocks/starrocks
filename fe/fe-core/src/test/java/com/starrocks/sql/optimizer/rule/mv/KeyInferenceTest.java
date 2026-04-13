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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyInferenceTest extends PlanTestBase {

    private List<String> planAndInferenceKey(String sql) throws Exception {
        ExecPlan plan = getExecPlan(sql);
        OptExpression physicalPlan = plan.getPhysicalPlan();
        KeyInference.KeyPropertySet keys = KeyInference.infer(physicalPlan, null);
        Map<Integer, String> columnNames = plan.getOutputColumns().stream().collect(
                Collectors.toMap(ColumnRefOperator::getId, ColumnRefOperator::getName));

        if (keys == null || keys.empty()) {
            return Lists.newArrayList();
        }

        List<String> res = new ArrayList<>();
        for (KeyInference.KeyProperty key : keys.getKeys()) {
            res.add(key.format(columnNames));
        }
        return res;
    }

    private void assertInferenceContains(String sql, String key) throws Exception {
        List<String> keys = planAndInferenceKey(sql);
        if (StringUtils.isEmpty(key)) {
            Assertions.assertTrue(keys.isEmpty(), "expect empty but got " + keys);
        } else {
            Assertions.assertTrue(keys.contains(key), "expected is " + key + "\n, but got " + keys);
        }
    }

    private void assertInferenceNotSupported(String sql) throws Exception {
        Assertions.assertThrows(NotImplementedException.class, () -> planAndInferenceKey(sql));
    }

    @Test
    public void testProject() throws Exception {
        assertInferenceContains("select t0.v1 from t0;", "Key{unique=false, columns=v1}");
        assertInferenceContains("select t0.v2 from t0;", "Key{unique=false, columns=v2}");
        assertInferenceContains("select t0.v3 from t0;", "Key{unique=false, columns=v3}");
        assertInferenceContains("select v1,v2 from t0;", "Key{unique=false, columns=v1,v2}");
        assertInferenceContains("select t0.v1 + 1 from t0;", "Key{unique=false, columns=expr}");
        assertInferenceContains("select t0.v1 + t0.v2 from t0;", "Key{unique=false, columns=expr}");

        assertInferenceContains("select pk from tprimary;", "Key{unique=true, columns=pk}");
        assertInferenceContains("select pk,v1 from tprimary;", "Key{unique=true, columns=pk,v1}");
        assertInferenceContains("select v1 from tprimary;", "Key{unique=false, columns=v1}");
        assertInferenceContains("select pk+1 from tprimary;", "Key{unique=false, columns=expr}");
    }

    @Test
    public void testJoin() throws Exception {
        assertInferenceNotSupported("select * from t0 join t1 on t0.v1 = t1.v4");
        assertInferenceNotSupported("select * from tprimary join t1 on pk = t1.v4");
        assertInferenceNotSupported("select * from t0 cross join t1");
        assertInferenceNotSupported("select * from t0 left join t1 on t0.v1 < t1.v4");
    }

    @Test
    public void testAgg() throws Exception {
        assertInferenceNotSupported("select v1, count(*) from t0 group by v1");
        assertInferenceNotSupported("select v1,v2,v3, count(*) from t0 group by v1,v2,v3");
        assertInferenceNotSupported("select count(*) from t0 group by v1");
        assertInferenceNotSupported("select min(v1), max(v1) from t0");
    }
}
