// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyInferenceTest extends PlanTestBase {

    private List<String> planAndInferenceKey(String sql) throws Exception {
        ExecPlan plan = getExecPlan(sql);
        OptExpression physicalPlan = plan.getPhysicalPlan();
        KeyInference.KeyPropertySet keys = KeyInference.infer(physicalPlan, plan);
        Map<Integer, String> columnNames = plan.getOutputColumns().stream().collect(
                Collectors.toMap(ColumnRefOperator::getId, ColumnRefOperator::getName));

        if (keys == null || keys.empty()) {
            return Lists.newArrayList();
        }

        List<String> res = new ArrayList<>();
        for (KeyInference.KeyProperty key : keys.getKeys()) {
            String columnsStr = key.columns.getStream().mapToObj(columnNames::get).collect(Collectors.joining(","));
            res.add(columnsStr);
        }
        return res;
    }

    private void assertInferenceContains(String sql, String key) throws Exception {
        List<String> keys = planAndInferenceKey(sql);
        Assert.assertTrue("expected is " + key + "\n, but got " + keys.toString(), keys.contains(key));
    }

    @Test
    public void testProject() throws Exception {
        assertInferenceContains("select t0.v1 from t0;", "v1");
        assertInferenceContains("select t0.v1 + 1 from t0;", "expr");
        assertInferenceContains("select t0.v1, t0.v2 from t0;", "v1,v2");
    }
}
