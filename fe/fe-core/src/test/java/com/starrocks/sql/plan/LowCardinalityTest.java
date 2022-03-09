// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class LowCardinalityTest extends PlanTestBase {

    @Test
    public void testMetaScan() throws Exception {
        String sql = "select max(v1), min(v1) from t0 [_META_]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:MetaScan\n" +
                "     <id 6> : max_v1\n" +
                "     <id 7> : min_v1"));

        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("id_to_names:{6=max_v1, 7=min_v1}"));
    }

    @Test
    public void testMetaScan2() throws Exception {
        String sql = "select max(t1c), min(t1d), dict_merge(t1a) from test_all_type [_META_]";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  0:MetaScan\n" +
                "     <id 16> : dict_merge_t1a\n" +
                "     <id 14> : max_t1c\n" +
                "     <id 15> : min_t1d"));

        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("TFunctionName(function_name:dict_merge), " +
                "binary_type:BUILTIN, arg_types:[TTypeDesc(types:[TTypeNode(type:ARRAY), " +
                "TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))])]"));
    }
}
