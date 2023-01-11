// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AnyMapType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StructTypeTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("create table test(c0 INT, " +
                "c1 struct<a array<struct<b int>>>," +
                "c2 struct<a int,b double>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSelectArrayStruct() throws Exception {
        String sql = "select c1.a[10].b from test";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : 2: c1.a[10].b"));
    }

    @Test
    public void testStructMultiSelect() throws Exception {
        String sql = "select c2.a, c2.b from test";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : 3: c2.a\n" +
                "  |  <slot 5> : 3: c2.b\n" +
                "  |  "));
    }

    @Test
    public void testStructWithWindow() throws Exception {
        String sql = "select sum(c2.b) over(partition by c2.a order by c0) from test";
        assertPlanContains(sql, " 4:ANALYTIC\n" +
                "  |  functions: [, sum(6: c2.b), ]\n" +
                "  |  partition by: 7: c2.a\n" +
                "  |  order by: 1: c0 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select sum(c0) over(partition by c2.a order by c2.b) from test";
        assertPlanContains(sql, " 4:ANALYTIC\n" +
                "  |  functions: [, sum(4: c0), ]\n" +
                "  |  partition by: 7: c2.a\n" +
                "  |  order by: 8: c2.b ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select sum(c1.a[10].b) over(partition by c2.a order by c2.b) from test";
        assertPlanContains(sql, "4:ANALYTIC\n" +
                "  |  functions: [, sum(5: c1.a[10].b), ]\n" +
                "  |  partition by: 7: c2.a\n" +
                "  |  order by: 8: c2.b ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testStructMatchType() throws Exception {
        // "struct<struct_test:int,c1:struct<c1:int,cc1:string>>"
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultExternalTableString())
        ));
        StructType root = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", c1)
        ));

        // PseudoType MapType
        Type t = new AnyMapType();
        Assert.assertFalse(root.matchesType(t));

        // MapType
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        Type mapType = new MapType(keyType, valueType);

        Assert.assertFalse(root.matchesType(mapType));

        // Different fields length
        StructType c = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))));
        Assert.assertFalse(root.matchesType(c));

        // Types will match with different field names
        StructType diffName = new StructType(Lists.newArrayList(
                new StructField("st", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc", c1)
        ));
        Assert.assertTrue(root.matchesType(diffName));

        // Different field type
        StructType diffType = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))
        ));
        Assert.assertFalse(root.matchesType(diffType));

        // matched
        StructType mc1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultExternalTableString())
        ));
        StructType matched = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", mc1)
        ));
        Assert.assertTrue(root.matchesType(matched));

        // Won't match with different subfield order
        StructType mc2 = new StructType(Lists.newArrayList(
                new StructField("cc1", ScalarType.createDefaultExternalTableString()),
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))
        ));
        StructType matchedDiffOrder = new StructType(Lists.newArrayList(
                new StructField("c1", mc2),
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT))
        ));
        Assert.assertFalse(root.matchesType(matchedDiffOrder));
    }
}
