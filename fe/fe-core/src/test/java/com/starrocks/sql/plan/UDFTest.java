// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.plan;

import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UDFTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE tab0 (" +
                "c_0_0 DECIMAL(26, 2) NOT NULL ," +
                "c_0_1 DECIMAL128(19, 3) NOT NULL ," +
                "c_0_2 DECIMAL128(4, 3) NULL ," +
                "c_0_3 BOOLEAN NOT NULL ," +
                "c_0_4 DECIMAL128(25, 19) NOT NULL ," +
                "c_0_5 BOOLEAN REPLACE NOT NULL ," +
                "c_0_6 DECIMAL32(8, 5) MIN NULL ," +
                "c_0_7 BOOLEAN REPLACE NULL ," +
                "c_0_8 PERCENTILE PERCENTILE_UNION NULL ," +
                "c_0_9 LARGEINT SUM NULL ," +
                "c_0_10 PERCENTILE PERCENTILE_UNION NOT NULL ," +
                "c_0_11 BITMAP BITMAP_UNION NULL ," +
                "c_0_12 HLL HLL_UNION NOT NULL ," +
                "c_0_13 DECIMAL(16, 3) MIN NULL ," +
                "c_0_14 DECIMAL128(18, 6) MAX NOT NULL " +
                ") AGGREGATE KEY (c_0_0,c_0_1,c_0_2,c_0_3,c_0_4) " +
                "DISTRIBUTED BY HASH (c_0_3,c_0_0,c_0_2) " +
                "properties(\"replication_num\"=\"1\") ;");
    }

    @Test
    public void testUDTF() throws Exception {
        final GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        final Field functionSetField = GlobalStateMgr.class.getDeclaredField("functionSet");
        functionSetField.setAccessible(true);
        final FunctionSet functionSet = (FunctionSet) functionSetField.get(globalStateMgr);

        {
            String fn = "table_function";
            final FunctionName functionName = new FunctionName(fn);
            List<String> colNames = new ArrayList<>();
            colNames.add("table_function");
            List<Type> argTypes = new ArrayList<>();
            argTypes.add(Type.VARCHAR);
            List<Type> retTypes = new ArrayList<>();
            retTypes.add(Type.VARCHAR);
            final TableFunction tableFunction = new TableFunction(functionName, colNames, argTypes, retTypes);
            functionSet.addBuiltin(tableFunction);
        }

        String sql;
        String explain;

        sql = "select table_function from tab0,table_function(c_0_3)";
        explain = getVerboseExplain(sql);
        Assert.assertTrue(explain.contains("  1:Project"));
        Assert.assertTrue(explain.contains("  |  output columns:\n" +
                "  |  17 <-> cast([4: c_0_3, BOOLEAN, false] as VARCHAR)"));

        sql = "select table_function from tab0, table_function(c_0_3 + 3)";
        explain = getVerboseExplain(sql);
        Assert.assertTrue(
                explain.contains("  |  17 <-> cast(cast([4: c_0_3, BOOLEAN, false] as SMALLINT) + 3 as VARCHAR)"));

        sql = "select v1,v2,v3,t.unnest,o.unnest from t0,unnest([1,2,3]) t, unnest([4,5,6]) o ";
        explain = getFragmentPlan(sql);
        Assert.assertTrue(explain.contains("TableValueFunction"));
    }

    @Test
    public void testMultiUnnest() throws Exception {
        String sql = "with t as (select [1,2,3] as a, [4,5,6] as b, [4,5,6] as c) select * from t,unnest(a,b,c)";
        PhysicalTableFunctionOperator tp = (PhysicalTableFunctionOperator) getExecPlan(sql).getPhysicalPlan().getOp();

        Assert.assertEquals(3, tp.getFnParamColumnRef().size());
        Assert.assertEquals("[6, 8, 8]",
                tp.getFnParamColumnRef().stream().map(ColumnRefOperator::getId).collect(Collectors.toList()).toString());

        sql = "select * from tarray, unnest(v3, v3)";
        tp = (PhysicalTableFunctionOperator) getExecPlan(sql).getPhysicalPlan().getOp();
        Assert.assertEquals(2, tp.getFnParamColumnRef().size());
        Assert.assertEquals("[3, 3]",
                tp.getFnParamColumnRef().stream().map(ColumnRefOperator::getId).collect(Collectors.toList()).toString());

        sql = "WITH t AS (\n" +
                "SELECT array_sort(v3) AS a,\n" +
                "array_sort(v3) AS b\n" +
                "FROM tarray\n" +
                "GROUP BY v3 )\n" +
                "select unnest.a, unnest.b from t, unnest(a, b) as unnest(a, b);";
        tp = (PhysicalTableFunctionOperator) getExecPlan(sql).getPhysicalPlan().getOp();
        Assert.assertEquals(2, tp.getFnParamColumnRef().size());
        Assert.assertEquals("[8, 8]",
                tp.getFnParamColumnRef().stream().map(ColumnRefOperator::getId).collect(Collectors.toList()).toString());
    }
}
