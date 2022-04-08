package com.starrocks.sql.plan;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CastExprPruneTest extends PlanTestBase {
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

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS tab1 (" +
                "c_1_0 DECIMAL64(9, 9) NULL ," +
                "c_1_1 CHAR(1) NOT NULL ," +
                "c_1_2 DECIMAL32(5, 4) NOT NULL ," +
                "c_1_3 DECIMAL32(4, 0) NOT NULL ," +
                "c_1_4 CHAR(11) NOT NULL ," +
                "c_1_5 DATE NOT NULL ," +
                "c_1_6 DECIMAL128(20, 5) NULL ) " +
                "UNIQUE KEY (c_1_0,c_1_1) " +
                "DISTRIBUTED BY HASH (c_1_0) " +
                "properties(\"replication_num\"=\"1\") ;");

        starRocksAssert.withTable("CREATE TABLE tab2 (" +
                "c_2_0 BOOLEAN NULL ) " +
                "AGGREGATE KEY (c_2_0) " +
                "DISTRIBUTED BY HASH (c_2_0) " +
                "properties(\"replication_num\"=\"1\") ;");
    }

    @Test
    public void testQuery() throws Exception {
        String sql = "SELECT DISTINCT subt2.c_2_0 FROM tab0, " +
                "(SELECT tab2.c_2_0 FROM tab2 " +
                "WHERE ( ( tab2.c_2_0 ) = ( true ) ) < ( ((tab2.c_2_0) IN (false) ) BETWEEN (tab2.c_2_0) AND (tab2.c_2_0) ) ) subt2" +
                " FULL OUTER JOIN (SELECT tab1.c_1_0, tab1.c_1_1, tab1.c_1_2, tab1.c_1_3, tab1.c_1_4, tab1.c_1_5, tab1.c_1_6 FROM tab1 " +
                " ORDER BY tab1.c_1_4, tab1.c_1_2) subt1 ON subt2.c_2_0 = subt1.c_1_2 AND (6453) IN (4861, 4302) < subt1.c_1_2 " +
                " AND subt2.c_2_0 != subt1.c_1_1 AND subt2.c_2_0 <= subt1.c_1_1 AND subt2.c_2_0 > subt1.c_1_0 AND subt2.c_2_0 = subt1.c_1_0 " +
                " WHERE (((0.00) BETWEEN (CASE WHEN (subt1.c_1_5) BETWEEN (subt1.c_1_5) AND (subt1.c_1_5) THEN CAST(151971657 AS DECIMAL32 ) " +
                " WHEN false THEN CASE WHEN NULL THEN 0.03 ELSE 0.02 END ELSE 0.04 END) AND (0.04) ) IS NULL)";
        String explain = getFragmentPlan(sql);
        String snippet = "  0:OlapScanNode\n" +
                "     TABLE: tab1\n" +
                "     PREAGGREGATION: OFF. Reason: None aggregate function\n" +
                "     PREDICATES: TRUE\n" +
                "     partitions=0/1\n" +
                "     rollup: tab1\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     numNodes=0";
        System.out.println(explain);
        Assert.assertTrue(explain.contains(snippet));
    }
}
