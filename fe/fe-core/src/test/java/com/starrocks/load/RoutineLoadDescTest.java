// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load;

import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.qe.OriginStatement;
import org.junit.Assert;
import org.junit.Test;

public class RoutineLoadDescTest {
    @Test
    public void testToSql() throws Exception {
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement("CREATE ROUTINE LOAD job ON tbl " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c`=1), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "WHERE a = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"3\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);

        RoutineLoadDesc desc = new RoutineLoadDesc();
        // set column separator and check
        desc.setColumnSeparator(originLoad.getColumnSeparator());
        Assert.assertEquals("COLUMNS TERMINATED BY ';'", desc.toSql());
        // set row delimiter and check
        desc.setRowDelimiter(originLoad.getRowDelimiter());
        Assert.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n'", desc.toSql());
        // set columns and check
        desc.setColumnsInfo(originLoad.getColumnsInfo());
        Assert.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1)", desc.toSql());
        // set partitions and check
        desc.setPartitionNames(originLoad.getPartitionNames());
        Assert.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1), " +
                "TEMPORARY PARTITION(`p1`, `p2`)",
                desc.toSql());
        // set where and check
        desc.setWherePredicate(originLoad.getWherePredicate());
        Assert.assertEquals("COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), " +
                        "TEMPORARY PARTITION(`p1`, `p2`), " +
                        "WHERE `a` = 1",
                desc.toSql());
    }
}
