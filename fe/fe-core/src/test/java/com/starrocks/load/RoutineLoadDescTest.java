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


package com.starrocks.load;

import com.starrocks.qe.OriginStatement;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
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