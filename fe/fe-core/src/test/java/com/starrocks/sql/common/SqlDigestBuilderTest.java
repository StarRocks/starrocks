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

package com.starrocks.sql.common;

import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SqlDigestBuilderTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
        PlanTestBase.afterClass();
    }

    @ParameterizedTest
    @CsvSource(delimiterString = "|", value = {
            "select * from t2| SELECT * FROM test.t2",
            "select * from t1 where v4 = 1 and v5 = 2|SELECT * FROM test.t1 WHERE (test.t1.v4 = ?) AND (test.t1.v5 = " +
                    "?)",
            "select * from t1 where v4 = 1 or v5 = 2|SELECT * FROM test.t1 WHERE (test.t1.v4 = ?) OR (test.t1.v5 = ?)",
            "select * from t1 where v4 = 1 and (v5 = 2 or v6 = 3)| SELECT * FROM test.t1 WHERE (test.t1.v4 = ?) AND (" +
                    "(test.t1.v5 = ?) OR (test.t1.v6 = ?))",
            "select v4 from t1 limit 1|SELECT test.t1.v4 FROM test.t1 LIMIT  ? ",
            "select * from t1 where v4 in (1, 2, 3) | SELECT * FROM test.t1 WHERE test.t1.v4 IN (?)",
            "select * from t1 where v4 in (select v5 from t1)| SELECT * FROM test.t1 WHERE test.t1.v4 IN (((SELECT " +
                    "test.t1.v5 FROM test.t1)))",
            // with set_var
            "select /*+set_var(query_timeout=123)*/ * from t2| SELECT * FROM test.t2",

            // insert
            "insert into t1 values (1, 2, 3)| INSERT INTO `test`.`t1` VALUES(?, ?, ?)",
            "insert into t1 values (1, 2, 3),(4,5,6)| INSERT INTO `test`.`t1` VALUES(?, ?, ?)",
            "insert into t1 select * from t1| INSERT INTO `test`.`t1` SELECT * FROM test.t1",
            "insert into t1 with label abc select * from t1| " +
                    "INSERT INTO `test`.`t1` WITH LABEL ? SELECT * FROM test.t1",

            // delete
            "delete from t1 where v4 = 1|DELETE FROM `test`.`t1` WHERE v4 = ?",

            // partition dml
            "insert into part_t1 partition (p1) values(1,2,3) " +
                    "|INSERT INTO `test`.`part_t1` PARTITION (p1) VALUES(?, ?, ?)",
            "insert overwrite part_t1 partition (p1) values(1,2,3) " +
                    "|INSERT OVERWRITE `test`.`part_t1` PARTITION (p1) VALUES(?, ?, ?)",

            // massive compounds
            "select * from t1 where v4=1 or v4=2 or v4=3 or v4=4 or v4=5 or v4=6 or v4=7 or v4=8 or v4=9 or v4=10 " +
                    "or v4=11 or v4=12 or v4=13 or v4=14 or v4=15 or v4=16 or v4=17 or v4=18 " +
                    "or v4=19 or v4=20| " +
                    "SELECT * FROM test.t1 WHERE $massive_compounds[`test`.`t1`.`v4`]$",
            "select * from t1 where v4+v5=1 or v4+v5=2 or v4+v5=3 or v4=4 or v4=5 or v4=6 or v4=7 or v4=8 or v4=9 or " +
                    "v4=10 " +
                    "or v4=11 or v4=12 or v4=13 or v4=14 or v4=15 or v4=16 or v4=17 or v4=18 " +
                    "or v4=19 or v4=20| " +
                    "SELECT * FROM test.t1 WHERE $massive_compounds[`test`.`t1`.`v4`,`test`.`t1`.`v5`]$",
            "select * from t1 where v5 = 123 and (v4=1 or v4=2 or v4=3 or v4=4 or v4=5 or v4=6 or v4=7 or v4=8 or " +
                    "v4=9 or v4=10 " +
                    "or v4=11 or v4=12 or v4=13 or v4=14 or v4=15 or v4=16 or v4=17 or v4=18 " +
                    "or v4=19 or v4=20)| " +
                    "SELECT * FROM test.t1 WHERE $massive_compounds[`test`.`t1`.`v4`,`test`.`t1`.`v5`]$",
    })
    public void testBuild(String sql, String expectedDigest) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertEquals(StringUtils.trim(expectedDigest), StringUtils.trim(SqlDigestBuilder.build(stmt)));
    }

}