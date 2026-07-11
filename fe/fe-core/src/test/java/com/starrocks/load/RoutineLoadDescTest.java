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

import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RoutineLoadDescTest {
    @Test
    public void testToSql() throws Exception {
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo("CREATE ROUTINE LOAD job ON tbl " +
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
        Assertions.assertEquals("COLUMNS TERMINATED BY ';'", desc.toSql());
        // set row delimiter and check
        desc.setRowDelimiter(originLoad.getRowDelimiter());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n'", desc.toSql());
        // set columns and check
        desc.setColumnsInfo(originLoad.getColumnsInfo());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1)", desc.toSql());
        // set partitions and check
        desc.setPartitionNames(originLoad.getPartitionNames());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), " +
                        "TEMPORARY PARTITION(`p1`, `p2`)",
                desc.toSql());
        // set where and check
        desc.setWherePredicate(originLoad.getWherePredicate());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), " +
                        "TEMPORARY PARTITION(`p1`, `p2`), " +
                        "WHERE `a` = 1",
                desc.toSql());
    }

    @Test
    public void testIncludeMetadataRoundTrip() throws Exception {
        // parse + AstBuilder.visitIncludeMetadata + buildLoadDesc populate the clause.
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " +
                        "INCLUDE METADATA(KEY AS k, PARTITION AS p, OFFSET AS o, HEADERS AS h), " +
                        "COLUMNS(a, b) " +
                        "PROPERTIES (\"format\"=\"json\") " +
                        "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);

        Assertions.assertNotNull(originLoad.getMetadata());
        Assertions.assertEquals(4, originLoad.getMetadata().getItems().size());
        Assertions.assertEquals("KEY", originLoad.getMetadata().getItems().get(0).getKey());
        Assertions.assertEquals("k", originLoad.getMetadata().getItems().get(0).getAlias());

        // toSql renders the clause (write leg of the persistence round-trip).
        RoutineLoadDesc desc = new RoutineLoadDesc();
        desc.setMetadata(originLoad.getMetadata());
        Assertions.assertEquals("INCLUDE METADATA(KEY AS `k`, PARTITION AS `p`, OFFSET AS `o`, HEADERS AS `h`)",
                desc.toSql());

        // re-parse the rendered SQL (read leg) -> the clause survives an origStmt round-trip.
        RoutineLoadDesc reparsed = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " + desc.toSql() +
                        " PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);
        Assertions.assertNotNull(reparsed.getMetadata());
        Assertions.assertEquals(4, reparsed.getMetadata().getItems().size());
        Assertions.assertEquals("OFFSET", reparsed.getMetadata().getItems().get(2).getKey());
        Assertions.assertEquals("o", reparsed.getMetadata().getItems().get(2).getAlias());

        // A reserved-word alias round-trips because ParseUtil.backquote quotes it; without the backquotes
        // the rendered `AS from` would fail to re-parse.
        RoutineLoadDesc reserved = new RoutineLoadDesc();
        reserved.setMetadata(CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl INCLUDE METADATA(KEY AS `from`), COLUMNS(a, b) " +
                        "PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null)
                .getMetadata());
        Assertions.assertEquals("INCLUDE METADATA(KEY AS `from`)", reserved.toSql());
        RoutineLoadDesc reservedReparsed = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " + reserved.toSql() +
                        " PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);
        Assertions.assertEquals("from", reservedReparsed.getMetadata().getItems().get(0).getAlias());
    }
}
