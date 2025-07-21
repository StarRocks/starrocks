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

package com.starrocks.sql.ast.warehouse;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowNodesStmtTest {

    @Test
    public void testShowNodesFromWarehouse() {
        {
            String sqlText = "SHOW NODES FROM WAREHOUSES LIKE '%query%'";
            ShowNodesStmt stmt =
                    (ShowNodesStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertNull(stmt.getWarehouseName());
            Assertions.assertTrue(stmt.getCnGroupName().isEmpty());
            Assertions.assertEquals("%query%", stmt.getPattern());
        }

        {
            String sqlText = "SHOW NODES FROM WAREHOUSE warehouse1";
            ShowNodesStmt stmt =
                    (ShowNodesStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertEquals("warehouse1", stmt.getWarehouseName());
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertTrue(stmt.getCnGroupName().isEmpty());
        }

        {
            String sqlText = "SHOW NODES FROM WAREHOUSE warehouse1 CNGROUP group2";
            ShowNodesStmt stmt =
                    (ShowNodesStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertEquals("warehouse1", stmt.getWarehouseName());
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertEquals("group2", stmt.getCnGroupName());
        }

        {
            String sqlText = "SHOW NODES FROM WAREHOUSE warehouse1 CNGROUP";
            Assertions.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
    }
}
