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

package com.starrocks.sql.ast.warehouse.cngroup;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DropCnGroupStmtTest {

    @Test
    public void testDropCnGroupStmt() {
        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP IF EXISTS cngroup1 FORCE";
            DropCnGroupStmt stmt =
                    (DropCnGroupStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertEquals("warehouse1", stmt.getWarehouseName());
            Assertions.assertEquals("cngroup1", stmt.getCnGroupName());
            Assertions.assertTrue(stmt.isSetIfExists());
            Assertions.assertTrue(stmt.isSetForce());
        }

        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP IF EXISTS cngroup1";
            DropCnGroupStmt stmt =
                    (DropCnGroupStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertEquals("warehouse1", stmt.getWarehouseName());
            Assertions.assertEquals("cngroup1", stmt.getCnGroupName());
            Assertions.assertTrue(stmt.isSetIfExists());
            Assertions.assertFalse(stmt.isSetForce());
        }

        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP cngroup1";
            DropCnGroupStmt stmt =
                    (DropCnGroupStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertEquals("warehouse1", stmt.getWarehouseName());
            Assertions.assertEquals("cngroup1", stmt.getCnGroupName());
            Assertions.assertFalse(stmt.isSetIfExists());
            Assertions.assertFalse(stmt.isSetForce());
        }

        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP";
            Assertions.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
    }
}
