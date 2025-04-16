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
import org.junit.Assert;
import org.junit.Test;

public class DropCnGroupStmtTest {

    @Test
    public void testDropCnGroupStmt() {
        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP IF EXISTS cngroup1 FORCE";
            DropCnGroupStmt stmt =
                    (DropCnGroupStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assert.assertEquals("warehouse1", stmt.getWarehouseName());
            Assert.assertEquals("cngroup1", stmt.getCnGroupName());
            Assert.assertTrue(stmt.isSetIfExists());
            Assert.assertTrue(stmt.isSetForce());
        }

        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP IF EXISTS cngroup1";
            DropCnGroupStmt stmt =
                    (DropCnGroupStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assert.assertEquals("warehouse1", stmt.getWarehouseName());
            Assert.assertEquals("cngroup1", stmt.getCnGroupName());
            Assert.assertTrue(stmt.isSetIfExists());
            Assert.assertFalse(stmt.isSetForce());
        }

        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP cngroup1";
            DropCnGroupStmt stmt =
                    (DropCnGroupStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assert.assertEquals("warehouse1", stmt.getWarehouseName());
            Assert.assertEquals("cngroup1", stmt.getCnGroupName());
            Assert.assertFalse(stmt.isSetIfExists());
            Assert.assertFalse(stmt.isSetForce());
        }

        {
            String sqlText = "ALTER WAREHOUSE warehouse1 DROP CNGROUP";
            Assert.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
    }
}
