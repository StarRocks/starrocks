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


package com.starrocks.sql.analyzer;


import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TruncatePartitionClause;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.CaseInsensitiveStream;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.parser.StarRocksLexer;
import com.starrocks.sql.parser.StarRocksParser;
import com.starrocks.utframe.UtFrameUtils;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

public class AstBuilderTest {

    private static ConnectContext connectContext;


    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
    }

    @Test
    public void testModifyBackendHost() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        String sql = "alter system modify backend host '127.0.0.1' to 'testHost'";
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        StatementBase statement = (StatementBase) new AstBuilder(32).visitSingleStatement(sqlStatements.singleStatement(0));
        Field field = statement.getClass().getDeclaredField("alterClause");
        field.setAccessible(true);
        ModifyBackendAddressClause clause = (ModifyBackendAddressClause) field.get(statement);
        Assert.assertTrue(clause.getSrcHost().equals("127.0.0.1") && clause.getDestHost().equals("testHost"));
    }

    @Test
    public void testModifyFrontendHost() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        String sql = "alter system modify frontend host '127.0.0.1' to 'testHost'";
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        StatementBase statement = (StatementBase) new AstBuilder(32).visitSingleStatement(sqlStatements.singleStatement(0));
        Field field = statement.getClass().getDeclaredField("alterClause");
        field.setAccessible(true);
        ModifyFrontendAddressClause clause = (ModifyFrontendAddressClause) field.get(statement);
        Assert.assertTrue(clause.getSrcHost().equals("127.0.0.1") && clause.getDestHost().equals("testHost"));
    }

    @Test
    public void testTruncatePartition() throws Exception {
        String sql = "alter table db.test truncate partition p1";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        AlterTableStmt aStmt = (AlterTableStmt) statement;
        List<AlterClause> alterClauses = aStmt.getOps();
        TruncatePartitionClause c = (TruncatePartitionClause) alterClauses.get(0);
        Assert.assertTrue(c.getPartitionNames().getPartitionNames().get(0).equals("p1"));
    }
}
