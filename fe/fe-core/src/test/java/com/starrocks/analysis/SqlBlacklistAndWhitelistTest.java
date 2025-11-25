package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.sql.ast.ShowWhiteListStmt;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * TEST :
 * AddSqlBlackListStmt
 * DelSqlBlackListStmt
 * ShowSqlBlackListStmt
 * ShowWhiteListStmt
 */
public class SqlBlacklistAndWhitelistTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAddSqlBlacklist() throws Exception {
        AddSqlBlackListStmt stmt =
                (AddSqlBlackListStmt) analyzeSuccess("ADD SQLBLACKLIST \"select count\\(distinct .+\\) from .+\";");
        Assertions.assertEquals("select count(distinct .+) from .+", stmt.getSql());
        // bad cases
        //analyzeFail("add SQLBLACKLIST \"select from ?i)\";");

        stmt = (AddSqlBlackListStmt) SqlParser.parse("add SQLBLACKLIST \"select from ?i)\";",
                        AnalyzeTestUtil.getConnectContext().getSessionVariable())
                .get(0);
        StmtExecutor stmtExecutor = new StmtExecutor(AnalyzeTestUtil.getConnectContext(), stmt);
        stmtExecutor.execute();
        Assertions.assertTrue(AnalyzeTestUtil.getConnectContext().getState().isError());
    }

    @Test
    public void testDelSqlBlacklist() {
        DelSqlBlackListStmt stmt = (DelSqlBlackListStmt) analyzeSuccess("delete sqlblacklist  2, 6;");
        Assertions.assertEquals(Lists.asList(2L, new Long[] {6L}), stmt.getIndexs());
        // bad cases
        analyzeFail("DELETE SQLBLACKLIST");
    }

    @Test
    public void testShowSqlBlacklist() {
        ShowSqlBlackListStmt stmt = (ShowSqlBlackListStmt) analyzeSuccess("show sqlblacklist");
        Assertions.assertEquals(2, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("Id", new ShowResultMetaFactory().getMetadata(stmt).getColumn(0).getName());
        Assertions.assertEquals("Forbidden SQL", new ShowResultMetaFactory().getMetadata(stmt).getColumn(1).getName());

        // bad cases
        analyzeFail("show blacklist");
    }

    @Test
    public void testShowWhiteBlacklist() {
        ShowWhiteListStmt stmt = (ShowWhiteListStmt) analyzeSuccess("show whitelist");
        Assertions.assertEquals(2, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("user_name", new ShowResultMetaFactory().getMetadata(stmt).getColumn(0).getName());
        Assertions.assertEquals("white_list", new ShowResultMetaFactory().getMetadata(stmt).getColumn(1).getName());
    }
}
