// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.starrocks.connector.analyzer.SimpleQueryAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParserAndAnalyzerAdaptiveTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testTrinoQuerySQL()  {
        ConnectContext connectorContext = AnalyzeTestUtil.getConnectContext();
        String sql = "select\n" +
                "    distinct user_id\n" +
                "from\n" +
                "    dw.dw_user_act_info_sd\n" +
                "where\n" +
                "    day >= cast((current_date - interval '7' day) as varchar)\n" +
                "    and age >= 18\n" +
                "    and substr(to_hex(md5(to_utf8(cast(user_id as varchar)))),32, 1) in ('C', 'D', 'E', 'F')";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectorContext);
            new SimpleQueryAnalyzer().analyze(statementBase);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
