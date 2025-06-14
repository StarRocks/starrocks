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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.HintNode;
import com.starrocks.qe.SessionVariable;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

class HintCollectorTest {

    private String hintStr = "/*+ set_var(abc = abc) */";

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("generateHint")
    void collect(String sql, int num) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.setSqlMode(32);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.SingleStatementContext singleStatementContext =
                parser.sqlStatements().singleStatement().get(0);
        HintCollector collector = new HintCollector((CommonTokenStream) parser.getTokenStream(), new SessionVariable());
        collector.collect(singleStatementContext);
        Assert.assertEquals(num, collector.getContextWithHintMap().size());
        for (List<HintNode> hintNodes : collector.getContextWithHintMap().values()) {
            Assert.assertEquals(1, hintNodes.size());
            Assert.assertEquals(hintStr, hintNodes.get(0).toSql());
        }
    }

    private static Stream<Arguments> generateHint() {
        List<Arguments> arguments = Lists.newArrayList();
        arguments.add(Arguments.of("select /*+ set_var(abc = abc) */ * from tbl union " +
                "select /*+ set_var(abc = abc) */ * from tbl", 2));
        arguments.add(Arguments.of("insert  /*+ set_var(abc = abc) */ into tbl " +
                "select /*+ set_var(abc = abc) */ * from (select /*+ set_var(abc = abc) */ * from tbl) t1", 3));
        arguments.add(Arguments.of("update /*+ set_var(abc = abc) */ tbl set col = 1 " +
                "from (select /*+ set_var(abc = abc) */ * from (select /*+ set_var(abc = abc) */ * from t1) t) t1 " +
                "where t1.col = tbl.col", 3));
        arguments.add(Arguments.of("delete /*+ set_var(abc = abc) */ from tbl " +
                "using (select /*+ set_var(abc = abc) */ * from (select /*+ set_var(abc = abc) */ * from t1) t) t " +
                "where tbl.col = t1.col", 3));
        arguments.add(Arguments.of("submit /*+ set_var(abc = abc) */ task " +
                "as create table temp as select count(*) as cnt from tbl1", 1));
        arguments.add(Arguments.of("LOAD /*+ set_var(abc = abc) */ LABEL test.testLabel " +
                "(DATA INFILE(\"hdfs://hdfs_host:hdfs_port/file\") " +
                "INTO TABLE `t0`) WITH BROKER hdfs_broker (\"username\"=\"sr\", \"password\"=\"PASSWORDDDD\") " +
                "PROPERTIES (\"strict_mode\"=\"true\")", 1));
        return arguments.stream();
    }
}