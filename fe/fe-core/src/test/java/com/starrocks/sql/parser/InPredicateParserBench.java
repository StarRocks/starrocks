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

import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.IdentityHashMap;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(time = 1, iterations = 3)
@Measurement(time = 5, iterations = 3)
public class InPredicateParserBench {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(InPredicateParserBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    private String sql;

    @Setup
    public void setup() {
        sql = generateSQL();
    }

    @Param({"5000", "50000", "200000"})
    public int count;

    @Param({"false", "true"})
    public boolean positive;

    public final int baseNumber = 10_000_000;

    @Benchmark
    public void parseInPredicate() {
        parseSql(sql);
    }

    private String generateSQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM t WHERE c1 IN (");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(",");
            }
            if (positive) {
                sb.append(baseNumber + i);
            } else {
                sb.append(-i - baseNumber);
            }
        }
        sb.append(") AND c2 > 1;");
        return sb.toString();
    }

    private StatementBase parseSql(String sql) {
        com.starrocks.sql.parser.StarRocksLexer lexer =
                new com.starrocks.sql.parser.StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        com.starrocks.sql.parser.StarRocksParser parser = new com.starrocks.sql.parser.StarRocksParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener());
        parser.removeParseListeners();
        com.starrocks.sql.parser.StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        return (StatementBase) new AstBuilder(SqlModeHelper.MODE_DEFAULT)
                .visitSingleStatement(sqlStatements.singleStatement(0));
    }
}

