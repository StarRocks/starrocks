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
import com.starrocks.common.conf.Config;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
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

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class ParserBench {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParserBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    private String sql;

    @Setup
    public void setup() {
        sql = generateSQL();
    }

    @Param({"SLL", "LL"})
    public String mode;

    @Param({"100", "1000", "5000", "10000"})
    public int times;

    @Param({"true", "false"})
    public boolean isRightSql;

    @Param({"true", "false"})
    public boolean isLimit;

    @Benchmark
    public void parseInsertIntoValues() {
        parseSql(sql);
    }

    private String generateSQL() {
        List<String> wrongValues = Lists.newArrayList("K0.14044384266968246155471433667116798460483551025390625",
                "-1869445626", "K0.17698452552099786", "K127", "k-366217216");
        List<String> rightValues = Lists.newArrayList("0.14044384266968246155471433667116798460483551025390625",
                "-1869445626", "0.17698452552099786", "127", "-366217216");

        String joined;
        if (isRightSql) {
            joined = String.join(",", rightValues);
        } else {
            joined = String.join(",", wrongValues);
        }
        StringJoiner result = new StringJoiner(",", "(", ")");
        for (int i = 0; i < times; i++) {
            result.add(joined);
        }
        return "INSERT INTO test_load_decimal_1_0 VALUES " + result + ";";
    }

    private StatementBase parseSql(String sql) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener());
        parser.removeParseListeners();
        if (isLimit) {
            parser.addParseListener(new PostProcessListener(100000000, Config.expr_children_limit));
        }
        parser.getInterpreter().setPredictionMode(mode.equals("SLL") ? PredictionMode.SLL : PredictionMode.LL);
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        return (StatementBase) new AstBuilder(SqlModeHelper.MODE_DEFAULT)
                .visitSingleStatement(sqlStatements.singleStatement(0));
    }

}
