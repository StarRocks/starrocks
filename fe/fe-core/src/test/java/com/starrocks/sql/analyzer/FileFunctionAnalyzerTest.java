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

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.type.FileType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class FileFunctionAnalyzerTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testFieldTypes() {
        QueryStatement statement = (QueryStatement) analyzeSuccess(
                "select to_file(named_struct('SIZE', 3, 'uri', NULL, 'offset', cast(0 as smallint)))");
        FunctionCallExpr call = (FunctionCallExpr)
                ((SelectRelation) statement.getQueryRelation()).getOutputExpression().get(0);
        Assertions.assertEquals(FileType.FILE, call.getType());
        StructType argument = (StructType) call.getFn().getArgs()[0];
        Assertions.assertEquals(IntegerType.BIGINT, argument.getFields().get(0).getType());
        Assertions.assertEquals(VarcharType.VARCHAR, argument.getFields().get(1).getType());
        Assertions.assertEquals(IntegerType.BIGINT, argument.getFields().get(2).getType());
        for (String type : new String[] {"tinyint", "smallint", "int", "bigint"}) {
            analyzeSuccess("select to_file(named_struct('size', cast(1 as " + type + ")))");
        }
        analyzeSuccess("select to_file(named_struct('uri', cast('a' as char(1)), 'inline', to_binary('610062', 'hex'), " +
                "'checksum', '', 'content_type', 'image/jpeg'))");
        analyzeSuccess("select to_file(named_struct('size', NULL, 'inline', NULL))");
        analyzeSuccess("select to_file(NULL)");
        analyzeSuccess("select to_file(cast(NULL as struct<uri varchar(20)>))");
        analyzeSuccess("select to_file(s) from (select named_struct('uri', ta, 'size', tb) s from tall) t");
    }

    @Test
    public void testInvalidFields() {
        analyzeFail("select to_file(named_struct('path', 'a'))", "unknown field 'path'");
        analyzeFail("select to_file(named_struct('URI', 'a', 'uri', 'b'))", "duplicate subfield name");
        for (String value : new String[] {"'3'", "1.5", "true", "cast(1 as largeint)", "[1]"}) {
            analyzeFail("select to_file(named_struct('size', " + value + "))", "unsupported type");
        }
        analyzeFail("select to_file(named_struct('uri', 1))", "unsupported type");
        analyzeFail("select to_file(named_struct('inline', 'bytes'))", "unsupported type");
        analyzeFail("select to_file('a')");
        analyzeFail("select to_file()");
        analyzeFail("select to_file(named_struct('uri', 'a'), 1)");
    }

    @Test
    public void testPlanning() throws Exception {
        for (String sql : new String[] {
                "select to_file(named_struct('size', 3, 'inline', NULL))",
                "select to_file(NULL)",
                "select to_file(named_struct('uri', cast('a' as char(1))))",
                "select to_file(named_struct('uri', NULL)) is null",
                "select to_file(named_struct('uri', ta, 'size', tb)) from tall",
                "select to_file(cast(NULL as struct<uri varchar(20)>))"}) {
            UtFrameUtils.getFragmentPlan(AnalyzeTestUtil.getConnectContext(), sql);
        }
    }
}
