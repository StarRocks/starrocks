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
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.FileType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class FileFunctionAnalyzerTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @ParameterizedTest
    @ValueSource(strings = {"to_file", "try_to_file"})
    public void testFieldTypes(String functionName) {
        QueryStatement statement = (QueryStatement) analyzeSuccess(
                "select " + functionName + "(named_struct('SIZE', 3, 'uri', NULL, 'offset', cast(0 as smallint)))");
        FunctionCallExpr call = (FunctionCallExpr)
                ((SelectRelation) statement.getQueryRelation()).getOutputExpression().get(0);
        Assertions.assertEquals(FileType.FILE, call.getType());
        Assertions.assertEquals(functionName, call.getFn().functionName());
        Assertions.assertTrue(call.isNullable());
        Assertions.assertTrue(new CallOperator(functionName, call.getType(),
                List.of(new ColumnRefOperator(1, call.getFn().getArgs()[0], "s", false)), call.getFn()).isNullable());
        StructType argument = (StructType) call.getFn().getArgs()[0];
        Assertions.assertEquals(IntegerType.BIGINT, argument.getFields().get(0).getType());
        Assertions.assertEquals(VarcharType.VARCHAR, argument.getFields().get(1).getType());
        Assertions.assertEquals(IntegerType.BIGINT, argument.getFields().get(2).getType());
        for (String type : new String[] {"tinyint", "smallint", "int", "bigint"}) {
            analyzeSuccess("select " + functionName + "(named_struct('size', cast(1 as " + type + ")))");
        }
        analyzeSuccess("select " + functionName + "(named_struct('uri', cast('a' as char(1)), " +
                "'inline', to_binary('610062', 'hex'), 'checksum', '', 'content_type', 'image/jpeg'))");
        analyzeSuccess("select " + functionName + "(named_struct('size', NULL, 'inline', NULL))");
        analyzeSuccess("select " + functionName + "(NULL)");
        analyzeSuccess("select " + functionName + "(cast(NULL as struct<uri varchar(20)>))");
        analyzeSuccess("select " + functionName + "(s) from (select named_struct('uri', ta, 'size', tb) s from tall) t");
    }

    @ParameterizedTest
    @ValueSource(strings = {"to_file", "try_to_file"})
    public void testInvalidFields(String functionName) {
        analyzeFail("select " + functionName + "(named_struct('path', 'a'))", "unknown field 'path'");
        analyzeFail("select " + functionName + "(named_struct('URI', 'a', 'uri', 'b'))", "duplicate subfield name");
        for (String value : new String[] {"'3'", "1.5", "true", "cast(1 as largeint)", "[1]"}) {
            analyzeFail("select " + functionName + "(named_struct('size', " + value + "))", "unsupported type");
        }
        analyzeFail("select " + functionName + "(named_struct('uri', 1))", "unsupported type");
        analyzeFail("select " + functionName + "(named_struct('inline', 'bytes'))", "unsupported type");
        analyzeFail("select " + functionName + "('a')");
        analyzeFail("select " + functionName + "()");
        analyzeFail("select " + functionName + "(named_struct('uri', 'a'), 1)");
    }

    @ParameterizedTest
    @ValueSource(strings = {"to_file", "try_to_file"})
    public void testPlanning(String functionName) throws Exception {
        for (String sql : new String[] {
                "select " + functionName + "(named_struct('size', 3, 'inline', NULL))",
                "select " + functionName + "(NULL)",
                "select " + functionName + "(named_struct('uri', cast('a' as char(1))))",
                "select " + functionName + "(named_struct('uri', NULL)) is null",
                "select " + functionName + "(named_struct('uri', ta, 'size', tb)) from tall",
                "select " + functionName + "(cast(NULL as struct<uri varchar(20)>))"}) {
            UtFrameUtils.getFragmentPlan(AnalyzeTestUtil.getConnectContext(), sql);
        }
    }

    @Test
    public void testGetters() throws Exception {
        String[] names = {"fl_get_uri", "fl_get_offset", "fl_get_size", "fl_get_content_type",
                "fl_get_checksum", "fl_get_inline"};
        Type[] types = {VarcharType.VARCHAR, IntegerType.BIGINT, IntegerType.BIGINT, VarcharType.VARCHAR,
                VarcharType.VARCHAR, VarbinaryType.VARBINARY};
        for (int i = 0; i < names.length; ++i) {
            String name = names[i];
            String sql = "select " + name + "(to_file(named_struct('uri', 'a')))";
            QueryStatement statement = (QueryStatement) analyzeSuccess(sql);
            FunctionCallExpr call = (FunctionCallExpr)
                    ((SelectRelation) statement.getQueryRelation()).getOutputExpression().get(0);
            Assertions.assertEquals(types[i], call.getType());
            Assertions.assertEquals(FileType.FILE, call.getFn().getArgs()[0]);
            Assertions.assertTrue(call.isNullable());
            // A non-NULL FILE can still have a NULL field.
            Assertions.assertTrue(new CallOperator(name, call.getType(),
                    List.of(new ColumnRefOperator(1, FileType.FILE, "f", false)), call.getFn()).isNullable());
            analyzeSuccess("select " + name + "(NULL)");
            analyzeFail("select " + name + "('a')");
            analyzeFail("select " + name + "(1)");
            analyzeFail("select " + name + "(named_struct('uri', 'a'))");
            analyzeFail("select " + name + "()");
            analyzeFail("select " + name + "(NULL, NULL)");
            UtFrameUtils.getFragmentPlan(AnalyzeTestUtil.getConnectContext(), sql);
            UtFrameUtils.getFragmentPlan(AnalyzeTestUtil.getConnectContext(),
                    "select " + name + "(try_to_file(named_struct('uri', ta, 'size', tb))) is null from tall");
        }
    }
}
