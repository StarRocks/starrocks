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

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionExternalStateAnalyzerTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testDirectAndNestedDictionaryExpressionsRejected() {
        for (KeysType keysType : KeysType.values()) {
            for (String sql : new String[] {
                    "dict_mapping('test.partition_dict', k, 'part')",
                    "DICT_MAPPING('test.partition_dict', k, 'part')",
                    "abs(dict_mapping('test.partition_dict', k, 'part'))",
                    "coalesce(dict_mapping('test.partition_dict', k, 'part'), 0)",
                    "dictionary_get('partition_dictionary', k)",
                    "dictionary_get('partition_dictionary', k)[1]",
                    "abs(dictionary_get('partition_dictionary', k)[1])"
            }) {
                Expr expression = SqlParser.parseSqlToExpr(sql, 0);
                ListPartitionDesc desc = new ListPartitionDesc(List.of("k"), List.of());
                desc.setPartitionExprs(List.of(expression));
                AnalysisException error = assertThrows(AnalysisException.class,
                        () -> PartitionDescAnalyzer.analyzeListPartitionDesc(
                                desc, List.of(keyColumn()), Map.of(), keysType));
                String function = sql.toLowerCase(Locale.ROOT).contains("dict_mapping") ? "dict_mapping" : "dictionary_get";
                assertTrue(error.getMessage().contains("should not depend on mutable dictionary state"), error.getMessage());
                assertTrue(error.getMessage().toLowerCase(Locale.ROOT).contains(function), error.getMessage());
            }
        }
    }

    @Test
    public void testIdentifierPartitionChecksTheGeneratedExpression() {
        for (KeysType keysType : KeysType.values()) {
            for (String sql : new String[] {
                    "dict_mapping('test.partition_dict', k, 'part')",
                    "abs(dict_mapping('test.partition_dict', k, 'part'))",
                    "dictionary_get('partition_dictionary', k)[1]",
                    "abs(dictionary_get('partition_dictionary', k)[1])"
            }) {
                Expr expression = SqlParser.parseSqlToExpr(sql, 0);
                ColumnDef generated = new ColumnDef("g", new TypeDef(IntegerType.BIGINT), null, false,
                        null, null, true, ColumnDef.DefaultValueDef.NOT_SET, null, expression, "");
                ListPartitionDesc desc = new ListPartitionDesc(List.of("g"), List.of());
                AnalysisException error = assertThrows(AnalysisException.class,
                        () -> PartitionDescAnalyzer.analyzeListPartitionDesc(
                                desc, List.of(keyColumn(), generated), Map.of(), keysType));
                String function = sql.contains("dict_mapping") ? "dict_mapping" : "dictionary_get";
                assertTrue(error.getMessage().contains("should not depend on mutable dictionary state"), error.getMessage());
                assertTrue(error.getMessage().toLowerCase(Locale.ROOT).contains(function), error.getMessage());
            }
        }
    }

    @Test
    public void testRowOnlyExpressionRetainedAndGlobalFunctionClassificationUnchanged() {
        for (KeysType keysType : KeysType.values()) {
            ListPartitionDesc desc = new ListPartitionDesc(List.of("k"), List.of());
            desc.setPartitionExprs(List.of(SqlParser.parseSqlToExpr("abs(k)", 0)));
            assertDoesNotThrow(() -> PartitionDescAnalyzer.analyzeListPartitionDesc(
                    desc, List.of(keyColumn()), Map.of(), keysType));
        }
        // Dictionary lookups remain valid outside partitioning; do not change the global ND classifier.
        assertFalse(AnalyzerUtils.containsNonDeterministicFunction(
                SqlParser.parseSqlToExpr("dict_mapping('test.partition_dict', k, 'part')", 0)).first);
        assertFalse(AnalyzerUtils.containsNonDeterministicFunction(
                SqlParser.parseSqlToExpr("dictionary_get('partition_dictionary', k)[1]", 0)).first);
    }

    private ColumnDef keyColumn() {
        return new ColumnDef("k", new TypeDef(IntegerType.BIGINT), true, null, null,
                false, ColumnDef.DefaultValueDef.NOT_SET, "");
    }
}
