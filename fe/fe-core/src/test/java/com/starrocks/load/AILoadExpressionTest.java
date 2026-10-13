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

package com.starrocks.load;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AILoadExpressionTest {
    private static String previousEndpoint;
    private static String previousModel;
    private static String previousProvider;

    @BeforeAll
    public static void setUp() throws Exception {
        AnalyzeTestUtil.init();
        previousEndpoint = Config.ai_default_chat_endpoint;
        previousModel = Config.ai_default_chat_model;
        previousProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "load-test-model";
        Config.ai_default_chat_provider = "openai_compatible";
    }

    @AfterAll
    public static void restoreAIConfiguration() {
        Config.ai_default_chat_endpoint = previousEndpoint;
        Config.ai_default_chat_model = previousModel;
        Config.ai_default_chat_provider = previousProvider;
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_complete('prompt')", "upper(ai_complete('prompt'))"})
    public void testLoadMappingRejectsAI(String expression) {
        AnalyzeTestUtil.getConnectContext().setThreadLocalInfo();
        StarRocksException error = assertThrows(StarRocksException.class, () -> initMapping(expression));
        assertTrue(error.getMessage().contains("AI functions are not supported in LOAD"));
    }

    @Test
    public void testLoadMappingWithoutContextRejectsAIWithAnalysisError() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        try {
            assertDoesNotThrow(() -> initMapping("upper('prompt')"));
            StarRocksException error = assertThrows(StarRocksException.class, () -> initMapping("ai_complete('prompt')"));
            assertTrue(error.getMessage().contains("execution context"), error.getMessage());
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private static void initMapping(String expression) throws StarRocksException {
        Column column = new Column("answer", VarcharType.VARCHAR, true, null, true, null, "");
        Table table = new Table(1L, "load_target", Table.TableType.OLAP, List.of(column));
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        tuple.setTable(table);
        Expr ai = SqlParser.parseSqlToExpr(expression, AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
        Load.initColumns(table, List.of(new ImportColumnDesc("answer", ai)), null,
                new HashMap<>(), descriptors, tuple, new HashMap<>(), new TBrokerScanRangeParams(),
                true, true, List.of(), false);
    }
}
