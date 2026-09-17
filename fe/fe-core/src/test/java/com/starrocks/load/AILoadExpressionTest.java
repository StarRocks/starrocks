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
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AILoadExpressionTest {
    @BeforeAll
    public static void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testLoadMappingRejectsAI() {
        AnalyzeTestUtil.getConnectContext().setThreadLocalInfo();
        Column column = new Column("answer", VarcharType.VARCHAR, true, null, true, null, "");
        Table table = new Table(1L, "load_target", Table.TableType.OLAP, List.of(column));
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        tuple.setTable(table);
        FunctionCallExpr ai = new FunctionCallExpr("ai_custom_query",
                List.of(new StringLiteral("provider"), new StringLiteral("prompt")));
        StarRocksException error = assertThrows(StarRocksException.class,
                () -> Load.initColumns(table, List.of(new ImportColumnDesc("answer", ai)), null,
                        new HashMap<>(), descriptors, tuple, new HashMap<>(), new TBrokerScanRangeParams(),
                        true, true, List.of(), false));
        assertTrue(error.getMessage().contains("AI functions are not supported in LOAD"));
    }
}
