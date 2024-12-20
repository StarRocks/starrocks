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

package com.starrocks.planner;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableFunctionTableSinkTest {
    @Test
    public void testTableFunctionTableSink() {
        List<Column> columns = ImmutableList.of(new Column("k1", Type.INT));
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "s3://path/to/directory/");
        properties.put("format", "csv");
        properties.put("compression", "uncompressed");
        properties.put("csv.column_separator", ",");
        properties.put("csv.row_delimiter", "\n");
        TableFunctionTable tableFunctionTable = new TableFunctionTable(columns, properties, new SessionVariable());

        TableFunctionTableSink tableFunctionTableSink = new TableFunctionTableSink(tableFunctionTable);

        assertTrue(tableFunctionTableSink.canUsePipeLine());
        assertTrue(tableFunctionTableSink.canUseRuntimeAdaptiveDop());
        assertFalse(tableFunctionTableSink.isWriteSingleFile());

        assertEquals("TABLE FUNCTION TABLE SINK\n" +
                "  PATH: s3://path/to/directory/\n" +
                "  FORMAT: csv\n" +
                "  PARTITION BY: []\n" +
                "  SINGLE: false\n" +
                "  RANDOM\n", tableFunctionTableSink.getExplainString("", TExplainLevel.NORMAL));
        TDataSink tDataSink = tableFunctionTableSink.toThrift();
        assertEquals(tDataSink.getType(), TDataSinkType.TABLE_FUNCTION_TABLE_SINK);
    }
}
