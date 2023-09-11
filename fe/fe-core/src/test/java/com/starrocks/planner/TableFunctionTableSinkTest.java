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
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TExplainLevel;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableFunctionTableSinkTest {
    @Test
    public void testTableFunctionTableSink() {
        TableFunctionTable tableFunctionTable = new TableFunctionTable("s3://path/to/directory/", "parquet",
                "uncompressed", ImmutableList.of(new Column("k1", Type.INT)), null, false,
                ImmutableMap.of());

        TableFunctionTableSink tableFunctionTableSink = new TableFunctionTableSink(tableFunctionTable);

        assertTrue(tableFunctionTableSink.canUsePipeLine());
        assertTrue(tableFunctionTableSink.canUseRuntimeAdaptiveDop());
        assertFalse(tableFunctionTableSink.isWriteSingleFile());

        assertEquals("TABLE FUNCTION TABLE SINK\n" +
                "  PATH: s3://path/to/directory/\n" +
                "  FORMAT: parquet\n" +
                "  PARTITION BY: []\n" +
                "  SINGLE: false\n" +
                "  RANDOM\n", tableFunctionTableSink.getExplainString("", TExplainLevel.NORMAL));
        TDataSink tDataSink = tableFunctionTableSink.toThrift();
        assertEquals("TDataSink(type:TABLE_FUNCTION_TABLE_SINK, " +
                "table_function_table_sink:TTableFunctionTableSink(" +
                "target_table:TTableFunctionTable(path:s3://path/to/directory/, " +
                "columns:[TColumn(column_name:k1, is_key:false, is_allow_null:false, " +
                "is_auto_increment:false, col_unique_id:-1, index_len:4, " +
                "type_desc:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:INT))]))], " +
                "file_format:parquet, compression_type:NO_COMPRESSION, write_single_file:false), " +
                "cloud_configuration:TCloudConfiguration(cloud_type:DEFAULT)))", tDataSink.toString());
    }
}
