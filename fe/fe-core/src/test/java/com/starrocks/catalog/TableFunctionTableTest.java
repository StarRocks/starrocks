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

package com.starrocks.catalog;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableFunctionTableTest {
    @Test
    public void testNormal() {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "fake://some_bucket/some_path/*");
        properties.put("format", "ORC");
        properties.put("columns_from_path", "col_path1, col_path2,   col_path3");

        Assertions.assertDoesNotThrow(() -> {
            TableFunctionTable table = new TableFunctionTable(properties);
            List<Column> schema = table.getFullSchema();
            Assertions.assertEquals(5, schema.size());
            Assertions.assertEquals(new Column("col_int", Type.INT), schema.get(0));
            Assertions.assertEquals(new Column("col_string", Type.VARCHAR), schema.get(1));
            Assertions.assertEquals(new Column("col_path1", ScalarType.createDefaultString(), true), schema.get(2));
            Assertions.assertEquals(new Column("col_path2", ScalarType.createDefaultString(), true), schema.get(3));
            Assertions.assertEquals(new Column("col_path3", ScalarType.createDefaultString(), true), schema.get(4));
        });
    }
}
