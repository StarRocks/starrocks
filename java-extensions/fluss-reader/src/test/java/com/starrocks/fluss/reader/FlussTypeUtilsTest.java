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

package com.starrocks.fluss.reader;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class FlussTypeUtilsTest {

    @Test
    public void testTypeMappings() {
        Map<DataType, String> mappings = new LinkedHashMap<>();
        mappings.put(DataTypes.CHAR(8), "string");
        mappings.put(DataTypes.STRING(), "string");
        mappings.put(DataTypes.BOOLEAN(), "boolean");
        mappings.put(DataTypes.BINARY(8), "binary");
        mappings.put(DataTypes.BYTES(), "binary");
        mappings.put(DataTypes.DECIMAL(10, 2), "decimal(10,2)");
        mappings.put(DataTypes.TINYINT(), "tinyint");
        mappings.put(DataTypes.SMALLINT(), "short");
        mappings.put(DataTypes.INT(), "int");
        mappings.put(DataTypes.BIGINT(), "bigint");
        mappings.put(DataTypes.FLOAT(), "float");
        mappings.put(DataTypes.DOUBLE(), "double");
        mappings.put(DataTypes.DATE(), "date");
        mappings.put(DataTypes.TIME(), "time");
        mappings.put(DataTypes.TIMESTAMP(), "timestamp-millis");
        mappings.put(DataTypes.TIMESTAMP_LTZ(), "timestamp-millis");
        mappings.put(DataTypes.ARRAY(DataTypes.INT()), "array<int>");
        mappings.put(DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()), "map<string,bigint>");
        mappings.put(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING()))),
                "struct<id:int,tags:array<string>>");

        mappings.forEach((type, expected) ->
                Assertions.assertEquals(expected, FlussTypeUtils.fromFlussType(type), type.toString()));
    }
}
