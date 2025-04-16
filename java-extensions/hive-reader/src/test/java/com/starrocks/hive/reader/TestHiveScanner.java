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

package com.starrocks.hive.reader;

import com.starrocks.jni.connector.OffHeapTable;
import com.starrocks.utils.Platform;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestHiveScanner {

    @BeforeEach
    public void setUp() {
        System.setProperty(Platform.UT_KEY, Boolean.TRUE.toString());
    }

    @AfterEach
    public void tearDown() {
        System.setProperty(Platform.UT_KEY, Boolean.FALSE.toString());
    }

    Map<String, String> createScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHiveScanner.class.getResource("/test_primitive_type");
        String basePath = resource.getPath().toString();
        String filePath = basePath + "/row_1.avro";
        File file = new File(filePath);
        params.put("data_file_path", filePath);
        params.put("block_offset", "0");
        params.put("block_length", String.valueOf(file.length()));
        params.put("hive_column_names",
                "col_tinyint,col_smallint,col_int,col_bigint,col_float,col_double,col_decimal,col_string,col_char," +
                        "col_varchar,col_boolean,col_timestamp,col_date,col_array,col_map,col_struct");
        params.put("hive_column_types",
                "int#int#int#bigint#float#double#decimal(10,2)#string#char(10)#varchar(20)#boolean#timestamp" +
                        "#date#array<string>#map<string,int>#struct<name:string,age:int,gender:string>");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
        params.put("serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        params.put("required_fields", "col_tinyint,col_smallint,col_int,col_bigint,col_float,col_double," +
                "col_decimal,col_struct");
        params.put("SerDe.mongo.columns.mapping", "{\n\"id\":\"_id\",\n\"status\":\"status\"}");
        return params;
    }

    Map<String, String> createComplexTypeScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHiveScanner.class.getResource("/test_complex_type");
        String basePath = resource.getPath().toString();
        String filePath = basePath + "/complex_type_test.avro";
        File file = new File(filePath);
        params.put("data_file_path", filePath);
        params.put("block_offset", "0");
        params.put("block_length", String.valueOf(file.length()));
        params.put("hive_column_names",
                "id,array_col,map_col,struct_col");
        params.put("hive_column_types",
                "string#array<string>#map<string,string>#struct<uid:string,device_list:array<string>," +
                        "info:map<string,string>>");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
        params.put("serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        params.put("required_fields", "id,array_col,map_col,struct_col");
        return params;
    }

    Map<String, String> createStructScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHiveScanner.class.getResource("/test_complex_type");
        String basePath = resource.getPath().toString();
        String filePath = basePath + "/struct_test.avro";
        File file = new File(filePath);
        params.put("data_file_path", filePath);
        params.put("block_offset", "0");
        params.put("block_length", String.valueOf(file.length()));
        params.put("hive_column_names",
                "test");
        params.put("hive_column_types",
                "array<struct<a:int,b:int>>");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
        params.put("serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        params.put("required_fields", "test");
        return params;
    }

    String runStructScanOnParams(Map<String, String> params) throws Exception {
        HiveScanner scanner = new HiveScanner(4096, params);
        System.out.println(scanner.toString());
        scanner.open();
        StringBuilder sb = new StringBuilder();
        while (true) {
            scanner.getNextOffHeapChunk();
            OffHeapTable table = scanner.getOffHeapTable();
            if (table.getNumRows() == 0) {
                break;
            }
            table.show(10);
            sb.append(table.dump(10));
            table.checkTableMeta(true);
            Assertions.assertTrue(table.checkNullsLength());
            table.close();
        }
        scanner.close();
        return sb.toString();
    }

    String runComplexTypeScanOnParams(Map<String, String> params) throws Exception {
        HiveScanner scanner = new HiveScanner(4096, params);
        System.out.println(scanner.toString());
        scanner.open();
        StringBuilder sb = new StringBuilder();
        while (true) {
            scanner.getNextOffHeapChunk();
            OffHeapTable table = scanner.getOffHeapTable();
            if (table.getNumRows() == 0) {
                break;
            }
            table.show(10);
            sb.append(table.dump(10));
            table.checkTableMeta(true);
            table.close();
        }
        scanner.close();
        return sb.toString();
    }

    String runScanOnParams(Map<String, String> params) throws IOException {
        HiveScanner scanner = new HiveScanner(4096, params);
        System.out.println(scanner.toString());
        Assertions.assertTrue(scanner.toString().contains("mongo.columns.mapping"));
        scanner.open();
        StringBuilder sb = new StringBuilder();
        while (true) {
            scanner.getNextOffHeapChunk();
            OffHeapTable table = scanner.getOffHeapTable();
            if (table.getNumRows() == 0) {
                break;
            }
            table.show(10);
            sb.append(table.dump(10));
            table.checkTableMeta(true);
            table.close();
        }
        scanner.close();
        return sb.toString();
    }

    @Test
    public void c1DoScanTestOnPrimitiveType() throws IOException {
        Map<String, String> params = createScanTestParams();
        runScanOnParams(params);
    }

    @Test
    public void complexTypeTest() throws Exception {
        Map<String, String> params = createComplexTypeScanTestParams();
        // if error, NegativeArraySizeException will be throw
        runComplexTypeScanOnParams(params);

        // if error, AssertionError will be throw
        params = createStructScanTestParams();
        runStructScanOnParams(params);
    }
}
