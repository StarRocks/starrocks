package com.starrocks.hive.reader;

import com.starrocks.jni.connector.OffHeapTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestHiveScanner {

    @Before
    public void setUp() {
        System.setProperty("starrocks.fe.test", "1");
    }

    @After
    public void tearDown() {
        System.setProperty("starrocks.fe.test", "0");
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
                "col_tinyint,col_smallint,col_int,col_bigint,col_float,col_double,col_decimal,col_string,col_char,col_varchar,col_boolean,col_timestamp,col_date,col_array,col_map,col_struct");
        params.put("hive_column_types",
                "int#int#int#bigint#float#double#decimal(10,2)#string#char(10)#varchar(20)#boolean#timestamp#date#array<string>#map<string,int>#struct<name:string,age:int>");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
        params.put("serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        params.put("required_fields", "col_tinyint,col_smallint,col_int,col_bigint,col_float,col_double,col_decimal");
        return params;
    }

    String runScanOnParams(Map<String, String> params) throws IOException {
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

    @Test
    public void c1DoScanTestOnPrimitiveType() throws IOException {
        Map<String, String> params = createScanTestParams();
        runScanOnParams(params);
    }
}
