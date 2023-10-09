package com.starrocks.hive.reader;

import com.starrocks.jni.connector.OffHeapTable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        String filePath = basePath + "/basic.avro";
        File file = new File(filePath);
        params.put("data_file_path",filePath);
        params.put("data_file_length", String.valueOf(file.length()));
        params.put("hive_column_names",
                "booleantype,longtype,doubletype,stringtype");
        params.put("hive_column_types",
                "boolean#bigint#double#string");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
        params.put("serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        params.put("required_fields", "booleantype,longtype,doubletype,stringtype");
        return params;
    }

    public static void main(String[] args) throws IOException {
        // 1. 定义Avro Schema
        URL resource= TestHiveScanner.class.getResource("/test_primitive_type");
        String basePath = resource.getPath().toString();

        Schema schema = new Schema.Parser().parse(new File(basePath + "/avro_basic_schema.json"));


        // 2. 创建Avro数据对象
        GenericRecord person = new GenericData.Record(schema);
        person.put("booleantype", true);
        person.put("longtype", 4294967296L);
        person.put("doubletype", 1.234567);
        person.put("stringtype", "abcdefg");
//        GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(schema, "DIAMONDS");
//        person.put("enumtype", symbol);

        // 3. 将Avro数据对象序列化为二进制格式
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, new File(basePath + "/basic.avro"));
        dataFileWriter.append(person);
        dataFileWriter.close();
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
