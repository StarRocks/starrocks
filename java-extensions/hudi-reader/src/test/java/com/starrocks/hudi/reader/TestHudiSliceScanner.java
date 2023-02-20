package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.OffHeapTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestHudiSliceScanner {

    @Before
    public void setUp() {
        System.setProperty("starrocks.fe.test", "1");
    }

    @After
    public void tearDown() {
        System.setProperty("starrocks.fe.test", "0");
    }

    /*

CREATE TABLE `test_hudi_mor` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` array<int>,
  `d` map<string, int>,
  `e` struct<a:int, b:string>)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

spark-sql> select a,b,c,d,e from test_hudi_mor;
a       b       c       d       e
1       hello   [10,20,30]      {"key1":1,"key2":2}     {"a":10,"b":"world"}
*/

    Map<String, String> createScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiSliceScanner.class.getResource("/test_hudi_mor");
        String basePath = resource.getPath().toString();
        params.put("base_path", basePath);
        params.put("data_file_path",
                basePath + "/64798197-be6a-4eca-9898-0c2ed75b9d65-0_0-54-41_20230105142938081.parquet");
        params.put("delta_file_paths",
                basePath + "/.64798197-be6a-4eca-9898-0c2ed75b9d65-0_20230105142938081.log.1_0-95-78");
        params.put("hive_column_names",
                "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,uuid,ts,a,b,c,d,e");
        params.put("hive_column_types",
                "string#string#string#string#string#string#int#int#string#array<int>#map<string,int>#struct<a:int,b:string>");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "436081");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "a,b");
        return params;
    }

    String runScanOnParams(Map<String, String> params) throws IOException {
        HudiSliceScanner scanner = new HudiSliceScanner(4096, params);
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

    @Test
    public void c1DoScanTestOnArrayType() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "c");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnArrayType2() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "c");
        params.put("nested_fields", "c.$0");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnMapTypeKeyOnly() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "d");
        params.put("nested_fields", "d.$0");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnMapTypeValueOnly() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "d");
        params.put("nested_fields", "d.$1");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnMapTypeAll() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "d");
        params.put("nested_fields", "d.$0,d.$1");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnMapTypeAll2() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "d");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnStructTypeAll() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "e");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnStructTypeAll2() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.a,e.b");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnPruningStructType() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.b");
        runScanOnParams(params);
    }

    @Test
    public void c1DoScanTestOnPruningStructTypeNoField() throws IOException {
        Map<String, String> params = createScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.B,e.a");
        runScanOnParams(params);
    }


        /*


CREATE TABLE `test_hudi_mor2` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` array<array<int>>,
  `d` map<string, array<int>>,
  `e` struct<a:array<int>, b:map<string,int>, c:struct<a:array<int>, b:struct<a:int,b:string>>>)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

insert into test_hudi_mor2 values('AA0', 10, 0, "hello", array(array(10,20,30), array(40,50,60,70) ), map('key1', array(1,10), 'key2', array(2, 20), 'key3', null), struct(array(10, 20), map('key1', 10), struct(array(10, 20), struct(10, "world")))),
 ('AA1', 10, 0, "hello", null, null , struct(null, map('key1', 10), struct(array(10, 20), struct(10, "world")))),
 ('AA2', 10, 0, null, array(array(30, 40), array(10,20,30)), null , struct(null, map('key1', 10), struct(array(10, 20), null)));

spark-sql> select a,b,c,d,e from test_hudi_mor2;
a       b       c       d       e
0       hello   NULL    NULL    {"a":null,"b":{"key1":10},"c":{"a":[10,20],"b":{"a":10,"b":"world"}}}
0       NULL    [[30,40],[10,20,30]]    NULL    {"a":null,"b":{"key1":10},"c":{"a":[10,20],"b":null}}
0       hello   [[10,20,30],[40,50,60,70]]      {"key1":[1,10],"key2":[2,20],"key3":null}       {"a":[10,20],"b":{"key1":10},"c":{"a":[10,20],"b":{"a":10,"b":"world"}}}
    */

    Map<String, String> case2CreateScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiSliceScanner.class.getResource("/test_hudi_mor2");
        String basePath = resource.getPath().toString();
        params.put("base_path", basePath);
        params.put("data_file_path",
                basePath + "/0df0196b-f46f-43f5-8cf0-06fad7143af3-0_0-27-35_20230110191854854.parquet");
        params.put("delta_file_paths", "");
        params.put("hive_column_names",
                "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,uuid,ts,a,b,c,d,e");
        params.put("hive_column_types",
                "string#string#string#string#string#string#int#int#string#array<array<int>>#map<string,array<int>>#struct<a:array<int>,b:map<string,int>,c:struct<a:array<int>,b:struct<a:int,b:string>>>");
        params.put("instant_time", "20230110185815638");
        params.put("data_file_length", "438311");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "a,b");
        return params;
    }

    @Test
    public void c2DoScanTestOnMapArrayType() throws IOException {
        Map<String, String> params = case2CreateScanTestParams();
        params.put("required_fields", "a,b,c,d");
        runScanOnParams(params);
    }

    @Test
    public void c2DoScanTestOnStructType() throws IOException {
        Map<String, String> params = case2CreateScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.b,e.a");
        runScanOnParams(params);
    }

    @Test
    public void c2DoScanTestOnStructTypeOrder() throws IOException {
        Map<String, String> params = case2CreateScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.a,e.b");
        runScanOnParams(params);
    }

    @Test
    public void c2DoScanTestOnStructType2() throws IOException {
        Map<String, String> params = case2CreateScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.c.a,e.b.$1,e.a.$0");
        runScanOnParams(params);
    }

    @Test
    public void c2DoScanTestOnStructType3() throws IOException {
        Map<String, String> params = case2CreateScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.c.b.b,e.c.b.a");
        runScanOnParams(params);
    }

    @Test
    public void c2DoScanTestOnStructType4() throws IOException {
        Map<String, String> params = case2CreateScanTestParams();
        params.put("required_fields", "e");
        params.put("nested_fields", "e.c.b.b,e.c.a.$0");
        runScanOnParams(params);
    }

    /*

```
CREATE TABLE `test_hudi_mor5` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` timestamp)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

```
```
insert into test_hudi_mor5 values('AA1', 20, 1, "1", cast(date_format("2021-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss") as timestamp));
```
     */

    Map<String, String> case5CreateScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiSliceScanner.class.getResource("/test_hudi_mor5");
        String basePath = resource.getPath().toString();
        params.put("base_path", basePath);
        params.put("data_file_path",
                basePath + "/07eeba07-b04d-42b5-9e31-35b1a22ea31d-0_0-89-83_20230208203804485.parquet");
        params.put("delta_file_paths", "");
        params.put("hive_column_names",
                "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,uuid,ts,a,b,c");
        params.put("hive_column_types",
                "string#string#string#string#string#string#int#int#string#TimestampMicros");
        params.put("instant_time", "20230208203804485");
        params.put("data_file_length", "435028");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "a,b,c");
        return params;
    }

    @Test
    public void c5DoScanTestOnTimestamp() throws IOException {
        Map<String, String> params = case5CreateScanTestParams();
        runScanOnParams(params);
    }
}
