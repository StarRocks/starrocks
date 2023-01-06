package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.OffHeapTable;
import com.starrocks.utils.Platform;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestHudiSliceScanner {

    public class LocalHudiScanner extends HudiSliceScanner {
        public Map<Integer, ArrayList<Object>> buffer;

        LocalHudiScanner(int chunkSize, Map<String, String> params) {
            super(chunkSize, params);
            buffer = new HashMap<>();
        }

        @Override
        public void scanData(int index, Object data) {
            if (!buffer.containsKey(index)) {
                buffer.put(index, new ArrayList<Object>());
            }
            buffer.get(index).add(data);
        }

        public String dumpBuffer() {
            StringBuffer sb = new StringBuffer();
            sb.append("{\n");
            buffer.forEach((k, values) -> {
                String columnName = getRequiredField(k);
                sb.append("column = " + columnName + ", datta = [");
                int i = 0;
                for (Object obj : values) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(obj.toString());
                    i += 1;
                }
                sb.append("]\n");
            });
            sb.append("}");
            return sb.toString();
        }
    }

    @Before
    public void setUp() {
        Platform.enableUnsafeMemoryTracker();
    }

    @After
    public void tearDown() {
        Platform.disableUnsafeMemoryTracker();
    }

    /*
    // to create test hudi mor;
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

  // ingest data with several updates, final result is one row
  _hoodie_commit_time     _hoodie_commit_seqno    _hoodie_record_key      _hoodie_partition_path  _hoodie_file_name       uuid    ts      a       b       c       d       e
20230105143305070       20230105143305070_0_10  AA0             64798197-be6a-4eca-9898-0c2ed75b9d65-0  AA0     20      1       hello   [10,20,30]      {"key1":1,"key2":2}     {"a":10,"b":"world"}
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

    @Test
    public void doScanTest0() throws IOException {
        Map<String, String> params = createScanTestParams();
        LocalHudiScanner scanner = new LocalHudiScanner(4096, params);

        System.out.println(scanner.toString());
        scanner.open();
        while (true) {
            int numRows = scanner.getNext();
            if (numRows == 0) {
                break;
            }
        }
        System.out.println(scanner.dumpBuffer());
        scanner.close();
    }

    @Test
    public void doScanTest1() throws IOException {
        Map<String, String> params = createScanTestParams();
        HudiSliceScanner scanner = new HudiSliceScanner(4096, params);

        System.out.println(scanner.toString());
        scanner.open();
        while (true) {
            scanner.getNextOffHeapChunk();
            OffHeapTable table = scanner.getOffHeapTable();
            if (table.getNumRows() == 0) {
                break;
            }
            table.show(10);
        }
        scanner.close();
    }
}
