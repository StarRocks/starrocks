package com.starrocks.hudi.reader;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestHudiSliceScanner {
    public static HudiSliceScanner createHudiSliceScanner(int fetchSize, Map<String, String> params) {
        return new HudiSliceScanner(fetchSize, params);
    }

    @Test
    public void doScanTest() throws IOException {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiSliceScanner.class.getResource("/test_hudi_mor");
        String basePath = resource.getPath().toString();
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/64798197-be6a-4eca-9898-0c2ed75b9d65-0_0-54-41_20230105142938081.parquet");
        params.put("delta_file_paths", basePath + "/.64798197-be6a-4eca-9898-0c2ed75b9d65-0_20230105142938081.log.1_0-95-78");
        params.put("hive_column_names", "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,uuid,ts,a,b,c,d,e");
        params.put("hive_column_types", "string#string#string#string#string#string#int#int#string#array<int>#map<string,int>#struct<a:int,b:string>");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "436081");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "a,b");
        HudiSliceScanner scanner = createHudiSliceScanner(4096, params);

        System.out.println(scanner.toString());
        scanner.open();
        while (true) {
            try {
                long chunkMeta = scanner.getNextOffHeapChunk();
                scanner.getOffHeapTable().show(10);
            } catch (IOException e) {
                break;
            }
        }
        scanner.close();
    }
}
