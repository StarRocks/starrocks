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

package com.starrocks.paimon.reader;

import com.starrocks.jni.connector.OffHeapTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

public class TestPaimonSplitScanner {

    @Disabled("Unusable ut, because of lack native_table parameter")
    @Test
    public void runScan() throws IOException {
        String catalogType = "filesystem";
        String metastoreUri = "";
        URL resource = TestPaimonSplitScanner.class.getResource("/test_paimon_reader");
        String warehousePath = resource.getPath().toString();
        String databaseName = "paimon_test";
        String tableName = "spark_sql_created_hive_catalog_paimon_partitioned";
        String splitInfo = "rO0ABXNyAChvcmcuYXBhY2hlLnBhaW1vbi50YWJsZS5zb3VyY2UuRGF0YVNwbGl0AAAAAAAAAAIDAAZJAAZi" +
                "dWNrZXRaAA1pc0luY3JlbWVudGFsWgAOcmV2ZXJzZVJvd0tpbmRKAApzbmFwc2hvdElkTAAFZmlsZXN0ABBMamF2YS91" +
                "dGlsL0xpc3Q7TAAJcGFydGl0aW9udAAiTG9yZy9hcGFjaGUvcGFpbW9uL2RhdGEvQmluYXJ5Um93O3hwegAAAfYA" +
                "AAAAAAAAAQAAABQAAAABAAAAAAAAAABiYW5hbmEAhgAAAAAAAAABAAAByAAAAAAAAAAALwAAAHAAAACtAgAAAAAA" +
                "AAEAAAAAAAAAFAAAAKAAAAAUAAAAuAAAAGAAAADQAAAAkAAAADABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                "AAAAAAAAAgAAADAAQAAMz37eogBAABkYXRhLTA3ZWE4ZDQ4LTYwMTItNGE3Ni1hNTY0LWM0MjI5OTUxODlmMi0wLm9yY" +
                "wAAAAABAAAAAAAAAAACAAAAAAAAAAAAAAAAAAABAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAABQAAAAgAAAAFAAAA" +
                "DgAAAAQAAAAUAAAAAAAAAEAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAIAAAAAAAAAAAAAAAEAAAAAAAAAA" +
                "AAAAAAAAAAAAAAAAAAAACQAAAAgAAAAJAAAAEgAAAAgAAAAcAAAAAAAAAMAAAAAAAAAAAIAAAAAAAAAYmFuYW5hAIYAA" +
                "AAAAAAQQAAAAAAAAAADAAAAAAAAAAACAAAAAAAAAGJhbmFuYQCGAAAAAAAAEEAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAA" +
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAeA";
        String requiredFields = "uuid,name,price";
        String predicateInfo = "rO0ABXNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAAABdwQAAA" +
                "ABc3IALW9yZy5hcGFjaGUucGFpbW9uLnByZWRpY2F0ZS5Db21wb3VuZFByZWRpY2F0ZdA0yrPnfJptAgACTAAIY2hp" +
                "bGRyZW50ABBMamF2YS91dGlsL0xpc3Q7TAAIZnVuY3Rpb250ADhMb3JnL2FwYWNoZS9wYWltb24vcHJlZGljYXRlL0" +
                "NvbXBvdW5kUHJlZGljYXRlJEZ1bmN0aW9uO3hwc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtIC" +
                "AAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAKFtMb3JnLmFwYWNoZS5wYWltb24ucHJlZGljYXRlLlByZW" +
                "RpY2F0ZTtOH6coIOoYtAIAAHhwAAAAAnNyAClvcmcuYXBhY2hlLnBhaW1vbi5wcmVkaWNhdGUuTGVhZlByZWRpY2F0" +
                "ZQAAAAAAAAABAwAESQAKZmllbGRJbmRleEwACWZpZWxkTmFtZXQAEkxqYXZhL2xhbmcvU3RyaW5nO0wACGZ1bmN0aW" +
                "9udAAqTG9yZy9hcGFjaGUvcGFpbW9uL3ByZWRpY2F0ZS9MZWFmRnVuY3Rpb247TAAEdHlwZXQAIkxvcmcvYXBhY2hl" +
                "L3BhaW1vbi90eXBlcy9EYXRhVHlwZTt4cAAAAAB0AAR1dWlkc3IAIW9yZy5hcGFjaGUucGFpbW9uLnByZWRpY2F0ZS" +
                "5FcXVhbD4tGe7tAZAFAgAAeHIAN29yZy5hcGFjaGUucGFpbW9uLnByZWRpY2F0ZS5OdWxsRmFsc2VMZWFmQmluYXJ5" +
                "RnVuY3Rpb24AAAAAAAAAAQIAAHhyAChvcmcuYXBhY2hlLnBhaW1vbi5wcmVkaWNhdGUuTGVhZkZ1bmN0aW9ur57J5Q" +
                "A5OS4CAAB4cHNyAB9vcmcuYXBhY2hlLnBhaW1vbi50eXBlcy5JbnRUeXBlAAAAAAAAAAECAAB4cgAgb3JnLmFwYWNo" +
                "ZS5wYWltb24udHlwZXMuRGF0YVR5cGUAAAAAAAAAAQIAAloACmlzTnVsbGFibGVMAAh0eXBlUm9vdHQAJkxvcmcvYX" +
                "BhY2hlL3BhaW1vbi90eXBlcy9EYXRhVHlwZVJvb3Q7eHAAfnIAJG9yZy5hcGFjaGUucGFpbW9uLnR5cGVzLkRhdGFU" +
                "eXBlUm9vdAAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQAB0lOVEVHRVJ3CQAAAAEAAA" +
                "AAAXhzcQB-AAsAAAAAcQB-ABBxAH4AFHEAfgAYdwkAAAABAAAAAAJ4c3IAHm9yZy5hcGFjaGUucGFpbW9uLnByZWRp" +
                "Y2F0ZS5PcgAAAAAAAAABAgAAeHIANm9yZy5hcGFjaGUucGFpbW9uLnByZWRpY2F0ZS5Db21wb3VuZFByZWRpY2F0ZS" +
                "RGdW5jdGlvbsmt9tbpUzseAgAAeHB4";

        int fetchSize = 4096;

        HashMap<String, String> params = new HashMap<>(10);
        params.put("catalog_type", catalogType);
        params.put("metastore_uri", metastoreUri);
        params.put("warehouse_path", warehousePath);
        params.put("database_name", databaseName);
        params.put("table_name", tableName);
        params.put("required_fields", requiredFields);
        params.put("split_info", splitInfo);
        params.put("predicate_info", predicateInfo);

        PaimonSplitScanner scanner = new PaimonSplitScanner(fetchSize, params);
        scanner.open();
        while (true) {
            scanner.getNextOffHeapChunk();
            OffHeapTable table = scanner.getOffHeapTable();
            if (table.getNumRows() == 0) {
                break;
            }
            table.show(10);
            table.checkTableMeta(true);
            table.close();
        }
        scanner.close();
    }
}
