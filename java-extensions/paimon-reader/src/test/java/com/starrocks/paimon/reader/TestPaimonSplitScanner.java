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
import com.starrocks.utils.Platform;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPaimonSplitScanner {

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        System.setProperty(Platform.UT_KEY, Boolean.TRUE.toString());
    }

    @AfterEach
    public void tearDown() {
        System.setProperty(Platform.UT_KEY, Boolean.FALSE.toString());
    }

    @Test
    public void testScanSerializedPaimonTableAndSplit() throws Exception {
        Options options = new Options();
        options.set("warehouse", tempDir.toUri().toString());

        Identifier tableId = Identifier.create("paimon_test", "scanner_test");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase(tableId.getDatabaseName(), true);
            Schema schema = Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .build();
            catalog.createTable(tableId, schema, false);

            Table table = catalog.getTable(tableId);
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = writeBuilder.newWrite()) {
                write.write(GenericRow.of(1, BinaryString.fromString("paimon-2.0-reader")));
                write.write(GenericRow.of(2, BinaryString.fromString("off-heap-round-trip")));
                try (BatchTableCommit commit = writeBuilder.newCommit()) {
                    commit.commit(write.prepareCommit());
                }
            }

            List<Split> splits = table.newReadBuilder().newScan().plan().splits();
            Assertions.assertEquals(1, splits.size());

            Map<String, String> params = new HashMap<>();
            params.put("native_table", encode(table));
            params.put("split_info", encode(splits.get(0)));
            params.put("predicate_info", encode(Collections.emptyList()));
            params.put("required_fields", "id,payload");
            params.put("nested_fields", "");
            params.put("time_zone", "UTC");

            PaimonSplitScanner scanner = new PaimonSplitScanner(1, params);
            int totalRows = 0;
            StringBuilder rows = new StringBuilder();
            try {
                scanner.open();
                while (true) {
                    scanner.getNextOffHeapChunk();
                    OffHeapTable chunk = scanner.getOffHeapTable();
                    try {
                        chunk.checkTableMeta(false);
                        if (chunk.getNumRows() == 0) {
                            break;
                        }
                        totalRows += chunk.getNumRows();
                        rows.append(chunk.dump(chunk.getNumRows()));
                    } finally {
                        chunk.close();
                    }
                }
            } finally {
                scanner.close();
            }

            Assertions.assertEquals(2, totalRows);
            Assertions.assertTrue(rows.toString().contains("id:1,payload:paimon-2.0-reader"));
            Assertions.assertTrue(rows.toString().contains("id:2,payload:off-heap-round-trip"));
        }
    }

    private static String encode(Object value) throws IOException {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(InstantiationUtil.serializeObject(value));
    }
}
