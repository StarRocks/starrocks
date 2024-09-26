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

package com.starrocks.connector.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.checkpoints.CheckpointMetaData;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.starrocks.connector.delta.DeltaLakeTestBase.ADD_FILE_MOD_TIME_ORDINAL;
import static com.starrocks.connector.delta.DeltaLakeTestBase.ADD_FILE_SIZE_ORDINAL;
import static com.starrocks.connector.delta.DeltaLakeTestBase.getAddFileEntry;
import static com.starrocks.connector.delta.DeltaLakeTestBase.getAddFilePath;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_PATH_ORDINAL;

public class DeltaLakeJsonHandlerTest {
    private final Configuration hdfsConfiguration = new Configuration();
    private final String deltaLakePath = Objects.requireNonNull(ClassLoader.getSystemClassLoader()
            .getResource("connector/deltalake")).getPath();

    private final LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache = CacheBuilder.newBuilder()
            .expireAfterWrite(3600, TimeUnit.SECONDS)
            .maximumSize(100)
            .build(new CacheLoader<>() {
                @NotNull
                @Override
                public List<JsonNode> load(@NotNull DeltaLakeFileStatus fileStatus) throws IOException {
                    return DeltaLakeJsonHandler.readJsonFile(fileStatus.getPath(), hdfsConfiguration);
                }
            });

    @Test
    public void testReadLastCheckPoint() {
        String path = deltaLakePath + "/_last_checkpoint";
        DeltaLakeJsonHandler deltaLakeJsonHandler = new DeltaLakeJsonHandler(hdfsConfiguration, jsonCache);

        StructType readSchema = CheckpointMetaData.READ_SCHEMA;
        FileStatus fileStatus = FileStatus.of(path, 0, 0);
        try (CloseableIterator<ColumnarBatch> jsonIter = deltaLakeJsonHandler.readJsonFiles(
                Utils.singletonCloseableIterator(fileStatus), readSchema, Optional.empty())) {
            Optional<Row> checkpointRow = InternalUtils.getSingularRow(jsonIter);
            Assert.assertTrue(checkpointRow.isPresent());
            Row row = checkpointRow.get();
            Assert.assertEquals(row.getLong(0), 30);
        } catch (IOException e) {
            Assert.fail();
        }

        Assert.assertTrue(jsonCache.asMap().isEmpty());
    }

    @Test
    public void testReadJsonMetadata() {
        String path = deltaLakePath + "/00000000000000000031.json";
        DeltaLakeJsonHandler deltaLakeJsonHandler = new DeltaLakeJsonHandler(hdfsConfiguration, jsonCache);

        StructType readSchema = LogReplay.getAddRemoveReadSchema(true);
        FileStatus fileStatus = FileStatus.of(path, 123, 123);
        DeltaLakeFileStatus deltaLakeFileStatus = DeltaLakeFileStatus.of(fileStatus);

        List<Row> addRows = Lists.newArrayList();
        try (CloseableIterator<ColumnarBatch> jsonIter = deltaLakeJsonHandler.readJsonFiles(
                Utils.singletonCloseableIterator(fileStatus), readSchema, Optional.empty())) {
            while (jsonIter.hasNext()) {
                ColumnarBatch columnarBatch = jsonIter.next();
                ColumnVector addsVector = columnarBatch.getColumnVector(ADD_FILE_ORDINAL);
                for (int rowId = 0; rowId < addsVector.getSize(); rowId++) {
                    if (addsVector.isNullAt(rowId)) {
                        continue;
                    }
                    getAddFilePath(addsVector, rowId);
                }

                try (CloseableIterator<Row> rows = columnarBatch.getRows()) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        addRows.add(row);
                    }
                }
            }
        } catch (IOException e) {
            Assert.fail();
        }

        Assert.assertEquals(2, addRows.size());
        Row scanRow = addRows.get(1);
        Row addFile = getAddFileEntry(scanRow);
        Assert.assertEquals("col_date=2024-01-06/part-00000-3c9a556a-d185-4963-869d-b059d4c9b482.c000.snappy.parquet",
                addFile.getString(ADD_FILE_PATH_ORDINAL));
        Assert.assertEquals(724, addFile.getLong(ADD_FILE_SIZE_ORDINAL));
        Assert.assertEquals(1721830614469L, addFile.getLong(ADD_FILE_MOD_TIME_ORDINAL));

        Map<String, String> partitionValues = InternalScanFileUtils.getPartitionValues(scanRow);
        Assert.assertTrue(partitionValues.containsKey("col_date"));
        Assert.assertEquals("2024-01-06", partitionValues.get("col_date"));

        Assert.assertFalse(jsonCache.asMap().isEmpty());
        Assert.assertTrue(jsonCache.asMap().containsKey(deltaLakeFileStatus));
    }
}
