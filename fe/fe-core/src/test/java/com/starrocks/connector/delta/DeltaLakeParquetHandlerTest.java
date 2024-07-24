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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.starrocks.connector.delta.DeltaLakeTestBase.getAddFileEntry;
import static com.starrocks.connector.delta.DeltaLakeTestBase.getAddFilePath;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_PATH_ORDINAL;

public class DeltaLakeParquetHandlerTest {
    Configuration hdfsConfiguration = new Configuration();
    String deltaLakePath = Objects.requireNonNull(ClassLoader.getSystemClassLoader()
            .getResource("connector/deltalake")).getPath();

    private final LoadingCache<Pair<String, StructType>, List<ColumnarBatch>>  checkpointCache = CacheBuilder.newBuilder()
            .expireAfterWrite(3600, TimeUnit.SECONDS)
            .maximumSize(100)
            .build(new CacheLoader<>() {
                @NotNull
                @Override
                public List<ColumnarBatch> load(@NotNull Pair<String, StructType> pair) {
                    return DeltaLakeParquetHandler.readParquetFile(pair.first, pair.second, hdfsConfiguration);
                }
            });

    @Test
    public void testParquetMetadata() {
        String path = deltaLakePath + "/00000000000000000030.checkpoint.parquet";
        DeltaLakeParquetHandler deltaLakeParquetHandler = new DeltaLakeParquetHandler(hdfsConfiguration, checkpointCache);
        StructType readSchema = LogReplay.getAddRemoveReadSchema(true);
        FileStatus fileStatus = FileStatus.of(path, 0, 0);

        List<Row> addRows = Lists.newArrayList();
        try (CloseableIterator<ColumnarBatch> parquetIter = deltaLakeParquetHandler.readParquetFiles(
                Utils.singletonCloseableIterator(fileStatus), readSchema, Optional.empty())) {
            while (parquetIter.hasNext()) {
                ColumnarBatch columnarBatch = parquetIter.next();
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
            throw new RuntimeException(e);
        }

        Assert.assertEquals(32, addRows.size());
        List<String> pathList = Lists.newArrayList();
        Set<String> partitionValues = Sets.newHashSet();
        for (Row scanRow : addRows) {
            if (scanRow.isNullAt(InternalScanFileUtils.ADD_FILE_ORDINAL)) {
                continue;
            }
            Row addFile = getAddFileEntry(scanRow);
            pathList.add(addFile.getString(ADD_FILE_PATH_ORDINAL));
            partitionValues.addAll(InternalScanFileUtils.getPartitionValues(scanRow).values());
        }

        Assert.assertEquals(30, pathList.size());
        Assert.assertEquals(18, partitionValues.size());
        Assert.assertFalse(checkpointCache.asMap().isEmpty());
        Assert.assertTrue(checkpointCache.asMap().containsKey(Pair.create(path, readSchema)));
    }
}
