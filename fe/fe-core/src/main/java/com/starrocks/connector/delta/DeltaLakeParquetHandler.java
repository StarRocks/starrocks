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

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultParquetHandler;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static java.lang.String.format;

public class DeltaLakeParquetHandler extends DefaultParquetHandler {
    private final Configuration hadoopConf;
    private final LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache;

    public DeltaLakeParquetHandler(Configuration hadoopConf, LoadingCache<Pair<DeltaLakeFileStatus, StructType>,
            List<ColumnarBatch>> checkpointCache) {
        super(hadoopConf);
        this.hadoopConf = hadoopConf;
        this.checkpointCache = checkpointCache;
    }

    public static List<ColumnarBatch> readParquetFile(String filePath, StructType physicalSchema, Configuration hadoopConf) {
        try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL,
                "DeltaLakeParquetHandler.readParquetFileAndGetColumnarBatch")) {
            io.delta.kernel.defaults.internal.parquet.ParquetFileReader batchReader =
                    new io.delta.kernel.defaults.internal.parquet.ParquetFileReader(hadoopConf);
            CloseableIterator<ColumnarBatch> currentFileReader = batchReader.read(filePath, physicalSchema, Optional.empty());

            List<ColumnarBatch> result = Lists.newArrayList();
            while (currentFileReader != null && currentFileReader.hasNext()) {
                result.add(currentFileReader.next());
            }
            Utils.closeCloseables(currentFileReader);
            return result;
        }
    }

    @Override
    public CloseableIterator<ColumnarBatch> readParquetFiles(
            CloseableIterator<FileStatus> fileIter,
            StructType physicalSchema,
            Optional<Predicate> predicate) {
        return new CloseableIterator<>() {
            private int currentReadColumnarBatchIndex = -1;
            private List<ColumnarBatch> currentColumnarBatchList = Lists.newArrayList();
            private String currentFile;

            @Override
            public void close() {
                Utils.closeCloseables(fileIter);
                currentReadColumnarBatchIndex = -1;
                currentColumnarBatchList = null;
            }

            @Override
            public boolean hasNext() {
                if (hasNextToConsume()) {
                    return true;
                } else {
                    currentReadColumnarBatchIndex = -1;
                    currentColumnarBatchList = Lists.newArrayList();
                    // There is no file in reading or the current file being read has no more data.
                    // Initialize the next file reader or return false if there are no more files to
                    // read.
                    try {
                        tryGetNextFileColumnarBatch();
                    } catch (Exception ex) {
                        throw new KernelEngineException(
                                format("Error reading Parquet file: %s", currentFile), ex);
                    }

                    if (hasNextToConsume()) {
                        return true;
                    } else if (fileIter.hasNext()) {
                        // recurse since it's possible the loaded file is empty
                        return hasNext();
                    } else {
                        return false;
                    }
                }
            }

            private boolean hasNextToConsume() {
                return currentReadColumnarBatchIndex != -1 && !currentColumnarBatchList.isEmpty() &&
                        currentReadColumnarBatchIndex < currentColumnarBatchList.size();
            }

            @Override
            public ColumnarBatch next() {
                return currentColumnarBatchList.get(currentReadColumnarBatchIndex++);
            }

            private void tryGetNextFileColumnarBatch() throws ExecutionException {
                if (fileIter.hasNext()) {
                    DeltaLakeFileStatus deltaLakeFileStatus = DeltaLakeFileStatus.of(fileIter.next());
                    currentFile = deltaLakeFileStatus.getPath();
                    if (LogReplay.containsAddOrRemoveFileActions(physicalSchema)) {
                        Pair<DeltaLakeFileStatus, StructType> key = Pair.create(deltaLakeFileStatus, physicalSchema);
                        if (checkpointCache.getIfPresent(key) != null || predicate.isEmpty()) {
                            currentColumnarBatchList = checkpointCache.get(key);
                        } else {
                            currentColumnarBatchList = readParquetFile(currentFile, physicalSchema, hadoopConf);
                        }
                    } else {
                        currentColumnarBatchList = readParquetFile(currentFile, physicalSchema, hadoopConf);
                    }
                    currentReadColumnarBatchIndex = 0;
                }
            }
        };
    }
}
