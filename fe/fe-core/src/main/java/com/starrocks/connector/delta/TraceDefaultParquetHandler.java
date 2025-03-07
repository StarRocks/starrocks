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

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultParquetHandler;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Optional;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class TraceDefaultParquetHandler extends DefaultParquetHandler {
    private final Configuration hadoopConf;
    public TraceDefaultParquetHandler(Configuration hadoopConf) {
        super(hadoopConf);
        this.hadoopConf = hadoopConf;
    }

    // This method copies the implementation from DefaultParquetHandler.java
    @Override
    public CloseableIterator<ColumnarBatch> readParquetFiles(
            CloseableIterator<FileStatus> fileIter,
            StructType physicalSchema,
            Optional<Predicate> predicate) throws IOException {
        return new CloseableIterator<ColumnarBatch>() {
            private final io.delta.kernel.defaults.internal.parquet.ParquetFileReader batchReader =
                    new io.delta.kernel.defaults.internal.parquet.ParquetFileReader(hadoopConf);
            private CloseableIterator<ColumnarBatch> currentFileReader;

            @Override
            public void close() throws IOException {
                Utils.closeCloseables(currentFileReader, fileIter);
            }

            @Override
            public boolean hasNext() {
                if (currentFileReader != null && currentFileReader.hasNext()) {
                    return true;
                } else {
                    // There is no file in reading or the current file being read has no more data.
                    // Initialize the next file reader or return false if there are no more files to
                    // read.
                    Utils.closeCloseables(currentFileReader);
                    currentFileReader = null;
                    if (fileIter.hasNext()) {
                        try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL,
                                "TraceDefaultParquetHandler.readParquetFile")) {
                            String nextFile = fileIter.next().getPath();
                            currentFileReader = batchReader.read(nextFile, physicalSchema, predicate);
                            return hasNext(); // recurse since it's possible the loaded file is empty
                        }
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public ColumnarBatch next() {
                try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "TraceDefaultParquetHandler.GetColumnarBatch")) {
                    return currentFileReader.next();
                }
            }
        };
    }
}
