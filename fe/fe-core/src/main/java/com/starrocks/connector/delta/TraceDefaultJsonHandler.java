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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultJsonHandler;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static java.lang.String.format;

public class TraceDefaultJsonHandler extends DefaultJsonHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // by default BigDecimals are truncated and read as floats
    private static final ObjectReader OBJECT_READER_READ_BIG_DECIMALS = MAPPER
            .reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final Configuration hadoopConf;
    private final int maxBatchSize;
    public TraceDefaultJsonHandler(Configuration hadoopConf) {
        super(hadoopConf);
        this.hadoopConf = hadoopConf;
        this.maxBatchSize =
                hadoopConf.getInt("delta.kernel.default.json.reader.batch-size", 1024);
    }

    // This method copies the implementation from DefaultJsonHandler.java
    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(
            CloseableIterator<FileStatus> scanFileIter,
            StructType physicalSchema,
            Optional<Predicate> predicate) throws IOException {
        return new CloseableIterator<ColumnarBatch>() {
            private FileStatus currentFile;
            private BufferedReader currentFileReader;
            private String nextLine;

            @Override
            public void close() throws IOException {
                Utils.closeCloseables(currentFileReader, scanFileIter);
            }

            @Override
            public boolean hasNext() {
                if (nextLine != null) {
                    return true; // we have un-consumed last read line
                }

                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                try {
                    if (currentFileReader == null ||
                            (nextLine = currentFileReader.readLine()) == null) {

                        tryOpenNextFile();
                        if (currentFileReader != null) {
                            nextLine = currentFileReader.readLine();
                        }
                    }
                } catch (IOException ex) {
                    throw new KernelEngineException(
                            format("Error reading JSON file: %s", currentFile.getPath()), ex);
                }

                return nextLine != null;
            }

            @Override
            public ColumnarBatch next() {
                if (nextLine == null) {
                    throw new NoSuchElementException();
                }

                List<Row> rows = new ArrayList<>();
                int currentBatchSize = 0;
                try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "TraceDefaultJsonHandler.JsonToColumnarBatch")) {
                    do {
                        // hasNext already reads the next one and keeps it in member variable `nextLine`
                        rows.add(parseJson(nextLine, physicalSchema));
                        nextLine = null;
                        currentBatchSize++;
                    } while (currentBatchSize < maxBatchSize && hasNext());

                    return new io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch(
                            physicalSchema, rows);
                }
            }

            private void tryOpenNextFile() throws IOException {
                Utils.closeCloseables(currentFileReader); // close the current opened file
                currentFileReader = null;

                if (scanFileIter.hasNext()) {
                    try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "TraceDefaultJsonHandler.ReadJsonFile")) {
                        currentFile = scanFileIter.next();
                        Path filePath = new Path(currentFile.getPath());
                        FileSystem fs = filePath.getFileSystem(hadoopConf);
                        FSDataInputStream stream = null;
                        try {
                            stream = fs.open(filePath);
                            currentFileReader = new BufferedReader(
                                    new InputStreamReader(stream, StandardCharsets.UTF_8));
                        } catch (Exception e) {
                            Utils.closeCloseablesSilently(stream); // close it avoid leaking resources
                            throw e;
                        }
                    }
                }
            }
        };
    }

    private Row parseJson(String json, StructType readSchema) {
        try {
            final JsonNode jsonNode = OBJECT_READER_READ_BIG_DECIMALS.readTree(json);
            return new io.delta.kernel.defaults.internal.data.DefaultJsonRow(
                    (ObjectNode) jsonNode, readSchema);
        } catch (JsonProcessingException ex) {
            throw new KernelEngineException(format("Could not parse JSON: %s", json), ex);
        }
    }
}
