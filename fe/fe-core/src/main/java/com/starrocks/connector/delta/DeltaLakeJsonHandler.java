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
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
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
import java.util.concurrent.ExecutionException;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static io.delta.kernel.internal.checkpoints.Checkpointer.LAST_CHECKPOINT_FILE_NAME;
import static java.lang.String.format;

public class DeltaLakeJsonHandler extends DefaultJsonHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // by default BigDecimals are truncated and read as floats
    private static final ObjectReader OBJECT_READER_READ_BIG_DECIMALS = MAPPER
            .reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final Configuration hadoopConf;
    private final int maxBatchSize;
    private final LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache;

    public DeltaLakeJsonHandler(Configuration hadoopConf, LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache) {
        super(hadoopConf);
        this.hadoopConf = hadoopConf;
        this.maxBatchSize = hadoopConf.getInt("delta.kernel.default.json.reader.batch-size", 1024);
        this.jsonCache = jsonCache;
    }

    public static List<JsonNode> readJsonFile(String filePath, Configuration hadoopConf) throws IOException {
        try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "DeltaLakeJsonHandler.readParseJsonFile")) {
            Path readFilePath = new Path(filePath);
            FileSystem fs = readFilePath.getFileSystem(hadoopConf);
            FSDataInputStream stream = null;
            BufferedReader fileReader;
            try {
                stream = fs.open(readFilePath);
                fileReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            } catch (Exception e) {
                Utils.closeCloseablesSilently(stream); // close it avoid leaking resources
                throw e;
            }
            List<JsonNode> jsonNodeList = Lists.newArrayList();
            String readline;
            while ((readline = fileReader.readLine()) != null) {
                jsonNodeList.add(parseJsonToJsonNode(readline));
            }

            Utils.closeCloseables(fileReader);
            return jsonNodeList;
        }
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(
            CloseableIterator<FileStatus> scanFileIter,
            StructType physicalSchema,
            Optional<Predicate> predicate) {

        return new CloseableIterator<>() {
            private String currentFile;
            // index of the current line being read from the current read json list, -1 means no line is read yet
            private int currentReadLine = -1;
            private List<JsonNode> currentReadJsonList = Lists.newArrayList();

            @Override
            public void close() {
                Utils.closeCloseables(scanFileIter);
                currentReadLine = -1;
                currentReadJsonList = null;
            }

            @Override
            public boolean hasNext() {
                if (hasNextToConsume()) {
                    return true; // we have un-consumed last read line
                }

                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                try {
                    tryGetNextFileJson();
                } catch (Exception ex) {
                    throw new KernelEngineException(
                            format("Error reading JSON file: %s", currentFile), ex);
                }

                return hasNextToConsume();
            }

            private boolean hasNextToConsume() {
                return currentReadLine != -1 && !currentReadJsonList.isEmpty() && currentReadLine < currentReadJsonList.size();
            }

            @Override
            public ColumnarBatch next() {
                try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "DeltaLakeJsonHandler.JsonToColumnarBatch")) {
                    if (!hasNextToConsume()) {
                        throw new NoSuchElementException();
                    }

                    List<Row> rows = new ArrayList<>();
                    int currentBatchSize = 0;
                    do {
                        // hasNext already reads the next file and keeps it in member variable `cachedJsonList`
                        JsonNode jsonNode = currentReadJsonList.get(currentReadLine);
                        Row row = new io.delta.kernel.defaults.internal.data.DefaultJsonRow(
                                (ObjectNode) jsonNode, physicalSchema);
                        rows.add(row);
                        currentBatchSize++;
                        currentReadLine++;
                    } while (currentBatchSize < maxBatchSize && hasNext());

                    return new io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch(
                            physicalSchema, rows);
                }
            }

            private void tryGetNextFileJson() throws ExecutionException, IOException {
                if (scanFileIter.hasNext()) {
                    DeltaLakeFileStatus fileStatus = DeltaLakeFileStatus.of(scanFileIter.next());
                    currentFile = fileStatus.getPath();
                    Path filePath = new Path(fileStatus.getPath());
                    if (filePath.getName().equals(LAST_CHECKPOINT_FILE_NAME)) {
                        // can not read last_checkpoint file from cache
                        currentReadJsonList = readJsonFile(currentFile, hadoopConf);
                    } else {
                        currentReadJsonList = jsonCache.get(fileStatus);
                    }
                    currentReadLine = 0;
                }
            }
        };
    }

    private static JsonNode parseJsonToJsonNode(String json) {
        try {
            return OBJECT_READER_READ_BIG_DECIMALS.readTree(json);
        } catch (JsonProcessingException ex) {
            throw new KernelEngineException(format("Could not parse JSON: %s", json), ex);
        }
    }
}
