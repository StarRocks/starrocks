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

package com.starrocks.lance.reader;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceSplitScanner extends ConnectorScanner {
    private static final Logger LOG = LogManager.getLogger(LanceSplitScanner.class);
    private static final String STORAGE_OPTION_PREFIX = "storage_option.";

    private final int fetchSize;
    private final String[] requiredFields;
    private final String datasetUri;
    private final int fragmentId;
    private final Map<String, String> storageOptions;
    private final ClassLoader classLoader;

    private BufferAllocator allocator;
    private Dataset dataset;
    private LanceScanner scanner;
    private ArrowReader arrowReader;
    private VectorSchemaRoot vectorSchemaRoot;
    private ColumnType[] requiredTypes;

    public LanceSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(params.get("required_fields"), ",");
        this.datasetUri = params.get("dataset_uri");
        this.fragmentId = Integer.parseInt(params.get("fragment_id"));
        this.storageOptions = extractStorageOptions(params);
        this.classLoader = this.getClass().getClassLoader();
    }

    private static Map<String, String> extractStorageOptions(Map<String, String> params) {
        Map<String, String> options = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(STORAGE_OPTION_PREFIX)) {
                options.put(entry.getKey().substring(STORAGE_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        return options;
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            initReader();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            close();
            String msg = "Failed to open the lance reader for dataset " + datasetUri;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    private void initReader() throws IOException {
        allocator = new RootAllocator(Long.MAX_VALUE);
        ReadOptions.Builder readOptionsBuilder = new ReadOptions.Builder();
        if (!storageOptions.isEmpty()) {
            readOptionsBuilder.setStorageOptions(new HashMap<>(storageOptions));
        }
        dataset = Dataset.open(allocator, datasetUri, readOptionsBuilder.build());

        Schema schema = dataset.getSchema();
        requiredTypes = new ColumnType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            Field field = schema.findField(requiredFields[i]);
            if (field == null) {
                throw new IOException("Cannot find field " + requiredFields[i] + " in lance schema " + schema);
            }
            requiredTypes[i] = new ColumnType(ArrowTypeUtils.fromArrowField(field));
        }

        List<String> scanFields = requiredFields.length == 0
                ? List.of(schema.getFields().get(0).getName())
                : Arrays.asList(requiredFields);
        ScanOptions scanOptions = new ScanOptions.Builder()
                .fragmentIds(List.of(fragmentId))
                .columns(scanFields)
                .batchSize(fetchSize)
                .build();
        scanner = dataset.newScan(scanOptions);
        arrowReader = scanner.scanBatches();
        vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (!arrowReader.loadNextBatch()) {
                return 0;
            }
            int rowCount = vectorSchemaRoot.getRowCount();
            if (rowCount > fetchSize) {
                throw new IOException("Invalid lance batch row count: " + rowCount + ", fetch size: " + fetchSize);
            }

            if (requiredFields.length > 0) {
                List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
                for (int columnIndex = 0; columnIndex < requiredFields.length; columnIndex++) {
                    FieldVector fieldVector = fieldVectors.get(columnIndex);
                    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                        if (fieldVector.isNull(rowIndex)) {
                            appendData(columnIndex, null);
                        } else {
                            ColumnValue fieldValue = new LanceColumnValue(fieldVector.getObject(rowIndex));
                            appendData(columnIndex, fieldValue);
                        }
                    }
                }
            }
            vectorSchemaRoot.clear();
            return rowCount;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk for lance dataset " + datasetUri;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (vectorSchemaRoot != null) {
                vectorSchemaRoot.close();
                vectorSchemaRoot = null;
            }
            if (arrowReader != null) {
                arrowReader.close();
                arrowReader = null;
            }
            if (scanner != null) {
                scanner.close();
                scanner = null;
            }
            if (dataset != null) {
                dataset.close();
                dataset = null;
            }
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
        } catch (Exception e) {
            String msg = "Failed to close the lance reader for dataset " + datasetUri;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }
}
