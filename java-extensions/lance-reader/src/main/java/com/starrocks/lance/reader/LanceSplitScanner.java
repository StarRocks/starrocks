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

    // Storage options passed from FE are prefixed so they can be carried through the generic
    // JNI param map without colliding with reader-specific params. The prefix is stripped here
    // and forwarded verbatim to the Lance reader (e.g. aws_access_key_id, aws_endpoint).
    private static final String STORAGE_OPTION_PREFIX = "storage_option.";

    private final String[] requiredFields;
    private ColumnType[] requiredTypes;
    private final int fetchSize;

    private final String datasetUri;
    private final int fragmentId;
    private final Map<String, String> storageOptions;

    private LanceScanner scanner;
    private ArrowReader arrowReader;
    private VectorSchemaRoot vectorSchemaRoot;

    private final ClassLoader classLoader;

    public LanceSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(params.get("required_fields"), ",");

        this.datasetUri = params.get("dataset_uri");
        this.fragmentId = Integer.parseInt(params.get("fragment_id"));
        this.storageOptions = extractStorageOptions(params);
        LOG.info("datasetUri in split scanner: {}, fragmentId: {}", this.datasetUri, this.fragmentId);

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

    private void initReader() throws IOException {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ReadOptions.Builder builder = new ReadOptions.Builder();

        // Credentials/endpoint are supplied by the table PROPERTIES (plumbed from FE), never hardcoded.
        if (!storageOptions.isEmpty()) {
            builder.setStorageOptions(new HashMap<>(storageOptions));
        }

        ReadOptions options = builder.build();

        Dataset dataset = Dataset.open(allocator, this.datasetUri, options);
        Schema schema = dataset.getSchema();
        this.requiredTypes = new ColumnType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            Field field = schema.findField(requiredFields[i]);
            String type = ArrowTypeUtils.fromArrowType(field.getType());
            LOG.info("type in split scanner {}: arrow type {}, sr type {}", field.getType(), requiredFields[i], type);
            requiredTypes[i] = new ColumnType(type);
        }

        ScanOptions.Builder scanOptions = new ScanOptions.Builder();
        scanOptions.columns(Arrays.asList(this.requiredFields));
        scanOptions.batchSize(this.fetchSize);
        this.scanner = dataset.getFragment(this.fragmentId).newScan(scanOptions.build());
        this.arrowReader = scanner.scanBatches();
        this.vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            initReader();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            close();
            String msg = "Failed to open the lance reader for table " + this.datasetUri;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            boolean hasNext = this.arrowReader.loadNextBatch();
            if (hasNext) {
                int rowCount = this.vectorSchemaRoot.getRowCount();
                if (rowCount > this.fetchSize) {
                    throw new Exception("Invalid rowCount: " + rowCount);
                }
                List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
                int needReadRows = Math.min(rowCount, this.fetchSize);
                while (numRows < needReadRows) {
                    for (int i = 0; i < requiredFields.length; i++) {
                        FieldVector fieldVector = fieldVectors.get(i);
                        if (fieldVector.isNull(numRows)) {
                            appendData(i, null);
                        } else {
                            ColumnValue fieldValue = new LanceColumnValue(fieldVector.getObject(numRows));
                            appendData(i, fieldValue);
                        }
                    }
                    numRows++;
                }
                this.vectorSchemaRoot.clear();
            }
            return numRows;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk for table " + this.datasetUri;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (scanner != null) {
                this.vectorSchemaRoot.close();
                this.arrowReader.close();
                this.scanner.close();
            }
        } catch (Exception e) {
            String msg = "Failed to close the lance reader for table " + this.datasetUri;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }
}
