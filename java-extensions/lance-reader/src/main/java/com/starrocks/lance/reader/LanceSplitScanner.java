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

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class LanceSplitScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(LanceSplitScanner.class);

    private final String datasetUri;
    private final String[] requiredFields;
    private final int fetchSize;

    private Dataset dataset;
    private LanceScanner scanner;
    private ArrowReader arrowReader;
    private VectorSchemaRoot currentRoot;
    private int currentRowInBatch;
    private int currentBatchRows;
    private boolean readerExhausted;

    public LanceSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(
                params.get("required_fields"), ",");
        this.datasetUri = params.get("lance_dataset_uri");
    }

    @Override
    public void open() throws IOException {
        try {
            LOG.info("Open lance reader with dataset URI: {}", datasetUri);

            dataset = Dataset.open(datasetUri);
            Schema schema = dataset.getSchema();
            Map<String, String> typeMap = LanceTypeUtils.buildTypeMapping(schema);

            ColumnType[] requiredTypes = new ColumnType[requiredFields.length];
            for (int i = 0; i < requiredFields.length; i++) {
                String typeStr = typeMap.getOrDefault(requiredFields[i], "string");
                requiredTypes[i] = new ColumnType(typeStr);
            }

            ScanOptions scanOptions = new ScanOptions.Builder()
                    .columns(Arrays.asList(requiredFields))
                    .batchSize(fetchSize)
                    .build();
            scanner = dataset.newScan(scanOptions);
            arrowReader = scanner.scanBatches();
            currentRowInBatch = 0;
            currentBatchRows = 0;
            readerExhausted = false;

            LOG.info("init off-heap table writer with requiredFields: {}, requiredTypes: {}, fetchSize: {}",
                    Arrays.toString(requiredFields), Arrays.toString(requiredTypes), fetchSize);
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            close();
            String msg = "Failed to open the lance reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            LOG.info("Closing lance reader for dataset: {}", datasetUri);
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
        } catch (Exception e) {
            String msg = "Failed to close the lance reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try {
            int numRows = 0;
            while (numRows < fetchSize) {
                if (currentRowInBatch >= currentBatchRows) {
                    if (!loadNextBatch()) {
                        break;
                    }
                }
                for (int i = 0; i < requiredFields.length; i++) {
                    FieldVector vector = currentRoot.getVector(requiredFields[i]);
                    if (vector.isNull(currentRowInBatch)) {
                        appendData(i, null);
                    } else {
                        appendData(i, new LanceColumnValue(vector, currentRowInBatch));
                    }
                }
                currentRowInBatch++;
                numRows++;
            }
            return numRows;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk of lance.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    /**
     * Load the next batch from the ArrowReader.
     *
     * @return true if a new batch was loaded, false if the reader is exhausted
     */
    private boolean loadNextBatch() throws IOException {
        if (readerExhausted) {
            return false;
        }
        if (arrowReader.loadNextBatch()) {
            currentRoot = arrowReader.getVectorSchemaRoot();
            currentBatchRows = currentRoot.getRowCount();
            currentRowInBatch = 0;
            return true;
        }
        readerExhausted = true;
        return false;
    }

    @Override
    public String toString() {
        return "LanceSplitScanner{"
                + "datasetUri='" + datasetUri + '\''
                + ", requiredFields=" + Arrays.toString(requiredFields)
                + ", fetchSize=" + fetchSize
                + '}';
    }
}
