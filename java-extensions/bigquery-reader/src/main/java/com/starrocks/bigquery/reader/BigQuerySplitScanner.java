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

package com.starrocks.bigquery.reader;

import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BigQuerySplitScanner extends ConnectorScanner {
    private static final Logger LOG = LogManager.getLogger(BigQuerySplitScanner.class);

    private final String readStreamName;
    private final String[] requiredFields;
    private final ColumnType[] requiredTypes;
    private final int fetchSize;
    private final ClassLoader classLoader;
    private final BigQueryReadClient readClient;

    /** Arrow schema bytes from ReadSession, used to build VectorSchemaRoot. */
    private final byte[] arrowSchemaBytes;

    private ServerStream<ReadRowsResponse> responseStream;
    private Iterator<ReadRowsResponse> responseIterator;
    private VectorSchemaRoot vectorSchemaRoot;
    private final BufferAllocator allocator;

    public BigQuerySplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.readStreamName = params.get("read_stream_name");
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(params.get("required_fields"), ",");
        this.classLoader = this.getClass().getClassLoader();
        this.allocator = new RootAllocator(Long.MAX_VALUE);

        String schemaBase64 = params.getOrDefault("arrow_schema_base64", "");
        this.arrowSchemaBytes = schemaBase64.isEmpty()
                ? new byte[0]
                : Base64.getDecoder().decode(schemaBase64);

        GoogleCredentials credentials = buildCredentials(params);
        this.readClient = buildReadClient(credentials);

        // ColumnType array: use "varchar" for all fields as the off-heap table will coerce values
        // via the ColumnValue interface methods (getDate, getDateTime, getLong, etc.).
        requiredTypes = new ColumnType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            requiredTypes[i] = new ColumnType("varchar");
        }
    }

    private GoogleCredentials buildCredentials(Map<String, String> params) {
        String credBase64 = params.getOrDefault("credentials_base64", "");
        if (!credBase64.isEmpty()) {
            try {
                String decoded = new String(Base64.getDecoder().decode(credBase64), StandardCharsets.UTF_8);
                if (decoded.startsWith("access_token:")) {
                    String token = decoded.substring("access_token:".length());
                    return GoogleCredentials.create(new AccessToken(token, null));
                }
            } catch (Exception e) {
                LOG.warn("Could not decode credentials_base64; falling back to ADC: {}", e.getMessage());
            }
        }
        // Fall back to Application Default Credentials (GCE node SA, Workload Identity, etc.)
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new RuntimeException("No BigQuery credentials available: " + e.getMessage(), e);
        }
    }

    private BigQueryReadClient buildReadClient(GoogleCredentials credentials) {
        try {
            return BigQueryReadClient.create(
                    BigQueryReadSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create BigQueryReadClient: " + e.getMessage(), e);
        }
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            // Build VectorSchemaRoot from the Arrow schema serialised in the ReadSession.
            if (arrowSchemaBytes.length > 0) {
                ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(arrowSchemaBytes));
                Schema arrowSchema = MessageSerializer.deserializeSchema(
                        new ReadChannel(channel));
                vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);
            }

            ReadRowsRequest request = ReadRowsRequest.newBuilder()
                    .setReadStream(readStreamName)
                    .build();
            responseStream = readClient.readRowsCallable().call(request);
            responseIterator = responseStream.iterator();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            close();
            String msg = "Failed to open BigQuery reader for stream " + readStreamName + ": ";
            LOG.error("{}{}", msg, e.getMessage(), e);
            throw new IOException(msg + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (responseStream != null) {
                responseStream.cancel();
                responseStream = null;
            }
            if (vectorSchemaRoot != null) {
                vectorSchemaRoot.close();
                vectorSchemaRoot = null;
            }
            if (readClient != null) {
                readClient.close();
            }
            if (allocator != null) {
                allocator.close();
            }
        } catch (Exception e) {
            String msg = "Failed to close BigQuery reader for stream " + readStreamName + ": ";
            LOG.error("{}{}", msg, e.getMessage(), e);
            throw new IOException(msg + e.getMessage(), e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (responseIterator == null || !responseIterator.hasNext()) {
                return 0;
            }
            ReadRowsResponse response = responseIterator.next();
            if (!response.hasArrowRecordBatch()) {
                return 0;
            }

            byte[] batchBytes = response.getArrowRecordBatch()
                    .getSerializedRecordBatch().toByteArray();

            // Deserialise the Arrow RecordBatch IPC message and load it into the VectorSchemaRoot.
            ArrowBuf buf = allocator.buffer(batchBytes.length);
            try {
                buf.writeBytes(batchBytes);
                ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(batchBytes));
                ArrowRecordBatch recordBatch = MessageSerializer.deserializeRecordBatch(
                        new ReadChannel(channel), allocator);
                try {
                    if (vectorSchemaRoot != null) {
                        VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                        loader.load(recordBatch);
                    }
                } finally {
                    recordBatch.close();
                }
            } finally {
                buf.close();
            }

            if (vectorSchemaRoot == null) {
                return 0;
            }

            int rowCount = vectorSchemaRoot.getRowCount();
            List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();

            for (int col = 0; col < requiredFields.length; col++) {
                String fieldName = requiredFields[col];
                FieldVector vector = findVector(fieldVectors, fieldName);
                if (vector == null) {
                    for (int row = 0; row < rowCount; row++) {
                        appendData(col, null);
                    }
                    continue;
                }
                for (int row = 0; row < rowCount; row++) {
                    if (vector.isNull(row)) {
                        appendData(col, null);
                    } else {
                        Object val = BigQueryTypeUtils.getArrowValue(vector, row);
                        appendData(col, val != null ? new BigQueryColumnValue(val) : null);
                    }
                }
            }
            return rowCount;
        } catch (Exception e) {
            close();
            String msg = "Failed to get next batch from BigQuery stream " + readStreamName + ": ";
            LOG.error("{}{}", msg, e.getMessage(), e);
            throw new IOException(msg + e.getMessage(), e);
        }
    }

    private FieldVector findVector(List<FieldVector> vectors, String name) {
        for (FieldVector v : vectors) {
            if (v.getName().equalsIgnoreCase(name)) {
                return v;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "BigQuerySplitScanner{readStreamName='" + readStreamName + '\'' +
                ", requiredFields=" + Arrays.toString(requiredFields) +
                ", fetchSize=" + fetchSize + '}';
    }
}
