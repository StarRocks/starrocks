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

package com.starrocks.odps.reader;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OdpsSplitScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(OdpsSplitScanner.class);

    private final String projectName;
    private final String tableName;
    private final String sessionId;
    private final int splitIndex;
    private final String[] requiredFields;
    private final Column[] requireColumns;
    private final ColumnType[] requiredTypes;
    private final int fetchSize;
    private final ClassLoader classLoader;
    private final EnvironmentSettings settings;
    private final TableBatchReadSession scan;
    private SplitReader<VectorSchemaRoot> reader;
    private Map<String, Integer> nameIndexMap;

    public OdpsSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.projectName = params.get("project_name");
        this.tableName = params.get("table_name");
        this.requiredFields = params.get("required_fields").split(",");
        this.sessionId = params.get("session_id");
        this.splitIndex = Integer.parseInt(params.get("split_index"));
        String serializedScan = params.get("read_session");
        try {
            this.scan = (TableBatchReadSession) deserialize(serializedScan);
        } catch (Exception e) {
            throw new RuntimeException("deserialize read session error", e);
        }
        Account account = new AliyunAccount(params.get("access_id"), params.get("access_key"));
        Odps odps = new Odps(account);
        TableSchema schema = odps.tables().get(projectName, tableName).getSchema();
        Map<String, Column> nameColumnMap = schema.getColumns().stream()
                .collect(Collectors.toMap(Column::getName, o -> o));
        nameColumnMap.putAll(schema.getPartitionColumns().stream().collect(Collectors.toMap(Column::getName, o -> o)));
        requireColumns = new Column[requiredFields.length];
        requiredTypes = new ColumnType[requiredFields.length];
        nameIndexMap = new HashMap<>();
        for (int i = 0; i < requiredFields.length; i++) {
            requireColumns[i] = nameColumnMap.get(requiredFields[i]);
            requiredTypes[i] = OdpsTypeUtils.convertToColumnType(requireColumns[i]);
            nameIndexMap.put(requiredFields[i], i);
        }
        settings =
                EnvironmentSettings.newBuilder().withServiceEndpoint(params.get("endpoint"))
                        .withCredentials(Credentials.newBuilder().withAccount(account).build()).build();
        this.classLoader = this.getClass().getClassLoader();
        LOG.info("endpoint: {}", params.get("endpoint"));
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            reader = scan.createArrowReader(
                    new IndexedInputSplit(sessionId, splitIndex),
                    ReaderOptions.newBuilder().withMaxBatchRowCount(fetchSize).withCompressionCodec(CompressionCodec.ZSTD)
                            .withSettings(settings).build());
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            close();
            String msg = "Failed to open the odps reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (reader != null) {
                reader.close();
            }
        } catch (Exception e) {
            String msg = "Failed to close the odps reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    // because of fe must reorder the column name, so the data's order are different from requiredFields
    // we make data is right order
    // right order: fieldVectors, columnAccessors
    // wrong order: requireColumns, requireFields

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (reader.hasNext()) {
                VectorSchemaRoot vectorSchemaRoot = reader.get();
                List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
                ArrowVectorAccessor[] columnAccessors = new ArrowVectorAccessor[requireColumns.length];
                List<Field> fields = vectorSchemaRoot.getSchema().getFields();
                for (int i = 0; i < fieldVectors.size(); i++) {
                    String filedName = fields.get(i).getName();
                    int fieldIndex = nameIndexMap.get(filedName);
                    columnAccessors[i] =
                            OdpsTypeUtils.createColumnVectorAccessor(fieldVectors.get(i),
                                    requireColumns[fieldIndex].getTypeInfo());
                }
                for (int rowId = 0; rowId < fieldVectors.size(); rowId++) {
                    String filedName = fields.get(rowId).getName();
                    int fieldIndex = nameIndexMap.get(filedName);
                    for (int index = 0; index < vectorSchemaRoot.getRowCount(); index++) {
                        Object data = OdpsTypeUtils.getData(columnAccessors[rowId], requireColumns[fieldIndex].getTypeInfo(),
                                index);
                        if (data == null) {
                            appendData(fieldIndex, null);
                        } else {
                            appendData(fieldIndex, new OdpsColumnValue(data, requireColumns[fieldIndex].getTypeInfo()));
                        }
                    }
                }
                return vectorSchemaRoot.getRowCount();
            }
            return 0;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk of odps.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("projectName: ");
        sb.append(projectName);
        sb.append("\n");
        sb.append("splitIndex: ");
        sb.append(splitIndex);
        sb.append("\n");
        sb.append("tableName: ");
        sb.append(tableName);
        sb.append("\n");
        sb.append("sessionId: ");
        sb.append(sessionId);
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("fetchSize: ");
        sb.append(fetchSize);
        sb.append("\n");
        return sb.toString();
    }

    private static Object deserialize(String serializedString) throws IOException, ClassNotFoundException {
        byte[] serializedBytes = Base64.getDecoder().decode(serializedString);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return objectInputStream.readObject();
    }
}
