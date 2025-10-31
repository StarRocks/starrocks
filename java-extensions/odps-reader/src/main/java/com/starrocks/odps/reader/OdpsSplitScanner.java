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
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.metrics.Metric;
import com.aliyun.odps.table.metrics.MetricNames;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.aliyun.odps.utils.StringUtils;
import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class OdpsSplitScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(OdpsSplitScanner.class);

    private final String projectName;
    private final String tableName;
    private final String endpoint;
    private final InputSplit inputSplit;
    private final String[] requiredFields;
    private final Column[] requireColumns;
    private final ColumnType[] requiredTypes;
    private final int fetchSize;
    private final ClassLoader classLoader;
    private final EnvironmentSettings settings;
    private final TableBatchReadSession scan;
    private SplitReader<VectorSchemaRoot> reader;
    private Map<String, Integer> nameIndexMap;

    private final long startTime;
    private final long splitId;

    private final String timezone;

    public OdpsSplitScanner(int fetchSize, Map<String, String> params) throws IOException {
        this.fetchSize = fetchSize;
        this.projectName = params.get("project_name");
        this.tableName = params.get("table_name");
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(params.get("required_fields"), ",");

        this.scan = new TableReadSessionBuilder().fromJson(params.get("read_session")).buildBatchReadSession();
        String splitPolicy = params.get("split_policy");
        String sessionId = params.get("session_id");
        switch (splitPolicy) {
            case "size":
                this.inputSplit = new IndexedInputSplit(sessionId, Integer.parseInt(params.get("split_index")));
                this.splitId = Long.parseLong(params.get("split_index"));
                break;
            case "row_offset":
                this.inputSplit = new RowRangeInputSplit(sessionId, Long.parseLong(params.get("start_index")),
                        Long.parseLong(params.get("num_record")));
                this.splitId = Long.parseLong(params.get("start_index"));
                break;
            default:
                throw new RuntimeException("unknown split policy: " + splitPolicy);
        }
        LOG.info("Start read split {}.", splitId);
        this.endpoint = params.get("endpoint");

        this.classLoader = this.getClass().getClassLoader();
        Account account = new AliyunAccount(params.get("access_id"), params.get("access_key"));
        Odps odps = new Odps(account);
        odps.setEndpoint(endpoint);

        Map<String, Column> nameColumnMap = scan.readSchema().getColumns().stream()
                .collect(Collectors.toMap(Column::getName, o -> o));
        requireColumns = new Column[requiredFields.length];
        requiredTypes = new ColumnType[requiredFields.length];
        nameIndexMap = new HashMap<>();
        for (int i = 0; i < requiredFields.length; i++) {
            requireColumns[i] = nameColumnMap.get(requiredFields[i]);
            requiredTypes[i] = OdpsTypeUtils.convertToColumnType(requireColumns[i]);
            nameIndexMap.put(requiredFields[i], i);
        }
        EnvironmentSettings.Builder builder = EnvironmentSettings.newBuilder().withServiceEndpoint(endpoint)
                .withRestOptions(RestOptions.newBuilder().witUserAgent("StarRocks").build())
                .withCredentials(Credentials.newBuilder().withAccount(account).build());
        if (!StringUtils.isNullOrEmpty(params.get("tunnel_endpoint"))) {
            builder.withTunnelEndpoint(params.get("tunnel_endpoint"));
        }
        if (!StringUtils.isNullOrEmpty(params.get("quota_name"))) {
            builder.withQuotaName(params.get("quota_name"));
        }
        settings = builder.build();

        this.timezone = params.get("time_zone");
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            reader = scan.createArrowReader(this.inputSplit,
                    ReaderOptions.newBuilder().withMaxBatchRowCount(fetchSize)
                            .withCompressionCodec(CompressionCodec.ZSTD)
                            .withSettings(settings).build());
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            close();
            String msg = "Failed to open the odps reader: ";
            LOG.error("{}{}", msg, e.getMessage(), e);
            throw new IOException(msg + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Optional<Metric> bytesCount = this.reader.currentMetricsValues().get(MetricNames.BYTES_COUNT);
            Optional<Metric> recordCount = this.reader.currentMetricsValues().get(MetricNames.RECORD_COUNT);
            String bytesStr = bytesCount.map(metric -> formatBytes(((BytesCount) metric).getValue())).orElse("N/A");
            String totalRowCount =
                    recordCount.map(metric -> String.valueOf(((RecordCount) metric).getCount())).orElse("N/A");
            LOG.info("ODPS Split Summary - SplitId: {}, BytesRead: {}, TotalRowCount: {}, ScanTime: {} ms",
                    this.splitId, bytesStr, totalRowCount, System.currentTimeMillis() - this.startTime);

            if (reader != null) {
                reader.close();
            }
        } catch (Exception e) {
            String msg = "Failed to close the odps reader: ";
            LOG.error("{}{}", msg, e.getMessage(), e);
            throw new IOException(msg + e.getMessage(), e);
        }
    }

    // because of fe must reorder the column name, so the data's order are different from requiredFields
    // we make data is right order
    // right order: fieldVectors, columnAccessors
    // wrong order: requireColumns, requireFields

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            VectorSchemaRoot vectorSchemaRoot;
            if (reader.hasNext()) {
                vectorSchemaRoot = reader.get();
            } else {
                return 0;
            }
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
            int rowCount = vectorSchemaRoot.getRowCount();
            for (int rowId = 0; rowId < fieldVectors.size(); rowId++) {
                String filedName = fields.get(rowId).getName();
                int fieldIndex = nameIndexMap.get(filedName);
                for (int index = 0; index < rowCount; index++) {
                    Object data =
                            OdpsTypeUtils.getData(columnAccessors[rowId], requireColumns[fieldIndex].getTypeInfo(),
                                    index);
                    if (data == null) {
                        appendData(fieldIndex, null);
                    } else {
                        appendData(fieldIndex,
                                new OdpsColumnValue(data, requireColumns[fieldIndex].getTypeInfo(), timezone));
                    }
                }
            }
            return rowCount;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk of odps: ";
            LOG.error("{}{}", msg, e.getMessage(), e);
            throw new IOException(msg + e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("projectName: ");
        sb.append(projectName);
        sb.append("\n");
        sb.append("\n");
        sb.append("tableName: ");
        sb.append(tableName);
        sb.append("\n");
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("fetchSize: ");
        sb.append(fetchSize);
        sb.append("\n");
        return sb.toString();
    }

    private String formatBytes(long bytes) {
        if (bytes < 0) {
            return "N/A";
        }
        if (bytes < 1024) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        char unit = "KMGTPE".charAt(exp - 1);
        double value = bytes / Math.pow(1024, exp);
        return String.format("%.2f %ciB", value, unit);
    }
}
