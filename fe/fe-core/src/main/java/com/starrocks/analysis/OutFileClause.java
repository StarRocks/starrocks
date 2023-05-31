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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/OutFileClause.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.StructType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TParquetOptions;
import com.starrocks.thrift.TResultFileSinkOptions;

import java.util.List;
import java.util.Map;
import java.util.Set;

// For syntax select * from tbl INTO OUTFILE xxxx
public class OutFileClause implements ParseNode {
    private static final String CSV_FORMAT = "csv";
    private static final String PARQUET_FORMAT = "parquet";

    public static final Map<String, TCompressionType> PARQUET_COMPRESSION_TYPE_MAP =
            Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    static {
        PARQUET_COMPRESSION_TYPE_MAP.put("snappy", TCompressionType.SNAPPY);
        PARQUET_COMPRESSION_TYPE_MAP.put("gzip", TCompressionType.GZIP);
        PARQUET_COMPRESSION_TYPE_MAP.put("brotli", TCompressionType.BROTLI);
        PARQUET_COMPRESSION_TYPE_MAP.put("zstd", TCompressionType.ZSTD);
        PARQUET_COMPRESSION_TYPE_MAP.put("lz4", TCompressionType.LZ4);
        PARQUET_COMPRESSION_TYPE_MAP.put("lzo", TCompressionType.LZO);
        PARQUET_COMPRESSION_TYPE_MAP.put("bz2", TCompressionType.BZIP2);
        PARQUET_COMPRESSION_TYPE_MAP.put("zlib", TCompressionType.ZLIB);
        PARQUET_COMPRESSION_TYPE_MAP.put("default", TCompressionType.DEFAULT_COMPRESSION);
    }

    public static final Set<PrimitiveType> PARQUET_SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
        PrimitiveType.BOOLEAN,
        PrimitiveType.TINYINT,
        PrimitiveType.SMALLINT,
        PrimitiveType.INT,
        PrimitiveType.BIGINT,
        PrimitiveType.FLOAT,
        PrimitiveType.DOUBLE,
        PrimitiveType.DATE,
        PrimitiveType.DATETIME,
        PrimitiveType.CHAR,
        PrimitiveType.VARCHAR,
        PrimitiveType.DECIMAL32,
        PrimitiveType.DECIMAL64,
        PrimitiveType.DECIMAL128
    );

    public static final Set<PrimitiveType> CSV_SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
        PrimitiveType.BOOLEAN,
        PrimitiveType.TINYINT,
        PrimitiveType.SMALLINT,
        PrimitiveType.INT,
        PrimitiveType.BIGINT,
        PrimitiveType.LARGEINT,
        PrimitiveType.FLOAT,
        PrimitiveType.DOUBLE,
        PrimitiveType.DATE,
        PrimitiveType.DATETIME,
        PrimitiveType.CHAR,
        PrimitiveType.VARCHAR,
        PrimitiveType.DECIMALV2,
        PrimitiveType.DECIMAL32,
        PrimitiveType.DECIMAL64,
        PrimitiveType.DECIMAL128,
        PrimitiveType.JSON,
        PrimitiveType.VARBINARY
    );


    // Old properties still use this prefix, new properties will not.
    private static final String OLD_BROKER_PROP_PREFIX = "broker.";
    private static final String PROP_BROKER_NAME = "broker.name";
    private static final String PROP_COLUMN_SEPARATOR = "column_separator";
    private static final String PROP_LINE_DELIMITER = "line_delimiter";
    private static final String PROP_MAX_FILE_SIZE = "max_file_size";
    public static final String PARQUET_COMPRESSION_TYPE = "compression_type";
    public static final String PARQUET_USE_DICT = "use_dictionary";
    public static final String PARQUET_MAX_ROW_GROUP_SIZE = "max_row_group_bytes";

    private static final long DEFAULT_MAX_FILE_SIZE_BYTES = 1024 * 1024 * 1024L; // 1GB
    private static final long MIN_FILE_SIZE_BYTES = 5 * 1024 * 1024L; // 5MB
    private static final long MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024L; // 2GB
    public static final long DEFAULT_MAX_PARQUET_ROW_GROUP_BYTES = 128 * 1024 * 1024; // 128MB

    private final String filePath;
    private final String format;
    private final Map<String, String> properties;

    // set following members after analyzing
    private String columnSeparator = "\t";
    private String rowDelimiter = "\n";
    private TFileFormatType fileFormatType;
    private long maxFileSizeBytes = DEFAULT_MAX_FILE_SIZE_BYTES;
    private BrokerDesc brokerDesc = null;
    private TCompressionType compressionType = TCompressionType.SNAPPY;
    private long maxParquetRowGroupBytes = DEFAULT_MAX_PARQUET_ROW_GROUP_BYTES;
    private boolean useDict = true;

    private final NodePosition pos;

    public OutFileClause(String filePath, String format, Map<String, String> properties) {
        this(filePath, format, properties, NodePosition.ZERO);
    }

    public OutFileClause(String filePath, String format, Map<String, String> properties, NodePosition pos) {
        this.pos = pos;
        this.filePath = filePath;
        this.format = Strings.isNullOrEmpty(format) ? CSV_FORMAT : format.toLowerCase();
        this.properties = properties;
    }

    public OutFileClause(OutFileClause other) {
        this.pos = other.pos;
        this.filePath = other.filePath;
        this.format = other.format;
        this.properties = other.properties == null ? null : Maps.newHashMap(other.properties);
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getRowDelimiter() {
        return rowDelimiter;
    }

    public TFileFormatType getFileFormatType() {
        return fileFormatType;
    }

    public long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public void analyze(Scope scope) throws SemanticException {
        if (Strings.isNullOrEmpty(filePath)) {
            throw new SemanticException("Must specify file in OUTFILE clause");
        }

        if (format.equals(CSV_FORMAT)) {
            fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        } else if (format.equals(PARQUET_FORMAT)) {
            fileFormatType = TFileFormatType.FORMAT_PARQUET;
        } else {
            throw new SemanticException("Only support CSV and PARQUET format");
        }

        analyzeProperties();

        if (brokerDesc == null) {
            throw new SemanticException("Must specify BROKER properties in OUTFILE clause");
        }

        // check if types are supported
        if (format.equals(PARQUET_FORMAT)) {
            for (Field field : scope.getRelationFields().getAllFields()) {
                if (!isSupportedTypeForParquetOutFile(field.getType())) {
                    throw new SemanticException(String.format("Type %s is not supported for PARQUET outfile", field.getType()));
                }
            }
        }

        if (format.equals(CSV_FORMAT)) {
            for (Field field : scope.getRelationFields().getAllFields()) {
                if (!isSupportedTypeForCSVOutFile(field.getType())) {
                    throw new SemanticException(String.format("Type %s is not supported for CSV outfile", field.getType()));
                }
            }
        }
    }

    private void analyzeProperties() throws SemanticException {
        setBrokerProperties();
        if (brokerDesc == null) {
            return;
        }

        if (properties.containsKey(PROP_COLUMN_SEPARATOR)) {
            if (!isCsvFormat()) {
                throw new SemanticException(PROP_COLUMN_SEPARATOR + " is only for CSV format");
            }
            columnSeparator = properties.get(PROP_COLUMN_SEPARATOR);
        }

        if (properties.containsKey(PROP_LINE_DELIMITER)) {
            if (!isCsvFormat()) {
                throw new SemanticException(PROP_LINE_DELIMITER + " is only for CSV format");
            }
            rowDelimiter = properties.get(PROP_LINE_DELIMITER);
        }

        if (properties.containsKey(PARQUET_COMPRESSION_TYPE)) {
            if (!isParquetFormat()) {
                throw new SemanticException(PARQUET_COMPRESSION_TYPE + " is only for PARQUET format");
            }
            String type = properties.get(PARQUET_COMPRESSION_TYPE);
            if (PARQUET_COMPRESSION_TYPE_MAP.containsKey(type)) {
                compressionType = PARQUET_COMPRESSION_TYPE_MAP.get(type);
            } else {
                throw new SemanticException("compression type is invalid, type: " + type);
            }
        }

        if (properties.containsKey(PARQUET_USE_DICT)) {
            if (!isParquetFormat()) {
                throw new SemanticException(PARQUET_USE_DICT + " is only for PARQUET format");
            }
            useDict = Boolean.parseBoolean(properties.get(PARQUET_USE_DICT));
        }

        if (properties.containsKey(PARQUET_MAX_ROW_GROUP_SIZE)) {
            if (!isParquetFormat()) {
                throw new SemanticException(PARQUET_MAX_ROW_GROUP_SIZE + " is only for PARQUET format");
            }
            try {
                maxParquetRowGroupBytes = Long.parseLong(properties.get(PARQUET_MAX_ROW_GROUP_SIZE));
            } catch (NumberFormatException e) {
                throw new SemanticException(
                        PARQUET_MAX_ROW_GROUP_SIZE + " should be a number of bytes (e.g. 134217728)");
            }
        }

        if (properties.containsKey(PROP_MAX_FILE_SIZE)) {
            try {
                maxFileSizeBytes = ParseUtil.analyzeDataVolumn(properties.get(PROP_MAX_FILE_SIZE));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            if (maxFileSizeBytes > MAX_FILE_SIZE_BYTES || maxFileSizeBytes < MIN_FILE_SIZE_BYTES) {
                throw new SemanticException("max file size should between 5MB and 2GB. Given: " + maxFileSizeBytes);
            }
        }
    }

    private boolean isSupportedTypeForParquetOutFile(Type type) {
        if (type.isScalarType()) {
            PrimitiveType primitiveType = type.getPrimitiveType();
            return PARQUET_SUPPORTED_PRIMITIVE_TYPES.contains(primitiveType);
        }

        // handle complex type
        if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            return isSupportedTypeForParquetOutFile(arrayType.getItemType());
        }

        if (type.isMapType()) {
            MapType mapType = (MapType) type;
            return isSupportedTypeForParquetOutFile(mapType.getKeyType()) && isSupportedTypeForParquetOutFile(mapType.getValueType());
        }

        if (type.isStructType()) {
            StructType structType = (StructType) type;
            for (StructField structField : structType.getFields()) {
                if (!isSupportedTypeForParquetOutFile(structField.getType())) {
                    return false;
                }
            }
            return true;
        }

        // unreachable
        return false;
    }

    private boolean isSupportedTypeForCSVOutFile(Type type) {
        if (type.isScalarType()) {
            PrimitiveType primitiveType = type.getPrimitiveType();
            return CSV_SUPPORTED_PRIMITIVE_TYPES.contains(primitiveType);
        }

        // handle complex type
        if (type.isStructType()) {
            return false;
        }

        if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            return isSupportedTypeForCSVOutFile(arrayType.getItemType());
        }

        if (type.isMapType()) {
            MapType mapType = (MapType) type;
            return isSupportedTypeForCSVOutFile(mapType.getKeyType()) && isSupportedTypeForCSVOutFile(mapType.getValueType());
        }

        // unreachable
        return false;
    }

    private void setBrokerProperties() {
        boolean outfile_without_broker = false;
        if (!properties.containsKey(PROP_BROKER_NAME)) {
            outfile_without_broker = true;
        }
        String brokerName = null;
        if (!outfile_without_broker) {
            brokerName = properties.get(PROP_BROKER_NAME);
        }

        Map<String, String> brokerProps = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(OLD_BROKER_PROP_PREFIX)) {
                brokerProps.put(entry.getKey().substring(OLD_BROKER_PROP_PREFIX.length()), entry.getValue());
            } else {
                // Put new properties without "broker." prefix
                brokerProps.put(entry.getKey(), entry.getValue());
            }
        }
        if (!outfile_without_broker) {
            brokerDesc = new BrokerDesc(brokerName, brokerProps);
        } else {
            brokerDesc = new BrokerDesc(brokerProps);
        }
    }

    private boolean isCsvFormat() {
        return fileFormatType == TFileFormatType.FORMAT_CSV_BZ2
                || fileFormatType == TFileFormatType.FORMAT_CSV_DEFLATE
                || fileFormatType == TFileFormatType.FORMAT_CSV_GZ
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZ4_FRAME
                || fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN;
    }

    private boolean isParquetFormat() {
        return fileFormatType == TFileFormatType.FORMAT_PARQUET;
    }

    @Override
    public OutFileClause clone() {
        return new OutFileClause(this);
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(" INTO OUTFILE '").append(filePath).append(" FORMAT AS ").append(format);
        if (properties != null && !properties.isEmpty()) {
            sb.append(" PROPERTIES(");
            sb.append(new PrintableMap<>(properties, " = ", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public TResultFileSinkOptions toSinkOptions(List<String> columnOutputNames) {
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions(filePath, fileFormatType);
        if (isCsvFormat()) {
            sinkOptions.setColumn_separator(columnSeparator);
            sinkOptions.setRow_delimiter(rowDelimiter);
        } else if (isParquetFormat()) {
            TParquetOptions parquetOptions = new TParquetOptions();
            parquetOptions.setCompression_type(compressionType);
            parquetOptions.setParquet_max_group_bytes(maxParquetRowGroupBytes);
            parquetOptions.setUse_dict(useDict);
            sinkOptions.setParquet_options(parquetOptions);
            sinkOptions.setFile_column_names(columnOutputNames);
        }

        sinkOptions.setMax_file_size_bytes(maxFileSizeBytes);
        if (brokerDesc != null) {
            if (!brokerDesc.hasBroker()) {
                sinkOptions.setUse_broker(false);
                sinkOptions.setHdfs_write_buffer_size_kb(Config.hdfs_write_buffer_size_kb);
                THdfsProperties hdfsProperties = new THdfsProperties();
                try {
                    HdfsUtil.getTProperties(filePath, brokerDesc, hdfsProperties);
                } catch (UserException e) {
                    throw new SemanticException(e.getMessage());
                }
                sinkOptions.setHdfs_properties(hdfsProperties);
            } else {
                sinkOptions.setUse_broker(true);
            }
            sinkOptions.setBroker_properties(brokerDesc.getProperties());
            // broker_addresses of sinkOptions will be set in Coordinator.
            // Because we need to choose the nearest broker with the result sink node.
        }
        return sinkOptions;
    }
}


