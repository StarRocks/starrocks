// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TResultFileSinkOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// For syntax select * from tbl INTO OUTFILE xxxx
public class OutFileClause implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(OutFileClause.class);

    private static final String BROKER_PROP_PREFIX = "broker.";
    private static final String PROP_BROKER_NAME = "broker.name";
    private static final String PROP_COLUMN_SEPARATOR = "column_separator";
    private static final String PROP_LINE_DELIMITER = "line_delimiter";
    private static final String PROP_MAX_FILE_SIZE = "max_file_size";
    private static final String VIEW_FS_PREFIX = "viewfs://";
    private static final String HDFS_PREFIX = "hdfs://";

    private static final long DEFAULT_MAX_FILE_SIZE_BYTES = 1024 * 1024 * 1024L; // 1GB
    private static final long MIN_FILE_SIZE_BYTES = 5 * 1024 * 1024L; // 5MB
    private static final long MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024L; // 2GB

    private String filePath;
    private String format;
    private Map<String, String> properties;

    // set following members after analyzing
    private String columnSeparator = "\t";
    private String rowDelimiter = "\n";
    private TFileFormatType fileFormatType;
    private long maxFileSizeBytes = DEFAULT_MAX_FILE_SIZE_BYTES;
    private BrokerDesc brokerDesc = null;

    public OutFileClause(String filePath, String format, Map<String, String> properties) {
        this.filePath = filePath;
        this.format = Strings.isNullOrEmpty(format) ? "csv" : format.toLowerCase();
        this.properties = properties;
    }

    public OutFileClause(OutFileClause other) {
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

    public void analyze() throws AnalysisException {
        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }

        if (!format.equals("csv")) {
            throw new AnalysisException("Only support CSV format");
        }
        fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;

        analyzeProperties();

        if (brokerDesc == null) {
            throw new AnalysisException("Must specify BROKER properties in OUTFILE clause");
        }
    }

    private void analyzeProperties() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        Set<String> processedPropKeys = Sets.newHashSet();
        getBrokerProperties(filePath, processedPropKeys);
        if (brokerDesc == null) {
            return;
        }

        if (properties.containsKey(PROP_COLUMN_SEPARATOR)) {
            if (!isCsvFormat()) {
                throw new AnalysisException(PROP_COLUMN_SEPARATOR + " is only for CSV format");
            }
            columnSeparator = properties.get(PROP_COLUMN_SEPARATOR);
            processedPropKeys.add(PROP_COLUMN_SEPARATOR);
        }

        if (properties.containsKey(PROP_LINE_DELIMITER)) {
            if (!isCsvFormat()) {
                throw new AnalysisException(PROP_LINE_DELIMITER + " is only for CSV format");
            }
            rowDelimiter = properties.get(PROP_LINE_DELIMITER);
            processedPropKeys.add(PROP_LINE_DELIMITER);
        }

        if (properties.containsKey(PROP_MAX_FILE_SIZE)) {
            maxFileSizeBytes = ParseUtil.analyzeDataVolumn(properties.get(PROP_MAX_FILE_SIZE));
            if (maxFileSizeBytes > MAX_FILE_SIZE_BYTES || maxFileSizeBytes < MIN_FILE_SIZE_BYTES) {
                throw new AnalysisException("max file size should between 5MB and 2GB. Given: " + maxFileSizeBytes);
            }
            processedPropKeys.add(PROP_MAX_FILE_SIZE);
        }

        if (processedPropKeys.size() != properties.size()) {
            LOG.debug("{} vs {}", processedPropKeys, properties);
            throw new AnalysisException("Unknown properties: " + properties.keySet().stream()
                    .filter(k -> !processedPropKeys.contains(k)).collect(Collectors.toList()));
        }
    }

    private void getBrokerProperties(String filePath, Set<String> processedPropKeys) {
        boolean outfile_without_broker = false;
        if (!properties.containsKey(PROP_BROKER_NAME)) {
            if (filePath.startsWith(HDFS_PREFIX) || filePath.startsWith(VIEW_FS_PREFIX)) {
                outfile_without_broker = true; 
            } else {
                return;
            }
        }
        String brokerName = null;
        if (!outfile_without_broker) {
            brokerName = properties.get(PROP_BROKER_NAME);
            processedPropKeys.add(PROP_BROKER_NAME);
        }

        Map<String, String> brokerProps = Maps.newHashMap();
        Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(BROKER_PROP_PREFIX) && !entry.getKey().equals(PROP_BROKER_NAME)) {
                brokerProps.put(entry.getKey().substring(BROKER_PROP_PREFIX.length()), entry.getValue());
                processedPropKeys.add(entry.getKey());
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

    public TResultFileSinkOptions toSinkOptions() {
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions(filePath, fileFormatType);
        if (isCsvFormat()) {
            sinkOptions.setColumn_separator(columnSeparator);
            sinkOptions.setRow_delimiter(rowDelimiter);
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


