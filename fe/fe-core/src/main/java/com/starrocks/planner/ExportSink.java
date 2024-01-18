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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/ExportSink.java

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

package com.starrocks.planner;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.conf.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TExportSink;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.lang.StringEscapeUtils;

public class ExportSink extends DataSink {
    private final String exportPath;
    private String fileNamePrefix;
    private final String columnSeparator;
    private final String rowDelimiter;
    private final BrokerDesc brokerDesc;
    private final THdfsProperties hdfsProperties;

    public ExportSink(String exportPath, String fileNamePrefix, String columnSeparator,
                      String rowDelimiter, BrokerDesc brokerDesc, THdfsProperties hdfsProperties) {
        this.exportPath = exportPath;
        this.fileNamePrefix = fileNamePrefix;
        this.columnSeparator = columnSeparator;
        this.rowDelimiter = rowDelimiter;
        this.brokerDesc = brokerDesc;
        this.hdfsProperties = hdfsProperties;
    }

    // for insert broker table
    public ExportSink(String exportPath, String columnSeparator,
                      String rowDelimiter, BrokerDesc brokerDesc) {
        this(exportPath, null, columnSeparator, rowDelimiter, brokerDesc, null);
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix + "EXPORT SINK\n");
        sb.append(prefix + "  path=" + exportPath + "\n");
        sb.append(prefix + "  columnSeparator="
                + StringEscapeUtils.escapeJava(columnSeparator) + "\n");
        sb.append(prefix + "  rowDelimiter="
                + StringEscapeUtils.escapeJava(rowDelimiter) + "\n");
        sb.append(prefix + "  broker_name=" + brokerDesc.getName() + " property("
                + new PrintableMap<String, String>(
                brokerDesc.getProperties(), "=", true, false)
                + ")");
        sb.append("\n");
        return sb.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.EXPORT_SINK);
        TExportSink tExportSink = new TExportSink(TFileType.FILE_BROKER, exportPath, columnSeparator, rowDelimiter);

        FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getAnyBroker(brokerDesc.getName());
        if (broker != null) {
            tExportSink.addToBroker_addresses(new TNetworkAddress(broker.ip, broker.port));
        }
        tExportSink.setUse_broker(brokerDesc.hasBroker());
        tExportSink.setHdfs_write_buffer_size_kb(Config.hdfs_write_buffer_size_kb);
        if (!brokerDesc.hasBroker()) {
            tExportSink.setHdfs_properties(this.hdfsProperties);
        }
        tExportSink.setProperties(brokerDesc.getProperties());

        if (fileNamePrefix != null) {
            tExportSink.setFile_name_prefix(fileNamePrefix);
        }

        result.setExport_sink(tExportSink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

}
