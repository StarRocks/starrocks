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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.connector.Connector;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.connector.hive.HiveWriteUtils;
import com.starrocks.connector.hive.TextFileFormatDesc;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THiveTableSink;

import java.util.List;
import java.util.Optional;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.toTextFileFormatDesc;

public class HiveTableSink extends DataSink {

    protected final TupleDescriptor desc;
    private final String fileFormat;
    private Optional<TextFileFormatDesc> textFileFormatDesc = Optional.empty();
    private final String stagingDir;
    private final List<String> dataColNames;
    private final List<String> partitionColNames;
    private final String compressionType;
    private final long targetMaxFileSize;
    private final boolean isStaticPartitionSink;
    private final String tableIdentifier;
    private final CloudConfiguration cloudConfiguration;

    public HiveTableSink(HiveTable hiveTable, TupleDescriptor desc, boolean isStaticPartitionSink, SessionVariable sessionVariable) {
        this.desc = desc;
        this.stagingDir = HiveWriteUtils.getStagingDir(hiveTable, sessionVariable.getHiveTempStagingDir());
        this.partitionColNames = hiveTable.getPartitionColumnNames();
        this.dataColNames = hiveTable.getDataColumnNames();
        this.tableIdentifier = hiveTable.getUUID();
        this.isStaticPartitionSink = isStaticPartitionSink;
        HiveStorageFormat format = hiveTable.getStorageFormat();
        if (format != HiveStorageFormat.PARQUET && format != HiveStorageFormat.ORC
                && format != HiveStorageFormat.TEXTFILE) {
            throw new StarRocksConnectorException("Writing to hive table in [%s] format is not supported.", format.name());
        }
        this.fileFormat = hiveTable.getStorageFormat().name().toLowerCase();
        if (format == HiveStorageFormat.TEXTFILE) {
            this.textFileFormatDesc = Optional.of(toTextFileFormatDesc(hiveTable.getSerdeProperties()));
            this.compressionType = String.valueOf(TCompressionType.NO_COMPRESSION);
        } else {
            this.compressionType = sessionVariable.getConnectorSinkCompressionCodec();
        }
        this.targetMaxFileSize = sessionVariable.getConnectorSinkTargetMaxFileSize();
        String catalogName = hiveTable.getCatalogName();
        Connector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalogName));

        this.cloudConfiguration = connector.getMetadata().getCloudConfiguration();

        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalogName));
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "Hive TABLE SINK\n");
        strBuilder.append(prefix + "  TABLE: " + tableIdentifier + "\n");
        strBuilder.append(prefix + "  TUPLE ID: " + desc.getId() + "\n");
        strBuilder.append(prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel));
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.HIVE_TABLE_SINK);
        THiveTableSink tHiveTableSink = new THiveTableSink();
        tHiveTableSink.setData_column_names(dataColNames);
        tHiveTableSink.setPartition_column_names(partitionColNames);
        tHiveTableSink.setStaging_dir(stagingDir);
        tHiveTableSink.setFile_format(fileFormat);
        tHiveTableSink.setIs_static_partition_sink(isStaticPartitionSink);
        Preconditions.checkState(CompressionUtils.getConnectorSinkCompressionType(compressionType).isPresent());
        TCompressionType compression = CompressionUtils.getConnectorSinkCompressionType(compressionType).get();
        tHiveTableSink.setCompression_type(compression);
        tHiveTableSink.setTarget_max_file_size(targetMaxFileSize);
        textFileFormatDesc.ifPresent(fileFormatDesc -> tHiveTableSink.setText_file_desc(fileFormatDesc.toThrift()));
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tHiveTableSink.setCloud_configuration(tCloudConfiguration);
        tDataSink.setHive_table_sink(tHiveTableSink);

        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    public String getStagingDir() {
        return stagingDir;
    }
}
