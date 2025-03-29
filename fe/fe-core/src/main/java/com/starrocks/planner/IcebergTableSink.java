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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import org.apache.iceberg.Table;

import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergTableSink extends DataSink {
    public final static int ICEBERG_SINK_MAX_DOP = 32;
    protected final TupleDescriptor desc;
    private final long targetTableId;
    private final String fileFormat;
    private final String location;
    private final String compressionType;
    private final long targetMaxFileSize;
    private final boolean isStaticPartitionSink;
    private final String tableIdentifier;
    private final CloudConfiguration cloudConfiguration;
    private String targetBranch;

    public IcebergTableSink(IcebergTable icebergTable, TupleDescriptor desc, boolean isStaticPartitionSink,
                            SessionVariable sessionVariable, String targetBranch) {
        Table nativeTable = icebergTable.getNativeTable();
        this.desc = desc;
        this.location = nativeTable.location();
        this.targetTableId = icebergTable.getId();
        this.tableIdentifier = icebergTable.getUUID();
        this.isStaticPartitionSink = isStaticPartitionSink;
        this.fileFormat = nativeTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toLowerCase();
        this.compressionType = sessionVariable.getConnectorSinkCompressionCodec();
        this.targetMaxFileSize = sessionVariable.getConnectorSinkTargetMaxFileSize();
        this.targetBranch = targetBranch;

        String catalogName = icebergTable.getCatalogName();
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalogName));

        // Try to set for tabular
        CloudConfiguration tabularTempCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForVendedCredentials(icebergTable.getNativeTable().io().properties());
        if (tabularTempCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            this.cloudConfiguration = tabularTempCloudConfiguration;
        } else {
            this.cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        }

        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalogName));
    }

    public String getTargetBranch() {
        return targetBranch;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "Iceberg TABLE SINK\n");
        strBuilder.append(prefix + "  TABLE: " + tableIdentifier + "\n");
        strBuilder.append(prefix + "  TUPLE ID: " + desc.getId() + "\n");
        strBuilder.append(prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel));
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
        TIcebergTableSink tIcebergTableSink = new TIcebergTableSink();
        tIcebergTableSink.setTarget_table_id(targetTableId);
        tIcebergTableSink.setLocation(location);
        tIcebergTableSink.setFile_format(fileFormat);
        tIcebergTableSink.setIs_static_partition_sink(isStaticPartitionSink);
        TCompressionType compression = PARQUET_COMPRESSION_TYPE_MAP.get(compressionType);
        tIcebergTableSink.setCompression_type(compression);
        tIcebergTableSink.setTarget_max_file_size(targetMaxFileSize);
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tIcebergTableSink.setCloud_configuration(tCloudConfiguration);

        tDataSink.setIceberg_table_sink(tIcebergTableSink);
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
}
