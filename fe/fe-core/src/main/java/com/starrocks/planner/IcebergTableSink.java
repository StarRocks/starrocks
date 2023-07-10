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
import com.starrocks.catalog.Type;
import com.starrocks.connector.Connector;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import org.apache.iceberg.Table;

import java.util.Locale;

import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT;

public class IcebergTableSink extends DataSink {
    public final static int ICEBERG_SINK_MAX_DOP = 32;
    protected final TupleDescriptor desc;
    private final long targetTableId;
    private final String fileFormat;
    private final String location;
    private final String compressionType;
    private final boolean isStaticPartitionSink;
    private final String tableIdentifier;
    private final CloudConfiguration cloudConfiguration;

    public IcebergTableSink(IcebergTable icebergTable, TupleDescriptor desc, boolean isStaticPartitionSink) {
        Table nativeTable = icebergTable.getNativeTable();
        this.desc = desc;
        this.location = nativeTable.location();
        this.targetTableId = icebergTable.getId();
        this.tableIdentifier = icebergTable.getUUID();
        this.isStaticPartitionSink = isStaticPartitionSink;
        this.fileFormat = nativeTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toLowerCase();
        switch (fileFormat) {
            case "parquet":
                compressionType = nativeTable.properties().getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT)
                        .toLowerCase(Locale.ROOT);
                break;
            case "orc":
                compressionType = nativeTable.properties().getOrDefault(ORC_COMPRESSION, ORC_COMPRESSION_DEFAULT)
                        .toLowerCase(Locale.ROOT);
                break;
            default:
                compressionType = "default";
        }
        String catalogName = icebergTable.getCatalogName();
        Connector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalogName));

        // Try to set for tabular
        if (icebergTable.getNativeTable().io().properties().containsKey(IcebergRESTCatalog.KEY_ENABLE_TABULAR_SUPPORT)) {
            CloudConfiguration tabularTempCloudConfiguration = CloudConfigurationFactory.
                    buildCloudConfigurationForTabular(icebergTable.getNativeTable().io().properties());
            // Tabular must using aws
            Preconditions.checkArgument(tabularTempCloudConfiguration.getCloudType() == CloudType.AWS,
                    "For tabular, we must using AWS S3's parameters");
            this.cloudConfiguration = tabularTempCloudConfiguration;
        } else {
            this.cloudConfiguration = connector.getCloudConfiguration();
        }

        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalogName));
    }

    public static boolean isUnSupportedPartitionColumnType(Type type) {
        return type.isFloat() || type.isDecimalOfAnyVersion() || type.isDatetime();
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
    public boolean canUsePipeLine() {
        return true;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
