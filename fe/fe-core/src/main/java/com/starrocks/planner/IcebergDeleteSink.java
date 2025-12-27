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

import com.starrocks.catalog.IcebergTable;;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.Table;

import static com.starrocks.sql.ast.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;

/**
 * IcebergDeleteSink is used to support delete operations to Iceberg tables.
 * It supports write position delete files
 * <p>
 * Required columns:
 * - _file (STRING): Path of the data file
 * - _pos (BIGINT): Row position within the file
 */
public class IcebergDeleteSink extends DataSink {
    protected final TupleDescriptor desc;
    private IcebergTable icebergTable;
    private final long targetTableId;
    private final String tableLocation;
    private final String dataLocation;
    private final String compressionType;
    private final long targetMaxFileSize;
    private final String tableIdentifier;
    private CloudConfiguration cloudConfiguration;

    /**
     * Constructor for IcebergDeleteSink
     *
     * @param icebergTable    The target Iceberg table
     * @param desc            Tuple descriptor containing operation columns
     * @param sessionVariable Session variables for configuration
     */
    public IcebergDeleteSink(IcebergTable icebergTable, TupleDescriptor desc,
                             SessionVariable sessionVariable) {
        this.icebergTable = icebergTable;
        Table nativeTable = icebergTable.getNativeTable();
        this.desc = desc;
        this.tableLocation = nativeTable.location();
        this.dataLocation = IcebergUtil.tableDataLocation(nativeTable);
        this.targetTableId = icebergTable.getId();
        this.tableIdentifier = icebergTable.getUUID();
        this.compressionType = sessionVariable.getConnectorSinkCompressionCodec();
        this.targetMaxFileSize = sessionVariable.getConnectorSinkTargetMaxFileSize() > 0 ?
                sessionVariable.getConnectorSinkTargetMaxFileSize() : 1024L * 1024 * 1024;
    }

    public void init() {
        String catalogName = icebergTable.getCatalogName();
        this.cloudConfiguration = IcebergUtil.getVendedCloudConfiguration(catalogName, icebergTable);
        // Validate tuple descriptor contains required columns
        validateDeleteTuple(desc);
    }

    /**
     * Validate that the tuple descriptor contains required columns
     *
     * @param desc The tuple descriptor to validate
     */
    private void validateDeleteTuple(TupleDescriptor desc) {
        boolean hasFilePathColumn = false;
        boolean hasPosColumn = false;

        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() != null) {
                String colName = slot.getColumn().getName();
                if (IcebergTable.FILE_PATH.equals(colName)) {
                    hasFilePathColumn = true;
                    if (!slot.getType().equals(VarcharType.VARCHAR)) {
                        throw new StarRocksConnectorException("_file column must be type of VARCHAR");
                    }
                } else if (IcebergTable.ROW_POSITION.equals(colName)) {
                    hasPosColumn = true;
                    if (!slot.getType().equals(IntegerType.BIGINT)) {
                        throw new StarRocksConnectorException("_pos column must be type of BIGINT");
                    }
                }
            }
        }

        if (!hasFilePathColumn || !hasPosColumn) {
            throw new StarRocksConnectorException("IcebergDeleteSink requires _file and _pos columns in tuple descriptor");
        }
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("ICEBERG DELETE SINK");
        strBuilder.append("\n");
        strBuilder.append(prefix).append("  TABLE: ").append(tableIdentifier).append("\n");
        strBuilder.append(prefix).append("  LOCATION: ").append(tableLocation).append("\n");
        strBuilder.append(prefix).append("  TUPLE ID: ").append(desc.getId()).append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.ICEBERG_DELETE_SINK);
        TIcebergTableSink tIcebergTableSink = new TIcebergTableSink();
        tIcebergTableSink.setTarget_table_id(targetTableId);
        tIcebergTableSink.setTuple_id(desc.getId().asInt());
        tIcebergTableSink.setLocation(tableLocation);
        // For delete sink, we set both data and delete locations
        tIcebergTableSink.setData_location(dataLocation);
        tIcebergTableSink.setFile_format("parquet"); // Delete files are always parquet
        tIcebergTableSink.setIs_static_partition_sink(false);
        TCompressionType compression = PARQUET_COMPRESSION_TYPE_MAP.get(compressionType);
        tIcebergTableSink.setCompression_type(compression);
        tIcebergTableSink.setTarget_max_file_size(targetMaxFileSize);
        com.starrocks.thrift.TCloudConfiguration tCloudConfiguration = new com.starrocks.thrift.TCloudConfiguration();
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