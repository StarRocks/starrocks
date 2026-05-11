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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.thrift.TIcebergWriteMode;
import org.apache.iceberg.Table;

import static com.starrocks.sql.ast.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;

/**
 * IcebergRowDeltaSink is used to support UPDATE operations on Iceberg tables
 * using the RowDelta model. It writes both position delete files and new data
 * files in a single atomic operation.
 * <p>
 * Required columns:
 * - _file (STRING): Path of the data file
 * - _pos (BIGINT): Row position within the file
 * - Plus data columns for the updated rows
 */
public class IcebergRowDeltaSink extends DataSink {
    public static final String WRITE_MODE_ROW_DELTA = "ROW_DELTA";

    /**
     * Operation codes for row-level delta operations.
     * Each row in the RowDelta output carries an op_code that tells the BE sink
     * how to route it. Values must stay in sync with BE IcebergRowDeltaSink::OP_*.
     */
    public enum OpCode {
        NO_OP(0),           // discard (MERGE row matches no WHEN clause)
        DELETE(1),          // position delete only
        UPDATE(2),          // position delete + new data row
        INSERT(3);          // new data row only

        private final int value;

        OpCode(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

    protected final TupleDescriptor desc;
    private IcebergTable icebergTable;
    private final long targetTableId;
    private final String fileFormat;
    private final String tableLocation;
    private final String dataLocation;
    private final String dataCompressionType;
    private final String deleteCompressionType;
    private final long targetMaxFileSize;
    private final String tableIdentifier;
    private CloudConfiguration cloudConfiguration;
    private com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra sinkExtraInfo;

    /**
     * Constructor for IcebergRowDeltaSink
     *
     * @param icebergTable    The target Iceberg table
     * @param desc            Tuple descriptor containing operation columns
     * @param sessionVariable Session variables for configuration
     */
    public IcebergRowDeltaSink(IcebergTable icebergTable, TupleDescriptor desc,
                               SessionVariable sessionVariable) {
        this.icebergTable = icebergTable;
        Table nativeTable = icebergTable.getNativeTable();
        this.desc = desc;
        this.tableLocation = nativeTable.location();
        this.dataLocation = IcebergUtil.tableDataLocation(nativeTable);
        this.targetTableId = icebergTable.getId();
        this.tableIdentifier = icebergTable.getUUID();
        this.fileFormat = nativeTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toLowerCase();
        // Row-delta writes both new data files and new position-delete files, so each
        // needs its own codec.
        // Data files: write.parquet.compression-codec > session variable.
        // Delete files: write.delete.parquet.compression-codec > write.parquet.compression-codec > session variable.
        String sessionCodec = sessionVariable.getConnectorSinkCompressionCodec();
        String dataCodec = nativeTable.properties().getOrDefault(PARQUET_COMPRESSION, sessionCodec);
        this.dataCompressionType = dataCodec;
        this.deleteCompressionType = nativeTable.properties().getOrDefault(DELETE_PARQUET_COMPRESSION, dataCodec);
        this.targetMaxFileSize = IcebergUtil.resolveTargetMaxFileSize(nativeTable, sessionVariable);
    }

    public void init() {
        String catalogName = icebergTable.getCatalogName();
        this.cloudConfiguration = IcebergUtil.getVendedCloudConfiguration(catalogName, icebergTable);
        // Validate tuple descriptor contains required columns
        validateTuple(desc);
    }

    /**
     * Validate that the tuple descriptor contains required columns for row delta operations.
     * The tuple must include _file and _pos columns for identifying rows to delete,
     * plus data columns for the new rows.
     *
     * @param desc The tuple descriptor to validate
     */
    private void validateTuple(TupleDescriptor desc) {
        boolean hasFilePathColumn = false;
        boolean hasPosColumn = false;

        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() == null) {
                continue;
            }
            String colName = slot.getColumn().getName();
            if (IcebergTable.FILE_PATH.equals(colName)) {
                hasFilePathColumn = true;
                if (!slot.getType().isVarchar()) {
                    throw new StarRocksConnectorException("_file column must be type of VARCHAR");
                }
                continue;
            }
            if (IcebergTable.ROW_POSITION.equals(colName)) {
                hasPosColumn = true;
                if (!slot.getType().isBigint()) {
                    throw new StarRocksConnectorException("_pos column must be type of BIGINT");
                }
            }
        }

        if (!hasFilePathColumn || !hasPosColumn) {
            throw new StarRocksConnectorException(
                    "IcebergRowDeltaSink requires _file and _pos columns in tuple descriptor");
        }
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("ICEBERG ROW DELTA SINK");
        strBuilder.append("\n");
        strBuilder.append(prefix).append("  TABLE: ").append(tableIdentifier).append("\n");
        strBuilder.append(prefix).append("  LOCATION: ").append(tableLocation).append("\n");
        strBuilder.append(prefix).append("  WRITE MODE: ").append(WRITE_MODE_ROW_DELTA).append("\n");
        strBuilder.append(prefix).append("  TUPLE ID: ").append(desc.getId()).append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.ICEBERG_ROW_DELTA_SINK);
        TIcebergTableSink tIcebergTableSink = new TIcebergTableSink();
        tIcebergTableSink.setTarget_table_id(targetTableId);
        tIcebergTableSink.setTuple_id(desc.getId().asInt());
        tIcebergTableSink.setLocation(tableLocation);
        tIcebergTableSink.setData_location(dataLocation);
        tIcebergTableSink.setFile_format(fileFormat);
        tIcebergTableSink.setIs_static_partition_sink(false);
        tIcebergTableSink.setWrite_mode(TIcebergWriteMode.ROW_DELTA);
        tIcebergTableSink.setCompression_type(PARQUET_COMPRESSION_TYPE_MAP.get(dataCompressionType));
        tIcebergTableSink.setDelete_compression_type(PARQUET_COMPRESSION_TYPE_MAP.get(deleteCompressionType));
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

    /**
     * Sets the sink extra info for this row delta operation
     *
     * @param sinkExtraInfo The extra info to set
     */
    public void setSinkExtraInfo(com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra sinkExtraInfo) {
        this.sinkExtraInfo = sinkExtraInfo;
    }

    /**
     * Gets the sink extra info for this row delta operation
     *
     * @return The extra info, or null if not set
     */
    public com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra getSinkExtraInfo() {
        return sinkExtraInfo;
    }

    public boolean isUnpartitionedTable() {
        return !icebergTable.isPartitioned();
    }
}
