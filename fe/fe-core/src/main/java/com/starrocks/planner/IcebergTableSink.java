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

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TExplainLevel;
import org.apache.iceberg.Table;

import java.util.Locale;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT;

public class IcebergTableSink extends DataSink {
    protected final TupleDescriptor desc;
    private final long targetTableId;
    private final String fileFormat;
    private final String location;
    private String compressionType;
    private final boolean isStatisticsPartitionSink;
    private final String tableIdentifier;

    public IcebergTableSink(IcebergTable icebergTable, TupleDescriptor desc, boolean isStatisticsPartitionSink) {
        Table nativeTable = icebergTable.getNativeTable();
        this.desc = desc;
        this.location = nativeTable.location();
        this.targetTableId = icebergTable.getId();
        this.tableIdentifier = icebergTable.getUUID();
        this.isStatisticsPartitionSink = isStatisticsPartitionSink;
        this.fileFormat = nativeTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toLowerCase();
        switch (fileFormat) {
            case "parquet":
                compressionType = nativeTable.properties().getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT)
                        .toLowerCase(Locale.ROOT);
            case "orc":
                compressionType = nativeTable.properties().getOrDefault(ORC_COMPRESSION, ORC_COMPRESSION_DEFAULT)
                        .toLowerCase(Locale.ROOT);
        }
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        return "";
    }

    @Override
    protected TDataSink toThrift() {
        return null;
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
