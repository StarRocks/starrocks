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
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.Connector;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.paimon.PaimonSinkExtra;
import com.starrocks.connector.paimon.PaimonWriteCapabilities;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPaimonTableSink;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Objects;

/** A native Paimon writer. Publication uses the same frozen contract as the backend writer. */
public class PaimonTableSink extends DataSink {
    private final TupleDescriptor desc;
    private final long targetTableId;
    private final String tableName;
    private final PaimonSinkExtra sinkExtra;
    private final TCloudConfiguration cloudConfiguration;
    private int routingBucketSlotId = -1;

    public PaimonTableSink(PaimonTable table, TupleDescriptor desc, PaimonSinkExtra sinkExtra) {
        PaimonWriteCapabilities.validateWrite(table.getNativeTable());
        FileStoreTable nativeTable = (FileStoreTable) table.getNativeTable();
        this.desc = Objects.requireNonNull(desc, "desc");
        this.sinkExtra = Objects.requireNonNull(sinkExtra, "sinkExtra");
        if (!table.getNativeWriteIdentity().equals(sinkExtra.getTableIdentity())
                || nativeTable.schema().id() != sinkExtra.getSchemaId()
                || !PaimonSinkExtra.canonicalSchemaJson(nativeTable).equals(sinkExtra.getSchemaJson())
                || !nativeTable.location().toString().equals(sinkExtra.getLocation())) {
            throw new StarRocksConnectorException("Paimon target identity or schema changed during planning");
        }
        this.targetTableId = table.getId();
        this.tableName = table.getCatalogName() + "." + table.getCatalogDBName() + "." + table.getCatalogTableName();

        Connector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(table.getCatalogName());
        Preconditions.checkState(connector != null, "Paimon sink catalog connector is unavailable");
        CloudConfiguration configuration = connector.getMetadata().getCloudConfiguration();
        Preconditions.checkState(configuration != null, "Paimon sink cloud configuration is unavailable");
        TCloudConfiguration thriftConfiguration = new TCloudConfiguration();
        configuration.toThrift(thriftConfiguration);
        this.cloudConfiguration = thriftConfiguration.deepCopy();
    }

    public PaimonSinkExtra getPaimonSinkExtra() {
        return sinkExtra;
    }

    public TupleDescriptor getTupleDescriptor() {
        return desc;
    }

    public void setBucketRouting(int slotId) {
        Preconditions.checkArgument(slotId >= 0, "Paimon routing bucket slot must be materialized");
        Preconditions.checkState(sinkExtra.getBucketCount() > 0, "Paimon unaware-bucket writes do not use fixed routing");
        routingBucketSlotId = slotId;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        // Never print native options, schema JSON or cloud credentials into EXPLAIN/query detail.
        return prefix + "Paimon TABLE SINK\n"
                + prefix + "  TABLE: " + tableName + "\n"
                + prefix + "  TUPLE ID: " + desc.getId() + "\n"
                + prefix + "  OVERWRITE MODE: " + sinkExtra.getOverwriteMode() + "\n"
                + prefix + "  SCHEMA ID: " + sinkExtra.getSchemaId() + "\n"
                + prefix + "  BUCKET ROUTING: " + (routingBucketSlotId >= 0 ? "NATIVE" : "UNAWARE") + "\n";
    }

    public TPaimonTableSink getThriftTableSink() {
        TPaimonTableSink sink = new TPaimonTableSink();
        sink.setTarget_table_id(targetTableId);
        sink.setTuple_id(desc.getId().asInt());
        sink.setLocation(sinkExtra.getLocation());
        sink.setTable_identity(sinkExtra.getTableIdentity());
        sink.setSchema_id(sinkExtra.getSchemaId());
        sink.setSchema_json(sinkExtra.getSchemaJson());
        sink.setColumn_names(new ArrayList<>(sinkExtra.getColumnNames()));
        sink.setPartition_columns(new ArrayList<>(sinkExtra.getPartitionColumns()));
        sink.setBucket_columns(new ArrayList<>(sinkExtra.getBucketColumns()));
        sink.setPrimary_key_columns(new ArrayList<>(sinkExtra.getPrimaryKeyColumns()));
        sink.setBucket_count(sinkExtra.getBucketCount());
        sink.setOptions(new LinkedHashMap<>(sinkExtra.getOptions()));
        sink.setCommit_user(sinkExtra.getCommitUser());
        sink.setCommit_identifier(sinkExtra.getCommitIdentifier());
        sink.setOperation_id(sinkExtra.getOperationId());
        sink.setOverwrite_mode(sinkExtra.getOverwriteMode());
        sink.setStatic_partition(new LinkedHashMap<>(sinkExtra.getStaticPartition()));
        sink.setCloud_configuration(cloudConfiguration.deepCopy());
        sink.setTime_zone(sinkExtra.getTimeZone());
        sink.setWriter_protocol_version(PaimonSinkExtra.WRITER_PROTOCOL_VERSION);
        if (routingBucketSlotId >= 0) {
            sink.setRouting_bucket_slot_id(routingBucketSlotId);
            sink.setBucket_routed(true);
        }
        return sink;
    }

    @Override
    protected TDataSink toThrift() {
        Preconditions.checkState(sinkExtra.getBucketCount() <= 0 || routingBucketSlotId >= 0,
                "Fixed-bucket Paimon writes require native partition/bucket-owner routing");
        TDataSink dataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        dataSink.setPaimon_table_sink(getThriftTableSink());
        return dataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
