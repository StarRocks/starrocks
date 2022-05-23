// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/PartitionPersistInfo.java

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

package com.starrocks.persist;

import com.facebook.presto.hive.$internal.com.google.common.reflect.TypeToken;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.PartitionKeyDesc;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class PartitionPersistInfo implements Writable {
    private Long dbId;
    private Long tableId;
    private Partition partition;

    // value for range partition
    private Range<PartitionKey> range;
    // value for single item list partition
    private List<String> values;
    // value from multi item list partition
    private List<List<String>> multiValues;
    private DataProperty dataProperty;
    private short replicationNum;
    private boolean isInMemory = false;
    private boolean isTempPartition = false;


    public PartitionPersistInfo() {
    }

    public PartitionPersistInfo(long dbId, long tableId, Partition partition, Range<PartitionKey> range,
                                DataProperty dataProperty, short replicationNum,
                                boolean isInMemory, boolean isTempPartition ) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;

        this.range = range;
        this.dataProperty = dataProperty;

        this.replicationNum = replicationNum;
        this.isInMemory = isInMemory;
        this.isTempPartition = isTempPartition;

         this.values = new ArrayList<>();
         this.multiValues = new ArrayList<>();
    }

    public PartitionPersistInfo(long dbId, long tableId, Partition partition,
                                List<String> values, List<List<String>> multiValues,
                                DataProperty dataProperty, short replicationNum,
                                boolean isInMemory, boolean isTempPartition ) throws DdlException {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;

        this.range = this.findTempRange();
        this.dataProperty = dataProperty;

        this.replicationNum = replicationNum;
        this.isInMemory = isInMemory;
        this.isTempPartition = isTempPartition;

        this.values = values;
        this.multiValues = multiValues;
    }

    private Range<PartitionKey> findTempRange() throws DdlException {
        try{
            PartitionKey lower = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-1")),
                    Arrays.asList(new Column("tinyint", Type.TINYINT)));
            PartitionKey upper = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("1")),
                    Arrays.asList(new Column("tinyint", Type.TINYINT)));
            return Range.closedOpen(lower, upper);
        }catch (AnalysisException e){
            throw new DdlException(e.getMessage());
        }
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getTableId() {
        return tableId;
    }

    public Partition getPartition() {
        return partition;
    }

    public Range<PartitionKey> getRange() {
        return range;
    }

    public DataProperty getDataProperty() {
        return dataProperty;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        partition.write(out);

        RangeUtils.writeRange(out, range);
        dataProperty.write(out);
        out.writeShort(replicationNum);
        out.writeBoolean(isInMemory);
        out.writeBoolean(isTempPartition);

        String valuesJsonStr = GsonUtils.GSON.toJson(values);
        Text.writeString(out, valuesJsonStr);

        String multiValuesJsonStr = GsonUtils.GSON.toJson(multiValues);
        Text.writeString(out, multiValuesJsonStr);
    }

    public static PartitionPersistInfo read(DataInput in) throws IOException {
        PartitionPersistInfo info = new PartitionPersistInfo();
        info.readFields(in);
        return info;
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partition = Partition.read(in);

        range = RangeUtils.readRange(in);
        dataProperty = DataProperty.read(in);
        replicationNum = in.readShort();
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_72) {
            isInMemory = in.readBoolean();
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_74) {
            isTempPartition = in.readBoolean();
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_93) {
            String valuesJsonStr = Text.readString(in);
            if (valuesJsonStr != null){
                values = GsonUtils.GSON.fromJson(valuesJsonStr, ArrayList.class);
            }
            String multiValuesJsonStr = Text.readString(in);
            if (multiValuesJsonStr != null){
                java.lang.reflect.Type multiValueType = new TypeToken<List<ArrayList<String>>>() {}.getType();
                multiValues =  GsonUtils.GSON.fromJson(multiValuesJsonStr, multiValueType);
            }
        }

    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PartitionPersistInfo)) {
            return false;
        }

        PartitionPersistInfo info = (PartitionPersistInfo) obj;

        return dbId.equals(info.dbId)
                && tableId.equals(info.tableId)
                && partition.equals(info.partition);
    }
}
