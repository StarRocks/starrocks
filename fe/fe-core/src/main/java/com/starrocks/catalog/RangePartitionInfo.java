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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/RangePartitionInfo.java

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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangePartitionInfo extends PartitionInfo {
    private static final Logger LOG = LogManager.getLogger(RangePartitionInfo.class);

    @SerializedName(value = "partitionColumns")
    private List<Column> partitionColumns = Lists.newArrayList();
    // formal partition id -> partition range
    protected Map<Long, Range<PartitionKey>> idToRange = Maps.newHashMap();
    // temp partition id -> partition range
    private Map<Long, Range<PartitionKey>> idToTempRange = Maps.newHashMap();

    // partitionId -> serialized Range<PartitionKey>
    // because Range<PartitionKey> and PartitionKey can not be serialized by gson
    // ATTN: call preSerialize before serialization and postDeserialized after deserialization
    @SerializedName(value = "serializedIdToRange")
    private Map<Long, byte[]> serializedIdToRange;

    // partitionId -> serialized Range<PartitionKey>
    // because Range<PartitionKey> and PartitionKey can not be serialized by gson
    // ATTN: call preSerialize before serialization and postDeserialized after deserialization
    @SerializedName(value = "serializedIdToTempRange")
    private Map<Long, byte[]> serializedIdToTempRange;

    public RangePartitionInfo() {
        // for persist
        super();
    }

    public RangePartitionInfo(List<Column> partitionColumns) {
        super(PartitionType.RANGE);
        this.partitionColumns = partitionColumns;
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public RangePartitionInfo(RangePartitionInfo other) {
        super(other.type);
        this.partitionColumns = Lists.newArrayList(other.partitionColumns);
        this.idToRange = Maps.newHashMap(other.idToRange);
        this.idToTempRange = Maps.newHashMap(other.idToTempRange);
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public void dropPartition(long partitionId) {
        super.dropPartition(partitionId);
        idToRange.remove(partitionId);
        idToTempRange.remove(partitionId);
    }

    public void addPartition(long partitionId, boolean isTemp, Range<PartitionKey> range, DataProperty dataProperty,
                             short replicationNum, boolean isInMemory, DataCacheInfo dataCacheInfo) {
        addPartition(partitionId, dataProperty, replicationNum, isInMemory, dataCacheInfo);
        setRangeInternal(partitionId, isTemp, range);
    }

    public void addPartition(long partitionId, boolean isTemp, Range<PartitionKey> range, DataProperty dataProperty,
                             short replicationNum, boolean isInMemory) {
        this.addPartition(partitionId, isTemp, range, dataProperty, replicationNum, isInMemory, null);
    }

    public Range<PartitionKey> checkAndCreateRange(SingleRangePartitionDesc desc, boolean isTemp) throws DdlException {
        Range<PartitionKey> newRange = null;
        PartitionKeyDesc partitionKeyDesc = desc.getPartitionKeyDesc();
        // check range
        try {
            newRange = createAndCheckNewRange(partitionKeyDesc, isTemp);
        } catch (AnalysisException e) {
            throw new DdlException("Invalid range value format: " + e.getMessage());
        }

        Preconditions.checkNotNull(newRange);
        return newRange;
    }

    // create a new range and check it.
    private Range<PartitionKey> createAndCheckNewRange(PartitionKeyDesc partKeyDesc, boolean isTemp)
            throws AnalysisException, DdlException {
        Range<PartitionKey> newRange = null;
        // generate and sort the existing ranges
        List<Map.Entry<Long, Range<PartitionKey>>> sortedRanges = getSortedRangeMap(isTemp);

        // create upper values for new range
        PartitionKey newRangeUpper = null;
        if (partKeyDesc.isMax()) {
            newRangeUpper = PartitionKey.createInfinityPartitionKey(partitionColumns, true);
        } else {
            newRangeUpper = PartitionKey.createPartitionKey(partKeyDesc.getUpperValues(), partitionColumns);
        }
        if (newRangeUpper.isMinValue()) {
            throw new DdlException("Partition's upper value should not be MIN VALUE: " + partKeyDesc);
        }

        Range<PartitionKey> lastRange = null;
        Range<PartitionKey> currentRange = null;
        for (Map.Entry<Long, Range<PartitionKey>> entry : sortedRanges) {
            currentRange = entry.getValue();
            // check if equals to upper bound
            PartitionKey upperKey = currentRange.upperEndpoint();
            if (upperKey.compareTo(newRangeUpper) >= 0) {
                newRange = checkNewRange(partKeyDesc, newRangeUpper, lastRange, currentRange);
                break;
            } else {
                lastRange = currentRange;
            }
        } // end for ranges

        if (newRange == null) /* the new range's upper value is larger than any existing ranges */ {
            newRange = checkNewRange(partKeyDesc, newRangeUpper, lastRange, currentRange);
        }
        return newRange;
    }

    private Range<PartitionKey> checkNewRange(PartitionKeyDesc partKeyDesc, PartitionKey newRangeUpper,
                                              Range<PartitionKey> lastRange, Range<PartitionKey> currentRange)
            throws AnalysisException, DdlException {
        Range<PartitionKey> newRange;
        PartitionKey lowKey = null;
        if (partKeyDesc.hasLowerValues()) {
            lowKey = PartitionKey.createPartitionKey(partKeyDesc.getLowerValues(), partitionColumns);
        } else {
            if (lastRange == null) {
                lowKey = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
            } else {
                lowKey = lastRange.upperEndpoint();
            }
        }
        // check: [left, right), error if left equal right
        if (lowKey.compareTo(newRangeUpper) >= 0) {
            throw new AnalysisException("The lower values must smaller than upper values");
        }
        newRange = Range.closedOpen(lowKey, newRangeUpper);

        if (currentRange != null) {
            // check if range intersected
            RangeUtils.checkRangeIntersect(newRange, currentRange);
        }
        return newRange;
    }

    public Range<PartitionKey> handleNewSinglePartitionDesc(SingleRangePartitionDesc desc,
                                                            long partitionId, boolean isTemp) throws DdlException {
        Preconditions.checkArgument(desc.isAnalyzed());
        Range<PartitionKey> range;
        try {
            range = checkAndCreateRange(desc, isTemp);
            setRangeInternal(partitionId, isTemp, range);
        } catch (IllegalArgumentException e) {
            // Range.closedOpen may throw this if (lower > upper)
            throw new DdlException("Invalid key range: " + e.getMessage());
        }
        idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
        idToReplicationNum.put(partitionId, desc.getReplicationNum());
        idToInMemory.put(partitionId, desc.isInMemory());
        idToStorageCacheInfo.put(partitionId, desc.getDataCacheInfo());
        return range;
    }

    @Override
    public void createAutomaticShadowPartition(long partitionId, String replicateNum) throws DdlException {
        Range<PartitionKey> range = null;
        try {
            PartitionKey shadowPartitionKey = PartitionKey.createShadowPartitionKey(partitionColumns);
            range = Range.closedOpen(shadowPartitionKey, shadowPartitionKey);
            setRangeInternal(partitionId, false, range);
        } catch (IllegalArgumentException e) {
            // Range.closedOpen may throw this if (lower > upper)
            throw new DdlException("Invalid key range: " + e.getMessage());
        } catch (AnalysisException e) {
            throw new DdlException("Invalid key range: " + e.getMessage());
        }
        idToDataProperty.put(partitionId, new DataProperty(TStorageMedium.HDD));
        idToReplicationNum.put(partitionId, Short.valueOf(replicateNum));
        idToInMemory.put(partitionId, false);
        idToStorageCacheInfo.put(partitionId, new DataCacheInfo(true, false));
    }

    public void handleNewRangePartitionDescs(Map<Partition, PartitionDesc> partitionMap,
                                             Set<String> existPartitionNameSet,
                                             boolean isTemp) throws DdlException {
        for (Partition partition : partitionMap.keySet()) {
            if (!existPartitionNameSet.contains(partition.getName())) {
                long partitionId = partition.getId();
                SingleRangePartitionDesc desc = (SingleRangePartitionDesc) partitionMap.get(partition);
                Preconditions.checkArgument(desc.isAnalyzed());
                Range<PartitionKey> range;
                try {
                    range = checkAndCreateRange((SingleRangePartitionDesc) partitionMap.get(partition), isTemp);
                    setRangeInternal(partitionId, isTemp, range);
                } catch (IllegalArgumentException e) {
                    // Range.closedOpen may throw this if (lower > upper)
                    throw new DdlException("Invalid key range: " + e.getMessage());
                }
                idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
                idToReplicationNum.put(partitionId, desc.getReplicationNum());
                idToInMemory.put(partitionId, desc.isInMemory());
                idToStorageCacheInfo.put(partitionId, desc.getDataCacheInfo());
            }
        }
    }

    public void unprotectHandleNewSinglePartitionDesc(long partitionId, boolean isTemp, Range<PartitionKey> range,
                                                      DataProperty dataProperty, short replicationNum,
                                                      boolean isInMemory) {
        setRangeInternal(partitionId, isTemp, range);
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
        idToInMemory.put(partitionId, isInMemory);
    }

    /**
     * @param info
     * @TODO This method may be used in future
     */
    public void unprotectHandleNewSinglePartitionDesc(RangePartitionPersistInfo info) {
        Partition partition = info.getPartition();
        long partitionId = partition.getId();
        setRangeInternal(partitionId, info.isTempPartition(), info.getRange());
        idToDataProperty.put(partitionId, info.getDataProperty());
        idToReplicationNum.put(partitionId, info.getReplicationNum());
        idToInMemory.put(partitionId, info.isInMemory());
        idToStorageCacheInfo.put(partitionId, info.getDataCacheInfo());
    }

    public void setRange(long partitionId, boolean isTemp, Range<PartitionKey> range) {
        setRangeInternal(partitionId, isTemp, range);
    }

    public Map<Long, Range<PartitionKey>> getIdToRange(boolean isTemp) {
        if (isTemp) {
            return idToTempRange;
        } else {
            return idToRange;
        }
    }

    public Range<PartitionKey> getRange(long partitionId) {
        Range<PartitionKey> range = idToRange.get(partitionId);
        if (range == null) {
            range = idToTempRange.get(partitionId);
        }
        return range;
    }

    public static void checkRangeColumnType(Column column) throws AnalysisException {
        PrimitiveType type = column.getPrimitiveType();
        if (!type.isFixedPointType() && !type.isDateType()) {
            throw new AnalysisException("Column[" + column.getName() + "] type[" + type
                    + "] cannot be a range partition key.");
        }
    }

    public List<Map.Entry<Long, Range<PartitionKey>>> getSortedRangeMap(boolean isTemp) {
        Map<Long, Range<PartitionKey>> tmpMap = idToRange;
        if (isTemp) {
            tmpMap = idToTempRange;
        }
        List<Map.Entry<Long, Range<PartitionKey>>> sortedList = Lists.newArrayList(tmpMap.entrySet());
        Collections.sort(sortedList, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        return sortedList;
    }

    public List<Map.Entry<Long, Range<PartitionKey>>> getSortedRangeMap(Set<Long> partitionIds)
            throws AnalysisException {
        Map<Long, Range<PartitionKey>> tmpMap = Maps.newHashMap();
        for (long partitionId : partitionIds) {
            Range<PartitionKey> range = getRange(partitionId);
            if (range == null) {
                throw new AnalysisException("partition does not exist. id: " + partitionId);
            }
            tmpMap.put(partitionId, range);
        }
        List<Map.Entry<Long, Range<PartitionKey>>> sortedList = Lists.newArrayList(tmpMap.entrySet());
        Collections.sort(sortedList, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        return sortedList;
    }

    // get a sorted range list, exclude partitions which ids are in 'excludePartitionIds'
    public List<Range<PartitionKey>> getRangeList(Set<Long> excludePartitionIds, boolean isTemp) {
        Map<Long, Range<PartitionKey>> tmpMap = idToRange;
        if (isTemp) {
            tmpMap = idToTempRange;
        }
        List<Range<PartitionKey>> resultList = Lists.newArrayList();
        for (Map.Entry<Long, Range<PartitionKey>> entry : tmpMap.entrySet()) {
            if (!excludePartitionIds.contains(entry.getKey())) {
                resultList.add(entry.getValue());
            }
        }
        return resultList;
    }

    // return any range intersect with the newRange.
    // return null if no range intersect.
    public Range<PartitionKey> getAnyIntersectRange(Range<PartitionKey> newRange, boolean isTemp) {
        Map<Long, Range<PartitionKey>> tmpMap = idToRange;
        if (isTemp) {
            tmpMap = idToTempRange;
        }
        for (Range<PartitionKey> range : tmpMap.values()) {
            if (range.isConnected(newRange)) {
                Range<PartitionKey> intersection = range.intersection(newRange);
                if (!intersection.isEmpty()) {
                    return range;
                }
            }
        }
        return null;
    }

    private void setRangeInternal(long partitionId, boolean isTemp, Range<PartitionKey> range) {
        if (isTemp) {
            idToTempRange.put(partitionId, range);
        } else {
            idToRange.put(partitionId, range);
        }
    }

    public void moveRangeFromTempToFormal(long tempPartitionId) {
        Range<PartitionKey> range = idToTempRange.remove(tempPartitionId);
        if (range != null) {
            idToRange.put(tempPartitionId, range);
        }
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    byte[] serializeRange(Range<PartitionKey> range) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(stream);
        RangeUtils.writeRange(dos, range);
        return stream.toByteArray();
    }

    Range<PartitionKey> deserializeRange(byte[] serializedRange) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(serializedRange);
        DataInput dataInput = new DataInputStream(inputStream);
        Range<PartitionKey> range = RangeUtils.readRange(dataInput);
        return range;
    }

    @Override
    public void gsonPreProcess() throws IOException {
        super.gsonPreProcess();
        serializedIdToRange = Maps.newConcurrentMap();
        for (Map.Entry<Long, Range<PartitionKey>> entry : idToRange.entrySet()) {
            byte[] serializedRange = serializeRange(entry.getValue());
            serializedIdToRange.put(entry.getKey(), serializedRange);
        }

        serializedIdToTempRange = Maps.newConcurrentMap();
        for (Map.Entry<Long, Range<PartitionKey>> entry : idToTempRange.entrySet()) {
            byte[] serializedRange = serializeRange(entry.getValue());
            serializedIdToTempRange.put(entry.getKey(), serializedRange);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        idToRange = Maps.newConcurrentMap();
        if (serializedIdToRange != null && !serializedIdToRange.isEmpty()) {
            for (Map.Entry<Long, byte[]> entry : serializedIdToRange.entrySet()) {
                idToRange.put(entry.getKey(), deserializeRange(entry.getValue()));
            }
            serializedIdToRange = null;
        }
        idToTempRange = Maps.newConcurrentMap();
        if (serializedIdToTempRange != null && !serializedIdToTempRange.isEmpty()) {
            for (Map.Entry<Long, byte[]> entry : serializedIdToTempRange.entrySet()) {
                idToTempRange.put(entry.getKey(), deserializeRange(entry.getValue()));
            }
            serializedIdToTempRange = null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // partition columns
        out.writeInt(partitionColumns.size());
        for (Column column : partitionColumns) {
            column.write(out);
        }

        out.writeInt(idToRange.size());
        for (Map.Entry<Long, Range<PartitionKey>> entry : idToRange.entrySet()) {
            out.writeLong(entry.getKey());
            RangeUtils.writeRange(out, entry.getValue());
        }

        out.writeInt(idToTempRange.size());
        for (Map.Entry<Long, Range<PartitionKey>> entry : idToTempRange.entrySet()) {
            out.writeLong(entry.getKey());
            RangeUtils.writeRange(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            Column column = Column.read(in);
            partitionColumns.add(column);
        }

        this.isMultiColumnPartition = partitionColumns.size() > 1;

        counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            Range<PartitionKey> range = RangeUtils.readRange(in);
            idToRange.put(partitionId, range);
        }

        counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            Range<PartitionKey> range = RangeUtils.readRange(in);
            idToTempRange.put(partitionId, range);
        }
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
        for (Column column : partitionColumns) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column.getName()).append("`");
            idx++;
        }
        sb.append(")\n(");

        // sort range
        List<Map.Entry<Long, Range<PartitionKey>>> entries =
                new ArrayList<Map.Entry<Long, Range<PartitionKey>>>(this.idToRange.entrySet());
        Collections.sort(entries, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);

        idx = 0;
        PartitionInfo tblPartitionInfo = table.getPartitionInfo();

        String replicationNumStr = table.getTableProperty().getProperties().get("replication_num");
        short replicationNum;
        if (replicationNumStr == null) {
            replicationNum = RunMode.defaultReplicationNum();
        } else {
            replicationNum = Short.parseShort(replicationNumStr);
        }

        for (Map.Entry<Long, Range<PartitionKey>> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();
            Range<PartitionKey> range = entry.getValue();

            // print all partitions' range is fixed range, even if some of them is created by less than range
            sb.append("PARTITION ").append(partitionName).append(" VALUES [");
            sb.append(range.lowerEndpoint().toSql());
            sb.append(", ").append(range.upperEndpoint().toSql()).append(")");

            if (partitionId != null) {
                partitionId.add(entry.getKey());
                break;
            }
            short curPartitionReplicationNum = tblPartitionInfo.getReplicationNum(entry.getKey());
            if (curPartitionReplicationNum != replicationNum) {
                sb.append("(").append("\"replication_num\" = \"").append(curPartitionReplicationNum).append("\")");
            }
            if (idx != entries.size() - 1) {
                sb.append(",\n");
            }
            idx++;
        }
        sb.append(")");
        return sb.toString();
    }

    public boolean isPartitionedBy(PrimitiveType type) {
        return partitionColumns.size() == 1 && partitionColumns.get(0).getType().getPrimitiveType() == type;
    }
}

