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
<<<<<<< HEAD
=======
import com.starrocks.analysis.Expr;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
<<<<<<< HEAD
=======
import com.starrocks.sql.common.MetaUtils;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
<<<<<<< HEAD
import java.io.DataOutput;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
=======
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

public class RangePartitionInfo extends PartitionInfo {
    private static final Logger LOG = LogManager.getLogger(RangePartitionInfo.class);

    @SerializedName(value = "partitionColumns")
<<<<<<< HEAD
    protected List<Column> partitionColumns = Lists.newArrayList();
=======
    @Deprecated // Use partitionColumnIds to get columns, this is reserved for rollback compatibility only.
    protected List<Column> deprecatedColumns = Lists.newArrayList();

    @SerializedName("colIds")
    protected List<ColumnId> partitionColumnIds = Lists.newArrayList();

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    // formal partition id -> partition range
    protected Map<Long, Range<PartitionKey>> idToRange = Maps.newConcurrentMap();
    // temp partition id -> partition range
    private Map<Long, Range<PartitionKey>> idToTempRange = Maps.newConcurrentMap();

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
<<<<<<< HEAD
        this.partitionColumns = partitionColumns;
=======
        this.deprecatedColumns = Objects.requireNonNull(partitionColumns, "partitionColumns is null");
        this.partitionColumnIds = partitionColumns.stream().map(Column::getColumnId).collect(Collectors.toList());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public RangePartitionInfo(RangePartitionInfo other) {
        super(other.type);
<<<<<<< HEAD
        this.partitionColumns = Lists.newArrayList(other.partitionColumns);
        this.idToRange.putAll(other.idToRange);
        this.idToTempRange.putAll(other.idToTempRange);
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partitionColumns;
=======
        this.deprecatedColumns = Lists.newArrayList(other.deprecatedColumns);
        this.partitionColumnIds = deprecatedColumns.stream().map(Column::getColumnId).collect(Collectors.toList());
        this.idToRange.putAll(other.idToRange);
        this.idToTempRange.putAll(other.idToTempRange);
        this.isMultiColumnPartition = deprecatedColumns.size() > 1;
    }

    @Override
    public List<Column> getPartitionColumns(Map<ColumnId, Column> idToColumn) {
        return MetaUtils.getColumnsByColumnIds(idToColumn, partitionColumnIds);
    }

    @Override
    public int getPartitionColumnsSize() {
        return partitionColumnIds.size();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    public Range<PartitionKey> checkAndCreateRange(SingleRangePartitionDesc desc, boolean isTemp) throws DdlException {
=======
    public Range<PartitionKey> checkAndCreateRange(Map<ColumnId, Column> schema, SingleRangePartitionDesc desc, boolean isTemp)
            throws DdlException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        Range<PartitionKey> newRange = null;
        PartitionKeyDesc partitionKeyDesc = desc.getPartitionKeyDesc();
        // check range
        try {
<<<<<<< HEAD
            newRange = createAndCheckNewRange(partitionKeyDesc, isTemp);
=======
            newRange = createAndCheckNewRange(schema, partitionKeyDesc, isTemp);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } catch (AnalysisException e) {
            throw new DdlException("Invalid range value format: " + e.getMessage());
        }

        Preconditions.checkNotNull(newRange);
        return newRange;
    }

    // create a new range and check it.
<<<<<<< HEAD
    private Range<PartitionKey> createAndCheckNewRange(PartitionKeyDesc partKeyDesc, boolean isTemp)
=======
    private Range<PartitionKey> createAndCheckNewRange(Map<ColumnId, Column> schema, PartitionKeyDesc partKeyDesc, boolean isTemp)
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            throws AnalysisException, DdlException {
        Range<PartitionKey> newRange = null;
        // generate and sort the existing ranges
        List<Map.Entry<Long, Range<PartitionKey>>> sortedRanges = getSortedRangeMap(isTemp);

<<<<<<< HEAD
=======
        List<Column> partitionColumns = getPartitionColumns(schema);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                newRange = checkNewRange(partKeyDesc, newRangeUpper, lastRange, currentRange);
=======
                newRange = checkNewRange(partitionColumns, partKeyDesc, newRangeUpper, lastRange, currentRange);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                break;
            } else {
                lastRange = currentRange;
            }
        } // end for ranges

        if (newRange == null) /* the new range's upper value is larger than any existing ranges */ {
<<<<<<< HEAD
            newRange = checkNewRange(partKeyDesc, newRangeUpper, lastRange, currentRange);
=======
            newRange = checkNewRange(partitionColumns, partKeyDesc, newRangeUpper, lastRange, currentRange);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return newRange;
    }

<<<<<<< HEAD
    private Range<PartitionKey> checkNewRange(PartitionKeyDesc partKeyDesc, PartitionKey newRangeUpper,
                                              Range<PartitionKey> lastRange, Range<PartitionKey> currentRange)
=======
    private Range<PartitionKey> checkNewRange(List<Column> partitionColumns, PartitionKeyDesc partKeyDesc,
                                              PartitionKey newRangeUpper, Range<PartitionKey> lastRange,
                                              Range<PartitionKey> currentRange)
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    public Range<PartitionKey> handleNewSinglePartitionDesc(SingleRangePartitionDesc desc,
                                                            long partitionId, boolean isTemp) throws DdlException {
        Preconditions.checkArgument(desc.isAnalyzed());
        Range<PartitionKey> range;
        try {
            range = checkAndCreateRange(desc, isTemp);
=======
    public Range<PartitionKey> handleNewSinglePartitionDesc(Map<ColumnId, Column> schema, SingleRangePartitionDesc desc,
                                                            long partitionId, boolean isTemp) throws DdlException {
        Range<PartitionKey> range;
        try {
            range = checkAndCreateRange(schema, desc, isTemp);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            setRangeInternal(partitionId, isTemp, range);
        } catch (IllegalArgumentException e) {
            // Range.closedOpen may throw this if (lower > upper)
            throw new DdlException("Invalid key range: " + e.getMessage());
        }
<<<<<<< HEAD
        idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
        idToReplicationNum.put(partitionId, desc.getReplicationNum());
        idToInMemory.put(partitionId, desc.isInMemory());
        idToStorageCacheInfo.put(partitionId, desc.getDataCacheInfo());
=======
        super.addPartition(partitionId, desc.getPartitionDataProperty(), desc.getReplicationNum(), desc.isInMemory(),
                desc.getDataCacheInfo());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return range;
    }

    @Override
<<<<<<< HEAD
    public void createAutomaticShadowPartition(long partitionId, String replicateNum) throws DdlException {
        Range<PartitionKey> range = null;
        try {
            PartitionKey shadowPartitionKey = PartitionKey.createShadowPartitionKey(partitionColumns);
=======
    public void createAutomaticShadowPartition(List<Column> schema, long partitionId, String replicateNum) throws DdlException {
        Range<PartitionKey> range = null;
        try {
            PartitionKey shadowPartitionKey = PartitionKey.createShadowPartitionKey(
                    getPartitionColumns(MetaUtils.buildIdToColumn(schema)));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            range = Range.closedOpen(shadowPartitionKey, shadowPartitionKey);
            setRangeInternal(partitionId, false, range);
        } catch (IllegalArgumentException e) {
            // Range.closedOpen may throw this if (lower > upper)
            throw new DdlException("Invalid key range: " + e.getMessage());
        } catch (AnalysisException e) {
            throw new DdlException("Invalid key range: " + e.getMessage());
        }
<<<<<<< HEAD
        idToDataProperty.put(partitionId, new DataProperty(TStorageMedium.HDD));
        idToReplicationNum.put(partitionId, Short.valueOf(replicateNum));
        idToInMemory.put(partitionId, false);
        idToStorageCacheInfo.put(partitionId, new DataCacheInfo(true, false));
    }

    public void handleNewRangePartitionDescs(List<Pair<Partition, PartitionDesc>> partitionList,
                                             Set<String> existPartitionNameSet,
                                             boolean isTemp) throws DdlException {
        for (Pair<Partition, PartitionDesc> entry : partitionList) {
            Partition partition = entry.first;
            if (!existPartitionNameSet.contains(partition.getName())) {
                long partitionId = partition.getId();
                SingleRangePartitionDesc desc = (SingleRangePartitionDesc) entry.second;
                Preconditions.checkArgument(desc.isAnalyzed());
                Range<PartitionKey> range;
                try {
                    range = checkAndCreateRange((SingleRangePartitionDesc) entry.second, isTemp);
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
=======
        super.addPartition(partitionId, new DataProperty(TStorageMedium.HDD), Short.valueOf(replicateNum), false,
                new DataCacheInfo(true, false));
    }

    public void handleNewRangePartitionDescs(Map<ColumnId, Column> schema,
                                             List<Pair<Partition, PartitionDesc>> partitionList,
                                             Set<String> existPartitionNameSet,
                                             boolean isTemp) throws DdlException {
        try {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                if (!existPartitionNameSet.contains(partition.getName())) {
                    long partitionId = partition.getId();
                    SingleRangePartitionDesc desc = (SingleRangePartitionDesc) entry.second;
                    Range<PartitionKey> range;
                    try {
                        range = checkAndCreateRange(schema, (SingleRangePartitionDesc) entry.second, isTemp);
                        setRangeInternal(partitionId, isTemp, range);
                    } catch (IllegalArgumentException e) {
                        // Range.closedOpen may throw this if (lower > upper)
                        throw new DdlException("Invalid key range: " + e.getMessage());
                    }
                    super.addPartition(partitionId, desc.getPartitionDataProperty(), desc.getReplicationNum(),
                            desc.isInMemory(), desc.getDataCacheInfo());
                }
            }
        } catch (Exception e) {
            // cleanup
            partitionList.forEach(entry -> {
                long partitionId = entry.first.getId();
                removeRangeInternal(partitionId, isTemp);
                super.dropPartition(partitionId);
            });
            throw e;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    public void unprotectHandleNewSinglePartitionDesc(long partitionId, boolean isTemp, Range<PartitionKey> range,
                                                      DataProperty dataProperty, short replicationNum,
                                                      boolean isInMemory) {
        setRangeInternal(partitionId, isTemp, range);
<<<<<<< HEAD
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
        idToInMemory.put(partitionId, isInMemory);
=======
        super.addPartition(partitionId, dataProperty, replicationNum, isInMemory);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    /**
     * @param info
     * @TODO This method may be used in future
     */
    public void unprotectHandleNewSinglePartitionDesc(RangePartitionPersistInfo info) {
        Partition partition = info.getPartition();
        long partitionId = partition.getId();
        setRangeInternal(partitionId, info.isTempPartition(), info.getRange());
<<<<<<< HEAD
        idToDataProperty.put(partitionId, info.getDataProperty());
        idToReplicationNum.put(partitionId, info.getReplicationNum());
        idToInMemory.put(partitionId, info.isInMemory());
        idToStorageCacheInfo.put(partitionId, info.getDataCacheInfo());
=======
        super.addPartition(partitionId, info.getDataProperty(), info.getReplicationNum(), info.isInMemory(),
                info.getDataCacheInfo());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
=======
    public static void checkExpressionRangeColumnType(Column column, Expr expr) throws AnalysisException {
        PrimitiveType type = column.getPrimitiveType();
        if (!type.isFixedPointType() && !type.isDateType()) {
            throw new AnalysisException("Expr[" + expr.toSql() + "] type[" + type
                    + "] cannot be a range partition key.");
        }
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
=======
    @Override
    public List<Long> getSortedPartitions(boolean asc) {
        Map<Long, Range<PartitionKey>> tmpMap = idToRange;
        List<Map.Entry<Long, Range<PartitionKey>>> sortedList = Lists.newArrayList(tmpMap.entrySet());
        sortedList.sort(asc ? RangeUtils.RANGE_MAP_ENTRY_COMPARATOR : RangeUtils.RANGE_MAP_ENTRY_COMPARATOR.reversed());
        if (sortedList.isEmpty()) {
            return Lists.newArrayList();
        }
        return sortedList.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    /**
     * For RangePartition, NULL value would be put in the MIN_VALUE partition but not a real NULL.
     * It's a little bit tricky, as that partition might contain NULL, or might not.
     */
    @Override
    public Set<Long> getNullValuePartitions() {
        return idToRange.entrySet().stream()
                .filter(x -> x.getValue().lowerEndpoint().isMinValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
=======
    private void removeRangeInternal(long partitionId, boolean isTemp) {
        if (isTemp) {
            idToTempRange.remove(partitionId);
        } else {
            idToRange.remove(partitionId);
        }
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public void moveRangeFromTempToFormal(long tempPartitionId) {
        Range<PartitionKey> range = idToTempRange.remove(tempPartitionId);
        if (range != null) {
            idToRange.put(tempPartitionId, range);
        }
    }

<<<<<<< HEAD
    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
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
=======
        if (partitionColumnIds == null || partitionColumnIds.size() <= 0) {
            partitionColumnIds = deprecatedColumns.stream().map(Column::getColumnId).collect(Collectors.toList());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
<<<<<<< HEAD
        for (Column column : partitionColumns) {
=======
        for (Column column : getPartitionColumns(table.getIdToColumn())) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    public boolean isPartitionedBy(PrimitiveType type) {
        return partitionColumns.size() == 1 && partitionColumns.get(0).getType().getPrimitiveType() == type;
=======
    public boolean isPartitionedBy(Table table, PrimitiveType type) {
        if (partitionColumnIds.size() != 1) {
            return false;
        }
        Column column = getPartitionColumns(table.getIdToColumn()).get(0);
        return column != null && column.getType().getPrimitiveType() == type;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    protected Object clone() {
        RangePartitionInfo info = (RangePartitionInfo) super.clone();
<<<<<<< HEAD
        info.partitionColumns = Lists.newArrayList(this.partitionColumns);
        info.idToRange = new ConcurrentHashMap<>(this.idToRange);
        info.idToTempRange = new ConcurrentHashMap<>(this.idToTempRange);
        info.isMultiColumnPartition = partitionColumns.size() > 1;
=======
        info.deprecatedColumns = Lists.newArrayList(this.deprecatedColumns);
        info.partitionColumnIds = Lists.newArrayList(this.partitionColumnIds);
        info.idToRange = new ConcurrentHashMap<>(this.idToRange);
        info.idToTempRange = new ConcurrentHashMap<>(this.idToTempRange);
        info.isMultiColumnPartition = partitionColumnIds.size() > 1;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return info;
    }

    @Override
    public void setPartitionIdsForRestore(Map<Long, Long> partitionOldIdToNewId) {
        super.setPartitionIdsForRestore(partitionOldIdToNewId);

        Map<Long, Range<PartitionKey>> oldIdToRange = this.idToRange;
        Map<Long, Range<PartitionKey>> oldIdToTempRange = this.idToTempRange;

        this.idToRange = new ConcurrentHashMap<>();
        this.idToTempRange = new ConcurrentHashMap<>();

        for (Map.Entry<Long, Long> entry : partitionOldIdToNewId.entrySet()) {
            Long oldId = entry.getKey();
            Long newId = entry.getValue();

            Range<PartitionKey> range = oldIdToRange.get(oldId);
            if (range != null) {
                this.idToRange.put(newId, range);
            }
            Range<PartitionKey> tempRange = oldIdToTempRange.get(oldId);
            if (tempRange != null) {
                this.idToTempRange.put(newId, tempRange);
            }
        }
    }
}
<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
