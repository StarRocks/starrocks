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


package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SinglePartitionDesc;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;

public class ListPartitionInfo extends PartitionInfo {

    private static final Logger LOG = LogManager.getLogger(ListPartitionInfo.class);

    @SerializedName("partitionColumns")
    @Deprecated // Use partitionColumnIds to get columns, this is reserved for rollback compatibility only.
    protected List<Column> deprecatedColumns = Lists.newArrayList();

    @SerializedName("colIds")
    protected List<ColumnId> partitionColumnIds = Lists.newArrayList();

    //serialize values for statement like `PARTITION p1 VALUES IN (("2022-04-01", "beijing"))`
    @SerializedName("idToMultiValues")
    private Map<Long, List<List<String>>> idToMultiValues;
    private Map<Long, List<List<LiteralExpr>>> idToMultiLiteralExprValues;
    //serialize values for statement like `PARTITION p1 VALUES IN ("beijing","chongqing")`
    @SerializedName("idToValues")
    private Map<Long, List<String>> idToValues;
    private Map<Long, List<LiteralExpr>> idToLiteralExprValues;
    @SerializedName("idToTemp")
    private Map<Long, Boolean> idToIsTempPartition;
    @SerializedName(value = "automaticPartition")
    private Boolean automaticPartition = false;

    public ListPartitionInfo(PartitionType partitionType,
                             List<Column> partitionColumns) {
        super(partitionType);
        this.deprecatedColumns = Objects.requireNonNull(partitionColumns, "partitionColumns is null");
        this.partitionColumnIds = partitionColumns.stream().map(Column::getColumnId).collect(Collectors.toList());
        this.setIsMultiColumnPartition();

        this.idToValues = new HashMap<>();
        this.idToLiteralExprValues = new HashMap<>();
        this.idToMultiValues = new HashMap<>();
        this.idToMultiLiteralExprValues = new HashMap<>();
        this.idToIsTempPartition = new HashMap<>();
    }

    public ListPartitionInfo() {
        super();
        this.idToValues = new HashMap<>();
        this.idToLiteralExprValues = new HashMap<>();
        this.idToMultiValues = new HashMap<>();
        this.idToMultiLiteralExprValues = new HashMap<>();
        this.deprecatedColumns = new ArrayList<>();
        this.partitionColumnIds = new ArrayList<>();
        this.idToIsTempPartition = new HashMap<>();
    }

    public static class ListPartitionValue implements Comparable<ListPartitionValue> {
        public LiteralExpr singleColumnValue;
        public List<LiteralExpr> multiColumnValues;

        public static ListPartitionValue none() {
            return null;
        }

        public static ListPartitionValue of(LiteralExpr single) {
            ListPartitionValue value = new ListPartitionValue();
            value.singleColumnValue = single;
            return value;
        }

        public static ListPartitionValue of(List<LiteralExpr> multi) {
            ListPartitionValue value = new ListPartitionValue();
            value.multiColumnValues = multi;
            return value;
        }

        public ConstantOperator toConstant() {
            Preconditions.checkState(multiColumnValues == null);
            return (ConstantOperator) SqlToScalarOperatorTranslator.translate(singleColumnValue);
        }

        @Override
        public int compareTo(@NotNull ListPartitionValue o) {
            if (singleColumnValue != null) {
                return singleColumnValue.compareTo(o.singleColumnValue);
            }
            if (multiColumnValues != null) {
                return compareColumns(multiColumnValues, o.multiColumnValues);
            }
            return 0;
        }
    }

    /**
     * Represent a partition cell, which can be single-column or multiple-columns
     */
    public static class ListPartitionCell {
        public static final ListPartitionCell EMPTY = new ListPartitionCell();

        private List<LiteralExpr> singleColumnValues;
        private List<List<LiteralExpr>> multiColumnValues;

        public static ListPartitionCell single(List<LiteralExpr> values) {
            ListPartitionCell res = new ListPartitionCell();
            res.singleColumnValues = values;
            return res;
        }

        public static ListPartitionCell multi(List<List<LiteralExpr>> multi) {
            ListPartitionCell res = new ListPartitionCell();
            res.multiColumnValues = multi;
            return res;
        }

        public boolean isEmpty() {
            return CollectionUtils.isEmpty(singleColumnValues) && CollectionUtils.isEmpty(multiColumnValues);
        }

        public ListPartitionValue minValue() {
            if (singleColumnValues != null) {
                return ListPartitionValue.of(singleColumnValues.stream().min(LiteralExpr::compareTo).get());
            }
            if (multiColumnValues != null) {
                return ListPartitionValue.of(multiColumnValues.stream().min(ListPartitionInfo::compareColumns).get());
            }
            return ListPartitionValue.none();
        }

        public ListPartitionValue maxValue() {
            if (singleColumnValues != null) {
                return ListPartitionValue.of(singleColumnValues.stream().max(LiteralExpr::compareTo).get());
            }
            if (multiColumnValues != null) {
                return ListPartitionValue.of(multiColumnValues.stream().max(ListPartitionInfo::compareColumns).get());
            }
            return ListPartitionValue.none();
        }
    }

    public ListPartitionCell getPartitionListExpr(long partitionId) {
        if (MapUtils.isNotEmpty(idToLiteralExprValues)) {
            return ListPartitionCell.single(idToLiteralExprValues.get(partitionId));
        } else if (MapUtils.isNotEmpty(idToMultiLiteralExprValues)) {
            return ListPartitionCell.multi(idToMultiLiteralExprValues.get(partitionId));
        } else {
            return ListPartitionCell.EMPTY;
        }
    }

    public void setValues(long partitionId, List<String> values) {
        this.idToValues.put(partitionId, values);
    }

    public void setIdToIsTempPartition(long partitionId, boolean isTemp) {
        this.idToIsTempPartition.put(partitionId, isTemp);
    }

    public void setLiteralExprValues(Map<ColumnId, Column> idToColumn, long partitionId, List<String> values)
            throws AnalysisException {
        List<LiteralExpr> partitionValues = new ArrayList<>(values.size());
        for (String value : values) {
            //there only one partition column for single partition list
            Type type = idToColumn.get(partitionColumnIds.get(0)).getType();
            LiteralExpr partitionValue = new PartitionValue(value).getValue(type);
            partitionValues.add(partitionValue);
        }
        this.idToLiteralExprValues.put(partitionId, partitionValues);
    }

    public void setDirectLiteralExprValues(long partitionId, List<LiteralExpr> values) {
        this.idToLiteralExprValues.put(partitionId, values);
    }

    public List<Long> getPartitionIds(boolean isTemp) {
        List<Long> partitionIds = Lists.newArrayList();
        idToIsTempPartition.forEach((k, v) -> {
            if (v.equals(isTemp)) {
                partitionIds.add(k);
            }
        });
        return partitionIds;
    }

    public void setBatchLiteralExprValues(Map<ColumnId, Column> idToColumn,
                                          Map<Long, List<String>> batchValues) throws AnalysisException {
        for (Map.Entry<Long, List<String>> entry : batchValues.entrySet()) {
            long partitionId = entry.getKey();
            List<String> values = entry.getValue();
            this.setLiteralExprValues(idToColumn, partitionId, values);
        }
    }

    public Map<Long, List<LiteralExpr>> getLiteralExprValues() {
        return this.idToLiteralExprValues;
    }

    /**
     * Return all unique values for specified partitionIds
     */
    public Set<LiteralExpr> getValuesSet(Set<Long> partitionIds) {
        if (MapUtils.isEmpty(idToLiteralExprValues)) {
            return Sets.newHashSet();
        }
        return idToLiteralExprValues
                .entrySet()
                .stream()
                .filter(x -> partitionIds.contains(x.getKey()))
                .flatMap(x -> x.getValue().stream())
                .collect(Collectors.toSet());
    }

    public void setMultiValues(long partitionId, List<List<String>> multiValues) {
        this.idToMultiValues.put(partitionId, multiValues);
    }

    @Override
    public boolean isAutomaticPartition() {
        return automaticPartition;
    }

    public void setAutomaticPartition(Boolean automaticPartition) {
        this.automaticPartition = automaticPartition;
    }

    public void setMultiLiteralExprValues(Map<ColumnId, Column> idToColumn, long partitionId,
                                          List<List<String>> multiValues) throws AnalysisException {
        List<List<LiteralExpr>> multiPartitionValues = new ArrayList<>(multiValues.size());
        List<Column> partitionColumns = MetaUtils.getColumnsByColumnIds(idToColumn, this.partitionColumnIds);
        for (List<String> values : multiValues) {
            List<LiteralExpr> partitionValues = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                String value = values.get(i);
                Type type = partitionColumns.get(i).getType();
                LiteralExpr partitionValue = new PartitionValue(value).getValue(type);
                partitionValues.add(partitionValue);
            }
            multiPartitionValues.add(partitionValues);
        }
        this.idToMultiLiteralExprValues.put(partitionId, multiPartitionValues);
    }

    public void setDirectMultiLiteralExprValues(long partitionId, List<List<LiteralExpr>> multiValues) {
        this.idToMultiLiteralExprValues.put(partitionId, multiValues);
    }

    public void setBatchMultiLiteralExprValues(Map<ColumnId, Column> idToColumn,
                                               Map<Long, List<List<String>>> batchMultiValues)
            throws AnalysisException {
        for (Map.Entry<Long, List<List<String>>> entry : batchMultiValues.entrySet()) {
            long partitionId = entry.getKey();
            List<List<String>> multiValues = entry.getValue();
            this.setMultiLiteralExprValues(idToColumn, partitionId, multiValues);
        }
    }

    public Map<Long, List<List<LiteralExpr>>> getMultiLiteralExprValues() {
        return this.idToMultiLiteralExprValues;
    }

    private void setIsMultiColumnPartition() {
        super.isMultiColumnPartition = this.partitionColumnIds.size() > 1;
    }

    public Map<Long, List<List<String>>> getIdToMultiValues() {
        return idToMultiValues;
    }

    public Map<Long, List<String>> getIdToValues() {
        return idToValues;
    }

    /**
     * If the list partition just has one value, we can prune it
     * otherwise we can not prune it because the partition has other values
     *
     * @param id
     * @return true if the partition can be pruned
     */
    public boolean pruneById(long id) {
        List<String> values = getIdToValues().get(id);
        if (values != null && values.size() == 1) {
            return true;
        }
        List<List<String>> multiValues = getIdToMultiValues().get(id);
        if (multiValues != null && multiValues.size() == 1) {
            return true;
        }
        return false;
    }

    public void updateLiteralExprValues(Map<ColumnId, Column> idToColumn) {
        try {
            Map<Long, List<String>> idToValuesMap = this.getIdToValues();
            for (Map.Entry<Long, List<String>> entry : idToValuesMap.entrySet()) {
                this.setLiteralExprValues(idToColumn, entry.getKey(), entry.getValue());
            }
            Map<Long, List<List<String>>> idToMultiValuesMap = this.getIdToMultiValues();
            for (Map.Entry<Long, List<List<String>>> entry : idToMultiValuesMap.entrySet()) {
                this.setMultiLiteralExprValues(idToColumn, entry.getKey(), entry.getValue());
            }
        } catch (AnalysisException e) {
            LOG.error("deserialize PartitionInfo error", e);
        }
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        String replicationNumStr = table.getTableProperty()
                .getProperties().get(PROPERTIES_REPLICATION_NUM);
        short tableReplicationNum = replicationNumStr == null ?
                RunMode.defaultReplicationNum() : Short.parseShort(replicationNumStr);

        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY ");
        if (!automaticPartition) {
            sb.append("LIST");
        }
        sb.append("(");
        sb.append(MetaUtils.getColumnsByColumnIds(table, partitionColumnIds).stream()
                .map(item -> "`" + item.getName() + "`")
                .collect(Collectors.joining(",")));
        sb.append(")");
        if (!automaticPartition) {
            List<Long> partitionIds = getPartitionIds(false);
            sb.append("(\n");
            if (!idToValues.isEmpty()) {
                sb.append(this.singleListPartitionSql(table, partitionIds, tableReplicationNum));
            }

            if (!idToMultiValues.isEmpty()) {
                sb.append(this.multiListPartitionSql(table, partitionIds, tableReplicationNum));
            }
            sb.append("\n)");
        }
        return sb.toString();
    }

    private String singleListPartitionSql(OlapTable table, List<Long> partitionIds, short tableReplicationNum) {
        StringBuilder sb = new StringBuilder();
        this.idToLiteralExprValues.forEach((partitionId, values) -> {
            if (partitionIds.contains(partitionId)) {
                Short partitionReplicaNum = table.getPartitionInfo().idToReplicationNum.get(partitionId);
                Optional.ofNullable(table.getPartition(partitionId)).ifPresent(partition -> {
                    String partitionName = partition.getName();
                    sb.append("  PARTITION ")
                            .append(partitionName)
                            .append(" VALUES IN ")
                            .append(this.valuesToString(values));

                    if (partitionReplicaNum != null && partitionReplicaNum != tableReplicationNum) {
                        sb.append(" (").append("\"" + PROPERTIES_REPLICATION_NUM + "\" = \"").append(partitionReplicaNum)
                                .append("\")");
                    }
                    sb.append(",\n");
                });
            }
        });
        return StringUtils.removeEnd(sb.toString(), ",\n");
    }

    private String valuesToString(List<LiteralExpr> values) {
        return "(" + values.stream().map(value -> "\'" + value.getStringValue() + "\'")
                .collect(Collectors.joining(", ")) + ")";
    }

    private String multiListPartitionSql(OlapTable table, List<Long> partitionIds, short tableReplicationNum) {
        StringBuilder sb = new StringBuilder();
        this.idToMultiLiteralExprValues.forEach((partitionId, multiValues) -> {
            if (partitionIds.contains(partitionId)) {
                Short partitionReplicaNum = table.getPartitionInfo().idToReplicationNum.get(partitionId);
                Optional.ofNullable(table.getPartition(partitionId)).ifPresent(partition -> {
                    String partitionName = partition.getName();
                    sb.append("  PARTITION ")
                            .append(partitionName)
                            .append(" VALUES IN ")
                            .append(this.multiValuesToString(multiValues));

                    if (partitionReplicaNum != null && partitionReplicaNum != tableReplicationNum) {
                        sb.append(" (").append("\"" + PROPERTIES_REPLICATION_NUM + "\" = \"").append(partitionReplicaNum)
                                .append("\")");
                    }

                    sb.append(",\n");
                });
            }
        });
        return StringUtils.removeEnd(sb.toString(), ",\n");
    }

    private String multiValuesToString(List<List<LiteralExpr>> multiValues) {
        return "(" + multiValues.stream()
                .map(values -> "(" + values.stream().map(value -> "\'" + value.getStringValue() + "\'")
                        .collect(Collectors.joining(", ")) + ")")
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public List<Column> getPartitionColumns(Map<ColumnId, Column> idToColumn) {
        return MetaUtils.getColumnsByColumnIds(idToColumn, partitionColumnIds);
    }

    @Override
    public int getPartitionColumnsSize() {
        return partitionColumnIds.size();
    }

    public String getValuesFormat(long partitionId) {
        if (!idToLiteralExprValues.isEmpty()) {
            List<LiteralExpr> literalExprs = idToLiteralExprValues.get(partitionId);
            if (literalExprs != null) {
                return this.valuesToString(literalExprs);
            }
        }
        if (!idToMultiLiteralExprValues.isEmpty()) {
            List<List<LiteralExpr>> lists = idToMultiLiteralExprValues.get(partitionId);
            if (lists != null) {
                return this.multiValuesToString(lists);
            }
        }
        return "";
    }

    public void handleNewListPartitionDescs(Map<ColumnId, Column> idToColumn,
                                            List<Pair<Partition, PartitionDesc>> partitionList,
                                            Set<String> existPartitionNameSet, boolean isTempPartition)
            throws DdlException {
        try {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                String name = partition.getName();
                if (!existPartitionNameSet.contains(name)) {
                    long partitionId = partition.getId();
                    PartitionDesc partitionDesc = entry.second;
                    Preconditions.checkArgument(partitionDesc instanceof SinglePartitionDesc);
                    this.idToDataProperty.put(partitionId, partitionDesc.getPartitionDataProperty());
                    this.idToReplicationNum.put(partitionId, partitionDesc.getReplicationNum());
                    this.idToInMemory.put(partitionId, partitionDesc.isInMemory());
                    if (partitionDesc instanceof MultiItemListPartitionDesc) {
                        MultiItemListPartitionDesc multiItemListPartitionDesc =
                                (MultiItemListPartitionDesc) partitionDesc;
                        this.idToMultiValues.put(partitionId, multiItemListPartitionDesc.getMultiValues());
                        this.setMultiLiteralExprValues(idToColumn, partitionId,
                                multiItemListPartitionDesc.getMultiValues());
                    } else if (partitionDesc instanceof SingleItemListPartitionDesc) {
                        SingleItemListPartitionDesc singleItemListPartitionDesc =
                                (SingleItemListPartitionDesc) partitionDesc;
                        this.idToValues.put(partitionId, singleItemListPartitionDesc.getValues());
                        this.setLiteralExprValues(idToColumn, partitionId, singleItemListPartitionDesc.getValues());
                    } else {
                        throw new DdlException(
                                "add list partition only support single item or multi item list partition now");
                    }
                    this.idToIsTempPartition.put(partitionId, isTempPartition);
                    this.idToStorageCacheInfo.put(partitionId, partitionDesc.getDataCacheInfo());
                }
            }
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }
    }

    public void unprotectHandleNewPartitionDesc(Map<ColumnId, Column> idToColumn,
                                                ListPartitionPersistInfo partitionPersistInfo)
            throws AnalysisException {
        Partition partition = partitionPersistInfo.getPartition();
        long partitionId = partition.getId();
        this.idToDataProperty.put(partitionId, partitionPersistInfo.getDataProperty());
        this.idToReplicationNum.put(partitionId, partitionPersistInfo.getReplicationNum());
        this.idToInMemory.put(partitionId, partitionPersistInfo.isInMemory());
        this.idToIsTempPartition.put(partitionId, partitionPersistInfo.isTempPartition());
        this.idToStorageCacheInfo.put(partitionId, partitionPersistInfo.getDataCacheInfo());

        List<List<String>> multiValues = partitionPersistInfo.getMultiValues();
        if (multiValues != null && multiValues.size() > 0) {
            this.idToMultiValues.put(partitionId, multiValues);
            this.setMultiLiteralExprValues(idToColumn, partitionId, multiValues);
        }

        List<String> values = partitionPersistInfo.getValues();
        if (values != null && values.size() > 0) {
            this.idToValues.put(partitionId, values);
            this.setLiteralExprValues(idToColumn, partitionId, values);
        }
    }

    @Override
    public void dropPartition(long partitionId) {
        super.dropPartition(partitionId);
        idToValues.remove(partitionId);
        idToLiteralExprValues.remove(partitionId);
        idToMultiValues.remove(partitionId);
        idToMultiLiteralExprValues.remove(partitionId);
        idToIsTempPartition.remove(partitionId);
        idToStorageCacheInfo.remove(partitionId);
    }

    @Override
    public void moveRangeFromTempToFormal(long tempPartitionId) {
        super.moveRangeFromTempToFormal(tempPartitionId);
        idToIsTempPartition.computeIfPresent(tempPartitionId, (k, v) -> false);
    }

    public void addPartition(Map<ColumnId, Column> idToColumn, long partitionId, DataProperty dataProperty,
                             short replicationNum, boolean isInMemory, DataCacheInfo dataCacheInfo, List<String> values,
                             List<List<String>> multiValues) throws AnalysisException {
        super.addPartition(partitionId, dataProperty, replicationNum, isInMemory, dataCacheInfo);
        if (multiValues != null && multiValues.size() > 0) {
            this.idToMultiValues.put(partitionId, multiValues);
            this.setMultiLiteralExprValues(idToColumn, partitionId, multiValues);
        }
        if (values != null && values.size() > 0) {
            this.idToValues.put(partitionId, values);
            this.setLiteralExprValues(idToColumn, partitionId, values);
        }
        this.idToStorageCacheInfo.put(partitionId, dataCacheInfo);
        idToIsTempPartition.put(partitionId, false);
    }

    @Override
    public void createAutomaticShadowPartition(List<Column> schema, long partitionId, String replicateNum) {
        if (isMultiColumnPartition()) {
            idToMultiValues.put(partitionId, Collections.emptyList());
            idToMultiLiteralExprValues.put(partitionId, Collections.emptyList());
        } else {
            idToValues.put(partitionId, Collections.emptyList());
            idToLiteralExprValues.put(partitionId, Collections.emptyList());
        }

        idToDataProperty.put(partitionId, new DataProperty(TStorageMedium.HDD));
        idToReplicationNum.put(partitionId, Short.valueOf(replicateNum));
        idToInMemory.put(partitionId, false);
        idToStorageCacheInfo.put(partitionId, new DataCacheInfo(true, false));
    }

    public static int compareByValue(List<List<String>> left, List<List<String>> right) {
        int valueSize = left.size();
        for (int i = 0; i < valueSize; i++) {
            int partitionSize = left.get(i).size();
            for (int j = 0; j < partitionSize; j++) {
                int compareResult = left.get(i).get(j).compareTo(right.get(i).get(j));
                if (compareResult != 0) {
                    return compareResult;
                }
            }
        }
        return 0;
    }

    public void setStorageCacheInfo(long partitionId, DataCacheInfo dataCacheInfo) {
        idToStorageCacheInfo.put(partitionId, dataCacheInfo);
    }

    @Override
    public List<Long> getSortedPartitions(boolean asc) {
        if (MapUtils.isNotEmpty(idToLiteralExprValues)) {
            return idToLiteralExprValues.entrySet().stream()
                    .filter(e -> !e.getValue().isEmpty())
                    .sorted((x, y) -> compareRow(x.getValue(), y.getValue(), asc))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        } else if (MapUtils.isEmpty(idToMultiLiteralExprValues)) {
            return idToMultiLiteralExprValues.entrySet().stream()
                    .filter(e -> !e.getValue().isEmpty())
                    .sorted((x, y) -> compareMultiValueList(x.getValue(), y.getValue(), asc))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        } else {
            throw new NotImplementedException("todo");
        }
    }

    /**
     * Compare based on the max/min value in the list
     */
    private static int compareRow(List<LiteralExpr> lhs, List<LiteralExpr> rhs, boolean asc) {
        ListPartitionValue lhsValue =
                asc ? ListPartitionCell.single(lhs).minValue() : ListPartitionCell.single(lhs).maxValue();
        ListPartitionValue rhsValue =
                asc ? ListPartitionCell.single(rhs).minValue() : ListPartitionCell.single(rhs).maxValue();
        return lhsValue.compareTo(rhsValue) * (asc ? 1 : -1);
    }

    private static int compareColumns(List<LiteralExpr> lhs, List<LiteralExpr> rhs) {
        assert lhs.size() == rhs.size();
        for (int i = 0; i < lhs.size(); i++) {
            int x = lhs.get(i).compareTo(rhs.get(i));
            if (x != 0) {
                return x;
            }
        }
        return 0;
    }

    private static int compareMultiValueList(List<List<LiteralExpr>> lhs, List<List<LiteralExpr>> rhs, boolean asc) {
        ListPartitionValue lhsValue =
                asc ? ListPartitionCell.multi(lhs).minValue() : ListPartitionCell.multi(lhs).maxValue();
        ListPartitionValue rhsValue =
                asc ? ListPartitionCell.multi(rhs).minValue() : ListPartitionCell.multi(rhs).maxValue();
        return lhsValue.compareTo(rhsValue) * (asc ? 1 : -1);
    }

    @Override
    public Object clone() {
        ListPartitionInfo info = (ListPartitionInfo) super.clone();
        info.deprecatedColumns = Lists.newArrayList(this.deprecatedColumns);
        info.idToMultiValues = Maps.newHashMap(this.idToMultiValues);
        info.idToMultiLiteralExprValues = Maps.newHashMap(this.idToMultiLiteralExprValues);
        info.idToValues = Maps.newHashMap(this.idToValues);
        info.idToLiteralExprValues = Maps.newHashMap(this.idToLiteralExprValues);
        info.idToIsTempPartition = Maps.newHashMap(this.idToIsTempPartition);
        info.automaticPartition = this.automaticPartition;
        return info;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        if (partitionColumnIds.size() <= 0) {
            partitionColumnIds = deprecatedColumns.stream().map(Column::getColumnId).collect(Collectors.toList());
        }
    }
}