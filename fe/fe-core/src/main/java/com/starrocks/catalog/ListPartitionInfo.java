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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;

public class ListPartitionInfo extends PartitionInfo {

    private static final Logger LOG = LogManager.getLogger(ListPartitionInfo.class);

    @SerializedName("partitionColumns")
    private List<Column> partitionColumns;
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
    @SerializedName("automaticPartition")
    private Boolean automaticPartition = false;

    public ListPartitionInfo(PartitionType partitionType,
                             List<Column> partitionColumns) {
        super(partitionType);
        this.partitionColumns = partitionColumns;
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
        this.partitionColumns = new ArrayList<>();
        this.idToIsTempPartition = new HashMap<>();
    }

    public void setValues(long partitionId, List<String> values) {
        this.idToValues.put(partitionId, values);
    }

    public void setIdToIsTempPartition(long partitionId, boolean isTemp) {
        this.idToIsTempPartition.put(partitionId, isTemp);
    }

    public void setLiteralExprValues(long partitionId, List<String> values) throws AnalysisException {
        List<LiteralExpr> partitionValues = new ArrayList<>(values.size());
        for (String value : values) {
            //there only one partition column for single partition list
            Type type = this.partitionColumns.get(0).getType();
            LiteralExpr partitionValue = new PartitionValue(value).getValue(type);
            partitionValues.add(partitionValue);
        }
        this.idToLiteralExprValues.put(partitionId, partitionValues);
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

    public void setBatchLiteralExprValues(Map<Long, List<String>> batchValues) throws AnalysisException {
        for (Map.Entry<Long, List<String>> entry : batchValues.entrySet()) {
            long partitionId = entry.getKey();
            List<String> values = entry.getValue();
            this.setLiteralExprValues(partitionId, values);
        }
    }

    public Map<Long, List<LiteralExpr>> getLiteralExprValues() {
        return this.idToLiteralExprValues;
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

    public void setMultiLiteralExprValues(long partitionId, List<List<String>> multiValues) throws AnalysisException {
        List<List<LiteralExpr>> multiPartitionValues = new ArrayList<>(multiValues.size());
        for (List<String> values : multiValues) {
            List<LiteralExpr> partitionValues = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                String value = values.get(i);
                Type type = this.partitionColumns.get(i).getType();
                LiteralExpr partitionValue = new PartitionValue(value).getValue(type);
                partitionValues.add(partitionValue);
            }
            multiPartitionValues.add(partitionValues);
        }
        this.idToMultiLiteralExprValues.put(partitionId, multiPartitionValues);
    }

    public void setDirectLiteralExprValues(long partitionId, List<LiteralExpr> values) {
        this.idToLiteralExprValues.put(partitionId, values);
    }

    public void setBatchMultiLiteralExprValues(Map<Long, List<List<String>>> batchMultiValues)
            throws AnalysisException {
        for (Map.Entry<Long, List<List<String>>> entry : batchMultiValues.entrySet()) {
            long partitionId = entry.getKey();
            List<List<String>> multiValues = entry.getValue();
            this.setMultiLiteralExprValues(partitionId, multiValues);
        }
    }

    public void setDirectMultiLiteralExprValues(long partitionId, List<List<LiteralExpr>> multiValues) {
        this.idToMultiLiteralExprValues.put(partitionId, multiValues);
    }

    public Map<Long, List<List<LiteralExpr>>> getMultiLiteralExprValues() {
        return this.idToMultiLiteralExprValues;
    }

    private void setIsMultiColumnPartition() {
        super.isMultiColumnPartition = this.partitionColumns.size() > 1;
    }

    public Map<Long, List<List<String>>> getIdToMultiValues() {
        return idToMultiValues;
    }

    public Map<Long, List<String>> getIdToValues() {
        return idToValues;
    }

    /**
     * serialize data to log
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    /**
     * deserialize data from log
     *
     * @param in
     * @throws IOException
     */
    public static PartitionInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ListPartitionInfo.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        try {
            Map<Long, List<String>> idToValuesMap = this.getIdToValues();
            for (Map.Entry<Long, List<String>> entry : idToValuesMap.entrySet()) {
                this.setLiteralExprValues(entry.getKey(), entry.getValue());
            }
            Map<Long, List<List<String>>> idToMultiValuesMap = this.getIdToMultiValues();
            for (Map.Entry<Long, List<List<String>>> entry : idToMultiValuesMap.entrySet()) {
                this.setMultiLiteralExprValues(entry.getKey(), entry.getValue());
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
        sb.append(partitionColumns.stream()
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
    public List<Column> getPartitionColumns() {
        return this.partitionColumns;
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

    public void handleNewListPartitionDescs(Map<Partition, PartitionDesc> partitionMap,
                                            Set<String> existPartitionNameSet, boolean isTempPartition)
            throws DdlException {
        try {
            for (Partition partition : partitionMap.keySet()) {
                String name = partition.getName();
                if (!existPartitionNameSet.contains(name)) {
                    long partitionId = partition.getId();
                    PartitionDesc partitionDesc = partitionMap.get(partition);
                    this.idToDataProperty.put(partitionId, partitionDesc.getPartitionDataProperty());
                    this.idToReplicationNum.put(partitionId, partitionDesc.getReplicationNum());
                    this.idToInMemory.put(partitionId, partitionDesc.isInMemory());
                    if (partitionDesc instanceof MultiItemListPartitionDesc) {
                        MultiItemListPartitionDesc multiItemListPartitionDesc =
                                (MultiItemListPartitionDesc) partitionDesc;
                        this.idToMultiValues.put(partitionId, multiItemListPartitionDesc.getMultiValues());
                        this.setMultiLiteralExprValues(partitionId, multiItemListPartitionDesc.getMultiValues());
                    } else if (partitionDesc instanceof SingleItemListPartitionDesc) {
                        SingleItemListPartitionDesc singleItemListPartitionDesc =
                                (SingleItemListPartitionDesc) partitionDesc;
                        this.idToValues.put(partitionId, singleItemListPartitionDesc.getValues());
                        this.setLiteralExprValues(partitionId, singleItemListPartitionDesc.getValues());
                    } else {
                        throw new DdlException(
                                "add list partition only support single item or multi item list partition now");
                    }
                    this.idToIsTempPartition.put(partitionId, isTempPartition);
                }
            }
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }
    }

    public void unprotectHandleNewPartitionDesc(ListPartitionPersistInfo partitionPersistInfo)
            throws AnalysisException {
        Partition partition = partitionPersistInfo.getPartition();
        long partitionId = partition.getId();
        this.idToDataProperty.put(partitionId, partitionPersistInfo.getDataProperty());
        this.idToReplicationNum.put(partitionId, partitionPersistInfo.getReplicationNum());
        this.idToInMemory.put(partitionId, partitionPersistInfo.isInMemory());
        this.idToIsTempPartition.put(partitionId, partitionPersistInfo.isTempPartition());

        List<List<String>> multiValues = partitionPersistInfo.getMultiValues();
        if (multiValues != null && multiValues.size() > 0) {
            this.idToMultiValues.put(partitionId, multiValues);
            this.setMultiLiteralExprValues(partitionId, multiValues);
        }

        List<String> values = partitionPersistInfo.getValues();
        if (values != null && values.size() > 0) {
            this.idToValues.put(partitionId, values);
            this.setLiteralExprValues(partitionId, values);
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
    }

    @Override
    public void moveRangeFromTempToFormal(long tempPartitionId) {
        super.moveRangeFromTempToFormal(tempPartitionId);
        idToIsTempPartition.computeIfPresent(tempPartitionId, (k, v) -> false);
    }

    public void addPartition(long partitionId, DataProperty dataProperty, short replicationNum, boolean isInMemory,
                             StorageCacheInfo storageCacheInfo, List<String> values,
                             List<List<String>> multiValues) throws AnalysisException {
        super.addPartition(partitionId, dataProperty, replicationNum, isInMemory, storageCacheInfo);
        if (multiValues != null && multiValues.size() > 0) {
            this.idToMultiValues.put(partitionId, multiValues);
            this.setMultiLiteralExprValues(partitionId, multiValues);
        }
        if (values != null && values.size() > 0) {
            this.idToValues.put(partitionId, values);
            this.setLiteralExprValues(partitionId, values);
        }
<<<<<<< HEAD
=======
        this.idToStorageCacheInfo.put(partitionId, dataCacheInfo);
        idToIsTempPartition.put(partitionId, false);
>>>>>>> 7414f9228d ([BugFix] Fix partition prune error after truncate list partition (#40495))
    }

    @Override
    public void createAutomaticShadowPartition(long partitionId, String replicateNum) {
        idToValues.put(partitionId, Collections.emptyList());
        idToDataProperty.put(partitionId, new DataProperty(TStorageMedium.HDD));
        idToReplicationNum.put(partitionId, Short.valueOf(replicateNum));
        idToInMemory.put(partitionId, false);
        idToStorageCacheInfo.put(partitionId, new StorageCacheInfo(true,
                Config.lake_default_storage_cache_ttl_seconds, false));
    }

    @Override
    public Object clone() {
        ListPartitionInfo info = (ListPartitionInfo) super.clone();
        info.partitionColumns = Lists.newArrayList(this.partitionColumns);
        info.idToMultiValues = Maps.newHashMap(this.idToMultiValues);
        info.idToMultiLiteralExprValues = Maps.newHashMap(this.idToMultiLiteralExprValues);
        info.idToValues = Maps.newHashMap(this.idToValues);
        info.idToLiteralExprValues = Maps.newHashMap(this.idToLiteralExprValues);
        info.idToIsTempPartition = Maps.newHashMap(this.idToIsTempPartition);
        info.automaticPartition = this.automaticPartition;
        return info;
    }
}