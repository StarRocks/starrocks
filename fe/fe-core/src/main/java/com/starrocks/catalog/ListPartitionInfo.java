// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public ListPartitionInfo(PartitionType partitionType,
                             List<Column> partitionColumns) {
        super(partitionType);
        this.partitionColumns = partitionColumns;
        this.setIsMultiColumnPartition();

        this.idToValues = new HashMap<>();
        this.idToLiteralExprValues = new HashMap<>();
        this.idToMultiValues = new HashMap<>();
        this.idToMultiLiteralExprValues = new HashMap<>();
    }

    public ListPartitionInfo() {
        super();
        this.idToValues = new HashMap<>();
        this.idToLiteralExprValues = new HashMap<>();
        this.idToMultiValues = new HashMap<>();
        this.idToMultiLiteralExprValues = new HashMap<>();
        this.partitionColumns = new ArrayList<>();
    }

    public void setValues(long partitionId, List<String> values) {
        this.idToValues.put(partitionId, values);
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

    public void setMultiValues(long partitionId, List<List<String>> multiValues) {
        this.idToMultiValues.put(partitionId, multiValues);
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
        super.write(out);

        //target to serialize member partitionColumns,idToMultiValues,idToValues
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
        PartitionInfo list = new ListPartitionInfo();
        list.readFields(in);

        //target to deserialize member partitionColumns,idToMultiValues,idToValues
        String json = Text.readString(in);
        ListPartitionInfo partitionInfo = GsonUtils.GSON.fromJson(json, ListPartitionInfo.class);
        list.idToInMemory.forEach((k, v) -> partitionInfo.setIsInMemory(k, v));
        list.idToDataProperty.forEach((k, v) -> partitionInfo.setDataProperty(k, v));
        list.idToReplicationNum.forEach((k, v) -> partitionInfo.setReplicationNum(k, v));
        list.idToTabletType.forEach((k, v) -> partitionInfo.setTabletType(k, v));
        partitionInfo.setIsMultiColumnPartition();
        partitionInfo.type = list.getType();

        try {
            Map<Long, List<String>> idToValuesMap = partitionInfo.getIdToValues();
            for (Map.Entry<Long, List<String>> entry : idToValuesMap.entrySet()) {
                partitionInfo.setLiteralExprValues(entry.getKey(), entry.getValue());
            }
            Map<Long, List<List<String>>> idToMultiValuesMap = partitionInfo.getIdToMultiValues();
            for (Map.Entry<Long, List<List<String>>> entry : idToMultiValuesMap.entrySet()) {
                partitionInfo.setMultiLiteralExprValues(entry.getKey(), entry.getValue());
            }
        } catch (AnalysisException e) {
            LOG.error("deserialize PartitionInfo error", e);
        }
        return partitionInfo;
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        String replicationNumStr = table.getTableProperty()
                .getProperties().get(PROPERTIES_REPLICATION_NUM);
        short tableReplicationNum = replicationNumStr == null ?
                FeConstants.default_replication_num : Short.parseShort(replicationNumStr);

        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY LIST(");
        sb.append(this.partitionColumns.stream()
                .map(item -> "`" + item.getName() + "`")
                .collect(Collectors.joining(",")));
        sb.append(")(\n");

        if (!this.idToValues.isEmpty()) {
            sb.append(this.singleListPartitionSql(table, tableReplicationNum));
        }

        if (!this.idToMultiValues.isEmpty()) {
            sb.append(this.multiListPartitionSql(table, tableReplicationNum));
        }

        sb.append("\n)");
        return sb.toString();
    }

    private String singleListPartitionSql(OlapTable table, short tableReplicationNum) {
        StringBuilder sb = new StringBuilder();
        this.idToLiteralExprValues.forEach((partitionId, values) -> {
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
        });
        return StringUtils.removeEnd(sb.toString(), ",\n");
    }

    private String valuesToString(List<LiteralExpr> values) {
        return "(" + values.stream().map(value -> "\'" + value.getStringValue() + "\'")
                .collect(Collectors.joining(", ")) + ")";
    }

    private String multiListPartitionSql(OlapTable table, short tableReplicationNum) {
        StringBuilder sb = new StringBuilder();
        this.idToMultiLiteralExprValues.forEach((partitionId, multiValues) -> {
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
        if (!this.idToLiteralExprValues.isEmpty()) {
            return this.valuesToString(this.idToLiteralExprValues.get(partitionId));
        }
        if (!this.idToMultiLiteralExprValues.isEmpty()) {
            return this.multiValuesToString(this.idToMultiLiteralExprValues.get(partitionId));
        }
        return "";
    }
}
