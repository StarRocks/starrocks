// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.lang3.StringUtils;

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

    @SerializedName("partitionColumns")
    private List<Column> partitionColumns;
    //serialize values for statement like `PARTITION p1 VALUES IN (("2022-04-01", "beijing"))`
    @SerializedName("idToMultiValues")
    private Map<Long, List<List<String>>> idToMultiValues;
    //serialize values for statement like `PARTITION p1 VALUES IN ("beijing","chongqing")`
    @SerializedName("idToValues")
    private Map<Long, List<String>> idToValues;

    public ListPartitionInfo(PartitionType partitionType,
                             List<Column> partitionColumns) {
        super(partitionType);
        this.partitionColumns = partitionColumns;
        this.setIsMultiColumnPartition();

        this.idToValues = new HashMap<>();
        this.idToMultiValues = new HashMap<>();
    }

    public ListPartitionInfo() {
        super();
        this.idToValues = new HashMap<>();
        this.idToMultiValues = new HashMap<>();
        this.partitionColumns = new ArrayList<>();
    }

    public void setValues(long partitionId, List<String> values) {
        this.idToValues.put(partitionId, values);
    }

    public void setMultiValues(long partitionId, List<List<String>> multiValues) {
        this.idToMultiValues.put(partitionId, multiValues);
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
        idToValues.forEach((partitionId, values) -> {
            Short partitionReplicaNum = table.getPartitionInfo().idToReplicationNum.get(partitionId);
            Optional.ofNullable(table.getPartition(partitionId)).ifPresent(partition -> {
                String partitionName = partition.getName();
                sb.append("  PARTITION ").append(partitionName).append(" VALUES IN (");
                sb.append(values.stream().map(value -> "\'" + value + "\'")
                        .collect(Collectors.joining(",")));
                sb.append(")");

                if (partitionReplicaNum != null && partitionReplicaNum != tableReplicationNum) {
                    sb.append(" (").append("\"" + PROPERTIES_REPLICATION_NUM + "\" = \"").append(partitionReplicaNum)
                            .append("\")");
                }
                sb.append(",\n");
            });
        });
        return StringUtils.removeEnd(sb.toString(), ",\n");
    }

    private String multiListPartitionSql(OlapTable table, short tableReplicationNum) {
        StringBuilder sb = new StringBuilder();
        idToMultiValues.forEach((partitionId, multiValues) -> {
            Short partitionReplicaNum = table.getPartitionInfo().idToReplicationNum.get(partitionId);
            Optional.ofNullable(table.getPartition(partitionId)).ifPresent(partition -> {
                String partitionName = partition.getName();
                sb.append("  PARTITION ").append(partitionName).append(" VALUES IN (");
                String items = multiValues.stream()
                        .map(values -> "(" + values.stream().map(value -> "\'" + value + "\'")
                                .collect(Collectors.joining(",")) + ")")
                        .collect(Collectors.joining(","));
                sb.append(items);
                sb.append(")");

                if (partitionReplicaNum != null && partitionReplicaNum != tableReplicationNum) {
                    sb.append(" (").append("\"" + PROPERTIES_REPLICATION_NUM + "\" = \"").append(partitionReplicaNum)
                            .append("\")");
                }

                sb.append(",\n");
            });
        });
        return StringUtils.removeEnd(sb.toString(), ",\n");
    }

    @Override
    public List<Column> getPartitionColumns() {
        return this.partitionColumns;
    }
}
