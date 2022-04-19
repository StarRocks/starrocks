package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.ListPartitionDesc;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ListPartitionInfo extends PartitionInfo{

    @SerializedName("partitionColumns")
    private List<Column> partitionColumns;
    @SerializedName("idToValues")
    private Map<Long,List<String>> idToValues;
    @SerializedName("idToMultiValues")
    private Map<Long,List<List<String>>> idToMultiValues;

    public ListPartitionInfo(PartitionType partitionType,
                              List<Column> partitionColumns){
        super(partitionType);
        this.partitionColumns = partitionColumns;
        this.setIsMultiColumnPartition();

        idToValues = new HashMap<>();
        idToMultiValues = new HashMap<>();
    }

    public ListPartitionInfo() {
        super();
        idToValues = new HashMap<>();
        idToMultiValues = new HashMap<>();
        partitionColumns = new ArrayList<>();
    }

    public void setValues(long partitionId, List<String> values) {
        idToValues.put(partitionId, values);
    }

    public void setMultiValues(long partitionId, List<List<String>> multiValues) {
        idToMultiValues.put(partitionId, multiValues);
    }

    public void setIsMultiColumnPartition(){
        super.isMultiColumnPartition = this.partitionColumns.size() > 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo list = new ListPartitionInfo();
        list.readFields(in);

        String json = Text.readString(in);
        ListPartitionInfo partitionInfo = GsonUtils.GSON.fromJson(json,ListPartitionInfo.class);
        list.idToInMemory.forEach((k,v) -> partitionInfo.setIsInMemory(k,v));
        list.idToDataProperty.forEach((k,v) -> partitionInfo.setDataProperty(k,v));
        list.idToReplicationNum.forEach((k,v) -> partitionInfo.setReplicationNum(k,v));
        list.idToTabletType.forEach((k,v) -> partitionInfo.setTabletType(k,v));
        partitionInfo.setIsMultiColumnPartition();
        partitionInfo.type = list.getType();
        return partitionInfo;
    }

    private String singleListPartitionSql(OlapTable table, short tableReplicationNum) {
        StringBuilder sb = new StringBuilder();
        idToValues.forEach((partitionId,values) -> {
            Short partitionReplicaNum = table.getPartitionInfo().idToReplicationNum.get(partitionId);
            Optional.ofNullable(table.getPartition(partitionId)).ifPresent(partition -> {
                String partitionName = partition.getName();
                sb.append("  PARTITION ").append(partitionName).append(" VALUES IN (");
                sb.append(values.stream().map(value -> "\"" + value + "\"")
                        .collect(Collectors.joining(",")));
                sb.append(")");

                if (partitionReplicaNum != null && partitionReplicaNum != tableReplicationNum){
                    sb.append(" (").append("\"replication_num\" = \"").append(partitionReplicaNum).append("\")");
                }
                sb.append(",\n");
            });
        });
        return StringUtils.removeEnd(sb.toString(),",\n");
    }

    private String multiListPartitionSql(OlapTable table, short tableReplicationNum) {
        StringBuilder sb = new StringBuilder();
        idToMultiValues.forEach((partitionId,multiValues) -> {
            Short partitionReplicaNum = table.getPartitionInfo().idToReplicationNum.get(partitionId);
            Optional.ofNullable(table.getPartition(partitionId)).ifPresent(partition -> {
                String partitionName = partition.getName();
                sb.append("  PARTITION ").append(partitionName).append(" VALUES IN (");
                String items = multiValues.stream()
                        .map(values -> "(" + values.stream().map(value -> "\"" + value + "\"")
                                .collect(Collectors.joining(","))+")")
                        .collect(Collectors.joining(","));
                sb.append(items);
                sb.append(")");

                if (partitionReplicaNum != null && partitionReplicaNum != tableReplicationNum){
                    sb.append(" (").append("\"replication_num\" = \"").append(partitionReplicaNum).append("\")");
                }

                sb.append(",\n");
            });
        });
        return StringUtils.removeEnd(sb.toString(),",\n");
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        String replicationNumStr = table.getTableProperty()
                .getProperties().get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
        short tableReplicationNum = replicationNumStr == null ?
                FeConstants.default_replication_num : Short.parseShort(replicationNumStr);

        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY LIST(");
        sb.append(this.partitionColumns.stream()
                .map(item -> "`" + item.getName() + "`")
                .collect(Collectors.joining(",")));
        sb.append(")(\n");

        if (!this.idToValues.isEmpty()){
            sb.append(this.singleListPartitionSql(table,tableReplicationNum));
        }

        if (!this.idToMultiValues.isEmpty()){
            sb.append(this.multiListPartitionSql(table,tableReplicationNum));
        }

        sb.append("\n)");
        return sb.toString();
    }

    public List<Column> getPartitionColumns() {
        return this.partitionColumns;
    }

    public void handleNewListPartitionDescsV2(List<PartitionDesc> listDesc,
                                              List<Partition> partitionList, Set<String> existPartitionNameSet,
                                              boolean isTemp) {
        int len = listDesc.size();
        for (int i = 0; i < len; i++) {
            if (!existPartitionNameSet.contains(partitionList.get(i).getName())) {
                long partitionId = partitionList.get(i).getId();
                ListPartitionDesc desc = (ListPartitionDesc)listDesc.get(i);
                idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
                idToReplicationNum.put(partitionId, desc.getReplicationNum());
                idToInMemory.put(partitionId, desc.isInMemory());
            }
        }
    }

    public Map<Long, List<String>> getIdToValues() {
        return idToValues;
    }

    public Map<Long, List<List<String>>> getIdToMultiValues() {
        return idToMultiValues;
    }
}
