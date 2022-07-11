// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class WorkGroup implements Writable {
    public static final String USER = "user";
    public static final String ROLE = "role";
    public static final String QUERY_TYPE = "query_type";
    public static final String SOURCE_IP = "source_ip";
    public static final String DATABASES = "db";
    public static final String CPU_CORE_LIMIT = "cpu_core_limit";
    public static final String MEM_LIMIT = "mem_limit";
    public static final String BIG_QUERY_MEM_LIMIT = "big_query_mem_limit";
    public static final String BIG_QUERY_SCAN_ROWS_LIMIT = "big_query_scan_rows_limit";
    public static final String BIG_QUERY_CPU_SECOND_LIMIT = "big_query_cpu_second_limit";
    public static final String CONCURRENCY_LIMIT = "concurrency_limit";
    public static final String WORKGROUP_TYPE = "type";
    public static final String DEFAULT_WORKGROUP_NAME = "default_wg";

    public static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("name", ScalarType.createVarchar(100)))
                    .addColumn(new Column("id", ScalarType.createVarchar(200)))
                    .addColumn(new Column("cpu_core_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("mem_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_cpu_second_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_scan_rows_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_mem_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("concurrency_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("type", ScalarType.createVarchar(200)))
                    .addColumn(new Column("classifiers", ScalarType.createVarchar(1024)))
                   .build();

    @SerializedName(value = "classifiers")
    List<WorkGroupClassifier> classifiers;
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "cpuCoreLimit")
    private Integer cpuCoreLimit;
    @SerializedName(value = "memLimit")
    private Double memLimit;
    @SerializedName(value = "bigQueryMemLimit")
    private Long bigQueryMemLimit;
    @SerializedName(value = "bigQueryScanRowsLimit")
    private Long bigQueryScanRowsLimit;
    @SerializedName(value = "bigQueryCpuSecondLimit")
    private Long bigQueryCpuSecondLimit;
    @SerializedName(value = "concurrencyLimit")
    private Integer concurrencyLimit;
    @SerializedName(value = "workGroupType")
    private TWorkGroupType workGroupType;
    @SerializedName(value = "version")
    private long version;

    public WorkGroup() {
    }

    public static WorkGroup read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkGroup.class);
    }

    private List<String> showClassifier(WorkGroupClassifier classifier) {
        List<String> row = new ArrayList<>();
        row.add(this.name);
        row.add("" + this.id);
        row.add("" + cpuCoreLimit);
        row.add("" + (memLimit * 100) + "%");
        if (bigQueryCpuSecondLimit != null) {
            row.add("" + bigQueryCpuSecondLimit);
        } else {
            row.add("" + 0);
        }
        if (bigQueryScanRowsLimit != null) {
            row.add("" + bigQueryScanRowsLimit);
        } else {
            row.add("" + 0);
        }
        if (bigQueryMemLimit != null) {
            row.add("" + bigQueryMemLimit);
        } else {
            row.add("" + 0);
        }
        row.add("" + concurrencyLimit);
        row.add("" + workGroupType.name().substring("WG_".length()));
        row.add(classifier.toString());
        return row;
    }

    public List<List<String>> showVisible(String user, String roleName, String ip) {
        return classifiers.stream().filter(c -> c.isVisible(user, roleName, ip))
                .map(this::showClassifier).collect(Collectors.toList());
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public List<List<String>> show() {
        return classifiers.stream().map(c -> this.showClassifier(c)).collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public TWorkGroup toThrift() {
        TWorkGroup twg = new TWorkGroup();
        twg.setName(name);
        twg.setId(id);
        if (cpuCoreLimit != null) {
            twg.setCpu_core_limit(cpuCoreLimit);
        }
        if (memLimit != null) {
            twg.setMem_limit(memLimit);
        }

        if (bigQueryMemLimit != null) {
            twg.setBig_query_mem_limit(bigQueryMemLimit);
        }

        if (bigQueryScanRowsLimit != null) {
            twg.setBig_query_scan_rows_limit(bigQueryScanRowsLimit);
        }

        if (bigQueryCpuSecondLimit != null) {
            twg.setBig_query_cpu_second_limit(bigQueryCpuSecondLimit);
        }

        if (concurrencyLimit != null) {
            twg.setConcurrency_limit(concurrencyLimit);
        }
        if (workGroupType != null) {
            twg.setWorkgroup_type(workGroupType);
        }
        twg.setVersion(version);
        return twg;
    }

    public Integer getCpuCoreLimit() {
        return cpuCoreLimit;
    }

    public void setCpuCoreLimit(int cpuCoreLimit) {
        this.cpuCoreLimit = cpuCoreLimit;
    }

    public Double getMemLimit() {
        return memLimit;
    }

    public void setMemLimit(double memLimit) {
        this.memLimit = memLimit;
    }

    public Long getBigQueryMemLimit() {
        return bigQueryMemLimit;
    }

    public void setBigQueryMemLimit(long limit) {
        bigQueryMemLimit = limit;
    }

    public Long getBigQueryScanRowsLimit() {
        return bigQueryScanRowsLimit;
    }

    public void setBigQueryScanRowsLimit(long limit) {
        bigQueryScanRowsLimit = limit;
    }

    public Long getBigQueryCpuSecondLimit() {
        return bigQueryCpuSecondLimit;
    }

    public void setBigQueryCpuSecondLimit(long limit) {
        bigQueryCpuSecondLimit = limit;
    }

    public Integer getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public TWorkGroupType getWorkGroupType() {
        return workGroupType;
    }

    public void setWorkGroupType(TWorkGroupType workGroupType) {
        this.workGroupType = workGroupType;
    }

    public List<WorkGroupClassifier> getClassifiers() {
        return classifiers;
    }

    public void setClassifiers(List<WorkGroupClassifier> classifiers) {
        this.classifiers = classifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WorkGroup workGroup = (WorkGroup) o;
        return id == workGroup.id && version == workGroup.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version);
    }
}
