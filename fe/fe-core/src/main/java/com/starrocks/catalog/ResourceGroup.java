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

import com.google.gson.annotations.SerializedName;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ResourceGroup {
    public static final String GROUP_TYPE = "type";
    public static final String USER = "user";
    public static final String ROLE = "role";
    public static final String QUERY_TYPE = "query_type";
    public static final String SOURCE_IP = "source_ip";
    public static final String DATABASES = "db";
    public static final String PLAN_CPU_COST_RANGE = "plan_cpu_cost_range";
    public static final String PLAN_MEM_COST_RANGE = "plan_mem_cost_range";
    public static final String CPU_CORE_LIMIT = "cpu_core_limit";
    public static final String MAX_CPU_CORES = "max_cpu_cores";
    public static final String MEM_LIMIT = "mem_limit";
    public static final String BIG_QUERY_MEM_LIMIT = "big_query_mem_limit";
    public static final String BIG_QUERY_SCAN_ROWS_LIMIT = "big_query_scan_rows_limit";
    public static final String BIG_QUERY_CPU_SECOND_LIMIT = "big_query_cpu_second_limit";
    public static final String CONCURRENCY_LIMIT = "concurrency_limit";
    public static final String DEFAULT_RESOURCE_GROUP_NAME = "default_wg";
    public static final String DISABLE_RESOURCE_GROUP_NAME = "disable_resource_group";
    public static final String DEFAULT_MV_RESOURCE_GROUP_NAME = "default_mv_wg";
    public static final String SPILL_MEM_LIMIT_THRESHOLD = "spill_mem_limit_threshold";

    public static final long DEFAULT_WG_ID = 0;
    public static final long DEFAULT_MV_WG_ID = 1;
    public static final long DEFAULT_MV_VERSION = 1;

    public static final ResourceGroup DEFAULT_WG = new ResourceGroup();
    public static final ResourceGroup DEFAULT_MV_WG = new ResourceGroup();

    static {
        DEFAULT_WG.setId(DEFAULT_WG_ID);
        DEFAULT_WG.setName(DEFAULT_RESOURCE_GROUP_NAME);

        DEFAULT_MV_WG.setId(DEFAULT_MV_WG_ID);
        DEFAULT_MV_WG.setName(DEFAULT_MV_RESOURCE_GROUP_NAME);
    }

    public static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("name", ScalarType.createVarchar(100)))
                    .addColumn(new Column("id", ScalarType.createVarchar(200)))
                    .addColumn(new Column("cpu_core_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("mem_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column(MAX_CPU_CORES, ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_cpu_second_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_scan_rows_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_mem_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("concurrency_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("spill_mem_limit_threshold", ScalarType.createVarchar(200)))
                    .addColumn(new Column("type", ScalarType.createVarchar(200)))
                    .addColumn(new Column("classifiers", ScalarType.createVarchar(1024)))
                    .build();
    @SerializedName(value = "classifiers")
    List<ResourceGroupClassifier> classifiers;
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "cpuCoreLimit")
    private Integer cpuCoreLimit;

    @SerializedName(value = "maxCpuCores")
    private Integer maxCpuCores;

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
    @SerializedName(value = "spillMemLimitThreshold")
    private Double spillMemLimitThreshold;
    @SerializedName(value = "workGroupType")
    private TWorkGroupType resourceGroupType;
    @SerializedName(value = "version")
    private long version;

    public ResourceGroup() {
    }

    private List<String> showClassifier(ResourceGroupClassifier classifier) {
        List<String> row = new ArrayList<>();
        row.add(this.name);
        row.add("" + this.id);
        row.add("" + cpuCoreLimit);
        row.add("" + (memLimit * 100) + "%");
        row.add("" + maxCpuCores);
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
        if (spillMemLimitThreshold != null) {
            row.add("" + (spillMemLimitThreshold * 100) + "%");
        } else {
            row.add("" + "100%");
        }
        row.add("" + resourceGroupType.name().substring("WG_".length()));
        row.add(classifier.toString());
        return row;
    }

    public List<List<String>> showVisible(String user, List<String> activeRoles, String ip) {
        return classifiers.stream().filter(c -> c.isVisible(user, activeRoles, ip))
                .map(this::showClassifier).collect(Collectors.toList());
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public List<List<String>> show() {
        if (classifiers.isEmpty()) {
            return Collections.singletonList(showClassifier(new ResourceGroupClassifier()));
        }
        return classifiers.stream().map(this::showClassifier).collect(Collectors.toList());
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

        if (maxCpuCores != null) {
            twg.setMax_cpu_cores(maxCpuCores);
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

        if (spillMemLimitThreshold != null) {
            twg.setSpill_mem_limit_threshold(spillMemLimitThreshold);
        }
        if (resourceGroupType != null) {
            twg.setWorkgroup_type(resourceGroupType);
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

    public boolean isMaxCpuCoresEffective() {
        return maxCpuCores != null && maxCpuCores > 0;
    }

    public void setMaxCpuCores(int maxCpuCores) {
        this.maxCpuCores = maxCpuCores;
    }

    public Integer getMaxCpuCores() {
        return maxCpuCores;
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

    public boolean isConcurrencyLimitEffective() {
        return concurrencyLimit != null && concurrencyLimit > 0;
    }

    public Integer getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public Double getSpillMemLimitThreshold() {
        return spillMemLimitThreshold;
    }

    public void setSpillMemLimitThreshold(double spillMemLimitThreshold) {
        this.spillMemLimitThreshold = spillMemLimitThreshold;
    }

    public TWorkGroupType getResourceGroupType() {
        return resourceGroupType;
    }

    public void setResourceGroupType(TWorkGroupType workGroupType) {
        this.resourceGroupType = workGroupType;
    }

    public List<ResourceGroupClassifier> getClassifiers() {
        return classifiers;
    }

    public void setClassifiers(List<ResourceGroupClassifier> classifiers) {
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
        ResourceGroup resourceGroup = (ResourceGroup) o;
        return id == resourceGroup.id && version == resourceGroup.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version);
    }

}
