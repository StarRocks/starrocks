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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupType;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
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
    public static final String CPU_WEIGHT = "cpu_weight";
    public static final String EXCLUSIVE_CPU_CORES = "exclusive_cpu_cores";
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

    /**
     * In the old version, DEFAULT_WG and DEFAULT_MV_WG are not saved and persisted in the FE, but are only created in each
     * BE. Its ID is 0 and 1. To distinguish it from the old version, a new ID 2 and 3 is used here.
     */
    public static final long DEFAULT_WG_ID = 2;
    public static final long DEFAULT_MV_WG_ID = 3;

    public static final Set<String> BUILTIN_WG_NAMES =
            ImmutableSet.of(DEFAULT_RESOURCE_GROUP_NAME, DEFAULT_MV_RESOURCE_GROUP_NAME);

    private static class ColumnMeta {
        public ColumnMeta(Column column, BiFunction<ResourceGroup, ResourceGroupClassifier, String> valueSupplier) {
            this(column, valueSupplier, true);
        }

        public ColumnMeta(Column column, BiFunction<ResourceGroup, ResourceGroupClassifier, String> valueSupplier,
                          boolean visible) {
            this.column = column;
            this.valueSupplier = valueSupplier;
            this.visible = visible;
        }

        private final Column column;
        private final BiFunction<ResourceGroup, ResourceGroupClassifier, String> valueSupplier;
        private final boolean visible;
    }

    private static final List<ColumnMeta> COLUMN_METAS = ImmutableList.of(
            new ColumnMeta(
                    new Column("name", ScalarType.createVarchar(100)),
                    (rg, classifier) -> rg.getName()),
            new ColumnMeta(
                    new Column("id", ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + rg.getId()),
            new ColumnMeta(
                    new Column(CPU_WEIGHT, ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + rg.getCpuWeight()),
            new ColumnMeta(
                    new Column(EXCLUSIVE_CPU_CORES, ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + rg.getNormalizedExclusiveCpuCores()),
            new ColumnMeta(
                    new Column(MEM_LIMIT, ScalarType.createVarchar(200)),
                    (rg, classifier) -> (rg.getMemLimit() * 100) + "%"),
            new ColumnMeta(
                    new Column(MAX_CPU_CORES, ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + rg.getMaxCpuCores(), false),
            new ColumnMeta(
                    new Column(BIG_QUERY_CPU_SECOND_LIMIT, ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + Objects.requireNonNullElse(rg.getBigQueryCpuSecondLimit(), 0)),
            new ColumnMeta(
                    new Column(BIG_QUERY_SCAN_ROWS_LIMIT, ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + Objects.requireNonNullElse(rg.getBigQueryScanRowsLimit(), 0)),
            new ColumnMeta(
                    new Column(BIG_QUERY_MEM_LIMIT, ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + Objects.requireNonNullElse(rg.getBigQueryMemLimit(), 0)),
            new ColumnMeta(
                    new Column(CONCURRENCY_LIMIT, ScalarType.createVarchar(200)),
                    (rg, classifier) -> "" + rg.getConcurrencyLimit()),
            new ColumnMeta(
                    new Column(SPILL_MEM_LIMIT_THRESHOLD, ScalarType.createVarchar(200)),
                    (rg, classifier) -> new DecimalFormat("#.##").format(
                            Objects.requireNonNullElse(rg.getSpillMemLimitThreshold(), 1.0) * 100) + "%"),
            new ColumnMeta(
                    new Column(GROUP_TYPE, ScalarType.createVarchar(200)),
                    (rg, classifier) -> rg.getResourceGroupType().name().substring("WG_".length()), false),
            new ColumnMeta(
                    new Column("classifiers", ScalarType.createVarchar(1024)),
                    (rg, classifier) -> classifier.toString())
    );

    public static final ShowResultSetMetaData META_DATA;
    public static final ShowResultSetMetaData VERBOSE_META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        COLUMN_METAS.stream()
                .filter(meta -> meta.visible)
                .forEach(item -> builder.addColumn(item.column));
        META_DATA = builder.build();

        ShowResultSetMetaData.Builder verboseBuilder = ShowResultSetMetaData.builder();
        COLUMN_METAS.forEach(item -> verboseBuilder.addColumn(item.column));
        VERBOSE_META_DATA = verboseBuilder.build();
    }

    @SerializedName(value = "classifiers")
    List<ResourceGroupClassifier> classifiers;
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "cpuCoreLimit")
    private Integer cpuWeight;
    @SerializedName(value = "exclusiveCpuCores")
    private Integer exclusiveCpuCores;

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

    private List<String> showClassifier(ResourceGroupClassifier classifier, boolean verbose) {
        return COLUMN_METAS.stream()
                .filter(meta -> verbose || meta.visible)
                .map(meta -> meta.valueSupplier.apply(this, classifier))
                .collect(Collectors.toList());
    }

    public List<List<String>> showVisible(String user, List<String> activeRoles, String ip, boolean verbose) {
        return classifiers.stream()
                .filter(c -> c.isVisible(user, activeRoles, ip))
                .map(c -> showClassifier(c, verbose))
                .collect(Collectors.toList());
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public List<List<String>> show(boolean verbose) {
        if (classifiers.isEmpty()) {
            return Collections.singletonList(showClassifier(new ResourceGroupClassifier(), verbose));
        }
        return classifiers.stream()
                .map(c -> showClassifier(c, verbose))
                .collect(Collectors.toList());
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
        if (cpuWeight != null) {
            twg.setCpu_core_limit(cpuWeight);
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

        twg.setExclusive_cpu_cores(getNormalizedExclusiveCpuCores());

        twg.setVersion(version);
        return twg;
    }

    public Integer getCpuWeight() {
        return cpuWeight;
    }

    public void setCpuWeight(int cpuWeight) {
        this.cpuWeight = cpuWeight;
    }

    public Integer getExclusiveCpuCores() {
        return exclusiveCpuCores;
    }

    public int getNormalizedExclusiveCpuCores() {
        if (exclusiveCpuCores != null && exclusiveCpuCores > 0) {
            return exclusiveCpuCores;
        }
        // For compatibility, resource group of deprecated type `short_query` uses `cpu_weight` (the original `cpu_core_limit`)
        // as the reserved cores.
        if (resourceGroupType == TWorkGroupType.WG_SHORT_QUERY && cpuWeight != null && cpuWeight > 0) {
            return cpuWeight;
        }
        return 0;
    }

    public void setExclusiveCpuCores(Integer exclusiveCpuCores) {
        this.exclusiveCpuCores = exclusiveCpuCores;
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

    public static void validateCpuParameters(Integer cpuWeight, Integer exclusiveCpuCores) {
        if ((cpuWeight == null || cpuWeight <= 0) && (exclusiveCpuCores == null || exclusiveCpuCores <= 0)) {
            throw new SemanticException(String.format("property '%s' or '%s' must be positive",
                    ResourceGroup.CPU_WEIGHT, ResourceGroup.EXCLUSIVE_CPU_CORES));
        }
        if ((cpuWeight != null && cpuWeight > 0) && (exclusiveCpuCores != null && exclusiveCpuCores > 0)) {
            throw new SemanticException(
                    String.format("property '%s' and '%s' cannot be present and positive at the same time",
                            ResourceGroup.CPU_WEIGHT, ResourceGroup.EXCLUSIVE_CPU_CORES));
        }
    }

}
