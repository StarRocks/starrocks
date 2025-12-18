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
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.common.io.Writable;
import com.starrocks.thrift.TWorkGroupType;

import java.util.List;

public class AlterResourceGroupLog implements Writable {

    @SerializedName(value = "classifiers")
    List<ResourceGroupClassifier> classifiers;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "cpuCoreLimit")
    private Integer cpuWeight;

    @SerializedName(value = "exclusiveCpuCores")
    private Integer exclusiveCpuCores;

    @SerializedName(value = "maxCpuCores")
    private Integer maxCpuCores;

    @SerializedName(value = "memLimit")
    private Double memLimit;

    @SerializedName(value = "memPool")
    private String memPool;

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

    public List<ResourceGroupClassifier> getClassifiers() {
        return classifiers;
    }

    public void setClassifiers(List<ResourceGroupClassifier> classifiers) {
        this.classifiers = classifiers;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCpuWeight() {
        return cpuWeight;
    }

    public void setCpuWeight(Integer cpuWeight) {
        this.cpuWeight = cpuWeight;
    }

    public Integer getExclusiveCpuCores() {
        return exclusiveCpuCores;
    }

    public void setExclusiveCpuCores(Integer exclusiveCpuCores) {
        this.exclusiveCpuCores = exclusiveCpuCores;
    }

    public Integer getMaxCpuCores() {
        return maxCpuCores;
    }

    public void setMaxCpuCores(Integer maxCpuCores) {
        this.maxCpuCores = maxCpuCores;
    }

    public Double getMemLimit() {
        return memLimit;
    }

    public void setMemLimit(Double memLimit) {
        this.memLimit = memLimit;
    }

    public String getMemPool() {
        return memPool;
    }

    public void setMemPool(String memPool) {
        this.memPool = memPool;
    }

    public Long getBigQueryMemLimit() {
        return bigQueryMemLimit;
    }

    public void setBigQueryMemLimit(Long bigQueryMemLimit) {
        this.bigQueryMemLimit = bigQueryMemLimit;
    }

    public Long getBigQueryScanRowsLimit() {
        return bigQueryScanRowsLimit;
    }

    public void setBigQueryScanRowsLimit(Long bigQueryScanRowsLimit) {
        this.bigQueryScanRowsLimit = bigQueryScanRowsLimit;
    }

    public Long getBigQueryCpuSecondLimit() {
        return bigQueryCpuSecondLimit;
    }

    public void setBigQueryCpuSecondLimit(Long bigQueryCpuSecondLimit) {
        this.bigQueryCpuSecondLimit = bigQueryCpuSecondLimit;
    }

    public Integer getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(Integer concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public Double getSpillMemLimitThreshold() {
        return spillMemLimitThreshold;
    }

    public void setSpillMemLimitThreshold(Double spillMemLimitThreshold) {
        this.spillMemLimitThreshold = spillMemLimitThreshold;
    }

    public TWorkGroupType getResourceGroupType() {
        return resourceGroupType;
    }

    public void setResourceGroupType(TWorkGroupType resourceGroupType) {
        this.resourceGroupType = resourceGroupType;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}
