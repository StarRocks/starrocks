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

package com.starrocks.format.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TablePartition {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("bucketNum")
    private Integer bucketNum;

    @JsonProperty("distributionType")
    private String distributionType;

    @JsonProperty("visibleVersion")
    private Long visibleVersion;

    @JsonProperty("visibleVersionTime")
    private Long visibleVersionTime;

    @JsonProperty("nextVersion")
    private Long nextVersion;

    @JsonProperty("isMinPartition")
    private Boolean isMinPartition;

    @JsonProperty("isMaxPartition")
    private Boolean isMaxPartition;

    @JsonProperty("startKeys")
    private List<Object> startKeys;

    @JsonProperty("endKeys")
    private List<Object> endKeys;

    @JsonProperty("inKeys")
    private List<List<Object>> inKeys;

    @JsonProperty("storagePath")
    private String storagePath;

    @JsonProperty("tablets")
    private List<Tablet> tablets;

    public TablePartition() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(Integer bucketNum) {
        this.bucketNum = bucketNum;
    }

    public String getDistributionType() {
        return distributionType;
    }

    public void setDistributionType(String distributionType) {
        this.distributionType = distributionType;
    }

    public Long getVisibleVersion() {
        return visibleVersion;
    }

    public void setVisibleVersion(Long visibleVersion) {
        this.visibleVersion = visibleVersion;
    }

    public Long getVisibleVersionTime() {
        return visibleVersionTime;
    }

    public void setVisibleVersionTime(Long visibleVersionTime) {
        this.visibleVersionTime = visibleVersionTime;
    }

    public Long getNextVersion() {
        return nextVersion;
    }

    public void setNextVersion(Long nextVersion) {
        this.nextVersion = nextVersion;
    }

    public Boolean getMinPartition() {
        return isMinPartition;
    }

    public void setMinPartition(Boolean minPartition) {
        isMinPartition = minPartition;
    }

    public Boolean getMaxPartition() {
        return isMaxPartition;
    }

    public void setMaxPartition(Boolean maxPartition) {
        isMaxPartition = maxPartition;
    }

    public List<Object> getStartKeys() {
        return startKeys;
    }

    public void setStartKeys(List<Object> startKeys) {
        this.startKeys = startKeys;
    }

    public List<Object> getEndKeys() {
        return endKeys;
    }

    public void setEndKeys(List<Object> endKeys) {
        this.endKeys = endKeys;
    }

    public List<List<Object>> getInKeys() {
        return inKeys;
    }

    public void setInKeys(List<List<Object>> inKeys) {
        this.inKeys = inKeys;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public List<Tablet> getTablets() {
        return tablets;
    }

    public void setTablets(List<Tablet> tablets) {
        this.tablets = tablets;
    }

    public static class Tablet {

        @JsonProperty("id")
        private Long id;

        @JsonProperty("primaryComputeNodeId")
        private Long primaryComputeNodeId;

        @JsonProperty("backendIds")
        private Set<Long> backendIds;

        public Tablet() {
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getPrimaryComputeNodeId() {
            return primaryComputeNodeId;
        }

        public void setPrimaryComputeNodeId(Long primaryComputeNodeId) {
            this.primaryComputeNodeId = primaryComputeNodeId;
        }

        public Set<Long> getBackendIds() {
            return backendIds;
        }

        public void setBackendIds(Set<Long> backendIds) {
            this.backendIds = backendIds;
        }
    }

}
