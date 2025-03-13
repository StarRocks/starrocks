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
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryPlan {

    @JsonProperty("status")
    private Integer status;

    @JsonProperty("exception")
    private String message;

    @JsonProperty("opaqued_query_plan")
    private String opaquedQueryPlan;

    @JsonProperty("partitions")
    private Map<String, TabletNode> partitions;

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getOpaquedQueryPlan() {
        return opaquedQueryPlan;
    }

    public void setOpaquedQueryPlan(String opaquedQueryPlan) {
        this.opaquedQueryPlan = opaquedQueryPlan;
    }

    public Map<String, TabletNode> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, TabletNode> partitions) {
        this.partitions = partitions;
    }

    public static class TabletNode {

        @JsonProperty("routings")
        private List<String> endpoints;

        private Integer version;

        private Long versionHash;

        private Long schemaHash;

        public TabletNode() {
        }

        public List<String> getEndpoints() {
            return endpoints;
        }

        public void setEndpoints(List<String> endpoints) {
            this.endpoints = endpoints;
        }

        public Integer getVersion() {
            return version;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }

        public Long getVersionHash() {
            return versionHash;
        }

        public void setVersionHash(Long versionHash) {
            this.versionHash = versionHash;
        }

        public Long getSchemaHash() {
            return schemaHash;
        }

        public void setSchemaHash(Long schemaHash) {
            this.schemaHash = schemaHash;
        }
    }

}
