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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Resource.java

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

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.CreateResourceStmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public abstract class Resource implements Writable {
    public enum ResourceType {
        UNKNOWN,
        SPARK,
        HIVE,
        ICEBERG,
        HUDI,
        ODBC_CATALOG,
        JDBC;

        public static ResourceType fromString(String resourceType) {
            for (ResourceType type : ResourceType.values()) {
                if (type.name().equalsIgnoreCase(resourceType)) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected ResourceType type;

    public Resource(String name, ResourceType type) {
        this.name = name;
        this.type = type;
    }

    public static Resource fromStmt(CreateResourceStmt stmt) throws DdlException {
        Resource resource = null;
        ResourceType type = stmt.getResourceType();
        switch (type) {
            case SPARK:
                resource = new SparkResource(stmt.getResourceName());
                break;
            case HIVE:
                resource = new HiveResource(stmt.getResourceName());
                break;
            case ICEBERG:
                resource = new IcebergResource(stmt.getResourceName());
                break;
            case HUDI:
                resource = new HudiResource(stmt.getResourceName());
                break;
            case ODBC_CATALOG:
                resource = new OdbcCatalogResource(stmt.getResourceName());
                break;
            case JDBC:
                resource = new JDBCResource(stmt.getResourceName());
                break;
            default:
                throw new DdlException("Unsupported resource type: " + type);
        }

        resource.setProperties(stmt.getProperties());
        return resource;
    }

    public String getName() {
        return name;
    }

    public ResourceType getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return Maps.newHashMap();
    }

    public String getHiveMetastoreURIs() {
        return "";
    }

    public boolean needMappingCatalog() {
        return type == ResourceType.HIVE || type == ResourceType.HUDI || type == ResourceType.ICEBERG;
    }

    /**
     * Set and check the properties in child resources
     */
    protected abstract void setProperties(Map<String, String> properties) throws DdlException;

    /**
     * Fill BaseProcResult with different properties in child resources
     * ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES format:
     * | Name | ResourceType | Key | Value |
     */
    protected abstract void getProcNodeData(BaseProcResult result);

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Resource read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Resource.class);
    }

    public String getDdlStmt() {
        return "";
    }
}

