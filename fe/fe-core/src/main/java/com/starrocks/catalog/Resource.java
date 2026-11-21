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
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.persist.AlterResourceInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.ResourceCorePB;
import com.starrocks.persist.proto.ResourcePB;
import com.starrocks.sql.ast.CreateResourceStmt;

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

    public AlterResourceInfo checkAlterProperties(Map<String, String> properties) throws DdlException {
        throw new DdlException("Alter resource statement only support external hive/hudi/iceberg now");
    }

    public void alterProperties(AlterResourceInfo info) {
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public String getDdlStmt() {
        return "";
    }

    /**
     * Image v3 form: the name goes into the core message, the kind names the concrete class, which fills its
     * variant. The type is not written: it only restates the class and the constructors rebuild it.
     */
    public ResourcePB toProto() {
        ResourceCorePB.Builder core = ResourceCorePB.newBuilder();
        if (name != null) {
            core.setName(name);
        }
        ResourcePB.Builder builder = ResourcePB.newBuilder().setCore(core);
        fillProtoVariant(builder);
        return builder.build();
    }

    /** Sets the kind and the variant message of the concrete class. Every persistable subclass overrides it. */
    protected void fillProtoVariant(ResourcePB.Builder builder) {
        throw new IllegalStateException("resource class " + getClass().getName() + " cannot be persisted");
    }

    /** Reads the variant message of the concrete class. */
    protected void fillFromVariantProto(ResourcePB pb) {
        throw new IllegalStateException("resource class " + getClass().getName() + " cannot be persisted");
    }

    public static Resource fromProto(ResourcePB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        String name = pb.getCore().hasName() ? pb.getCore().getName() : null;
        Resource resource;
        switch (pb.getKind()) {
            case SPARK_RESOURCE:
                resource = new SparkResource(name);
                break;
            case HIVE_RESOURCE:
                resource = new HiveResource(name);
                break;
            case ICEBERG_RESOURCE:
                resource = new IcebergResource(name);
                break;
            case HUDI_RESOURCE:
                resource = new HudiResource(name);
                break;
            case ODBC_CATALOG_RESOURCE:
                resource = new OdbcCatalogResource(name);
                break;
            case JDBC_RESOURCE:
                resource = new JDBCResource(name);
                break;
            default:
                throw new IllegalArgumentException("unknown resource kind " + pb.getKind());
        }
        resource.fillFromVariantProto(pb);
        return resource;
    }
}
