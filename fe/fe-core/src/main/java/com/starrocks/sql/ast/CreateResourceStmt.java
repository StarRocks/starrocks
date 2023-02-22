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


package com.starrocks.sql.ast;

import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// CREATE [EXTERNAL] RESOURCE resource_name
// PROPERTIES (key1 = value1, ...)
public class CreateResourceStmt extends DdlStmt {
    private final boolean isExternal;
    private final String resourceName;
    private final Map<String, String> properties;
    private ResourceType resourceType;

    public CreateResourceStmt(boolean isExternal, String resourceName, Map<String, String> properties) {
        this(isExternal, resourceName, properties, NodePosition.ZERO);
    }

    public CreateResourceStmt(boolean isExternal, String resourceName, Map<String, String> properties,
                              NodePosition pos) {
        super(pos);
        this.isExternal = isExternal;
        this.resourceName = resourceName;
        this.properties = properties;
        this.resourceType = ResourceType.UNKNOWN;
    }

    public String getResourceName() {
        return resourceName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public boolean isExternal() {
        return isExternal;
    }

    // only used for UT
    public void setResourceType(ResourceType type) {
        this.resourceType = type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateResourceStatement(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}

