// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Resource.ResourceType;

import java.util.Map;

// CREATE [EXTERNAL] RESOURCE resource_name
// PROPERTIES (key1 = value1, ...)
public class CreateResourceStmt extends DdlStmt {
    private final boolean isExternal;
    private final String resourceName;
    private final Map<String, String> properties;
    private ResourceType resourceType;

    public CreateResourceStmt(boolean isExternal, String resourceName, Map<String, String> properties) {
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
}

