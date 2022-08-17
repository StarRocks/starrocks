// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateResourceStmt.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.util.Arrays;
import java.util.Map;

// CREATE [EXTERNAL] RESOURCE resource_name
// PROPERTIES (key1 = value1, ...)
public class CreateResourceStmt extends DdlStmt {
    private static final String TYPE = "type";

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

    // only used for UT
    public void setResourceType(ResourceType type) {
        this.resourceType = type;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkResourceName(resourceName);

        // check type in properties
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Resource properties can't be null");
        }
        String type = properties.get(TYPE);
        if (type == null) {
            throw new AnalysisException("Resource type can't be null");
        }
        resourceType = ResourceType.fromString(type);
        if (resourceType == ResourceType.UNKNOWN) {
            throw new AnalysisException("Unrecognized resource type: " + type + ". " + "Only " +
                    Arrays.toString(Arrays.stream(ResourceType.values())
                            .filter(t -> t != ResourceType.UNKNOWN).toArray()) + " are supported.");
        }
        if (!isExternal) {
            throw new AnalysisException(resourceType + " resource type must be external.");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (isExternal) {
            sb.append("EXTERNAL ");
        }
        sb.append("RESOURCE '").append(resourceName).append("' ");
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }
}

