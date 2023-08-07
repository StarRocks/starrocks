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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Resource;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.Arrays;
import java.util.Map;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_TYPE;

// [SHOW | CREATE | DROP] resource Analyzer
public class ResourceAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new ResourceAnalyzer.ResourceAnalyzerVisitor().visit(stmt, session);
    }

    static class ResourceAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateResourceStatement(CreateResourceStmt createResourceStmt, ConnectContext context) {
            // check name
            FeNameFormat.checkResourceName(createResourceStmt.getResourceName());
            // check type in properties
            Map<String, String> properties = createResourceStmt.getProperties();
            if (properties == null || properties.isEmpty()) {
                throw new SemanticException("Resource properties can't be null");
            }
            String typeStr = null;
            if (properties.containsKey(PROPERTIES_TYPE)) {
                typeStr = properties.get(PROPERTIES_TYPE);
            }
            if (typeStr == null) {
                throw new SemanticException("Resource type can't be null");
            }
            Resource.ResourceType resourceType = Resource.ResourceType.fromString(typeStr);
            if (resourceType == Resource.ResourceType.UNKNOWN) {
                throw new SemanticException("Unrecognized resource type: " + typeStr + ". " + "Only " +
                        Arrays.toString(Arrays.stream(Resource.ResourceType.values())
                                .filter(t -> t != Resource.ResourceType.UNKNOWN).toArray()) + " are supported.");
            }
            createResourceStmt.setResourceType(resourceType);
            if (!createResourceStmt.isExternal()) {
                throw new SemanticException(resourceType + " resource type must be external.");
            }
            return null;
        }

        @Override
        public Void visitDropResourceStatement(DropResourceStmt dropResourceStmt, ConnectContext context) {
            FeNameFormat.checkResourceName(dropResourceStmt.getResourceName());
            return null;
        }

        @Override
        public Void visitAlterResourceStatement(AlterResourceStmt alterResourceStmt, ConnectContext context) {
            // check name
            FeNameFormat.checkResourceName(alterResourceStmt.getResourceName());
            // check properties
            Map<String, String> properties = alterResourceStmt.getProperties();
            String typeStr = PropertyAnalyzer.analyzeType(properties);
            // not allow to modify the resource type
            if (typeStr != null) {
                throw new SemanticException("Not allow to modify the resource type");
            }
            return null;
        }
    }
}
