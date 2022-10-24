// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Resource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
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
            try {
                FeNameFormat.checkResourceName(createResourceStmt.getResourceName());
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
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
            try {
                FeNameFormat.checkResourceName(dropResourceStmt.getResourceName());
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitAlterResourceStatement(AlterResourceStmt alterResourceStmt, ConnectContext context) {
            // check name
            try {
                FeNameFormat.checkResourceName(alterResourceStmt.getResourceName());
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
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
