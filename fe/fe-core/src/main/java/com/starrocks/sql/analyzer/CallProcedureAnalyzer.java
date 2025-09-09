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

import com.starrocks.connector.Procedure;
import com.starrocks.connector.iceberg.procedure.NamedArgument;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.CallProcedureStatement;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class CallProcedureAnalyzer {
    private static final String DEFAULT_DB_NAME = "system";

    public static void analyze(CallProcedureStatement stmt, ConnectContext session) {
        new CallProcedureAnalyzer.CallProcedureStmtAnalyzerVisitor().visit(stmt, session);
    }

    public static class CallProcedureStmtAnalyzerVisitor implements AstVisitorExtendInterface<Void, ConnectContext> {
        @Override
        public Void visitCallProcedureStatement(CallProcedureStatement stmt, ConnectContext context) {
            QualifiedName qualifiedName = stmt.getQualifiedName();
            if (qualifiedName.getParts().isEmpty()) {
                throw new SemanticException("Procedure name cannot be empty", stmt);
            }
            QualifiedName normalizeQualifiedName = normalizeQualifiedName(qualifiedName, context);
            stmt.setQualifiedName(normalizeQualifiedName);

            Procedure procedure = GlobalStateMgr.getCurrentState().getMetadataMgr().getProcedure(
                            normalizeQualifiedName.getParts().get(0), normalizeQualifiedName.getParts().get(1),
                            normalizeQualifiedName.getParts().get(2))
                    .orElseThrow(() -> new SemanticException("Procedure not found: " + normalizeQualifiedName));
            stmt.setProcedure(procedure);

            checkArguments(stmt, procedure, context);
            return null;
        }

        private void checkArguments(CallProcedureStatement stmt, Procedure procedure, ConnectContext context) {
            List<ProcedureArgument> callArgs = stmt.getArguments();
            List<NamedArgument> procedureArgs = procedure.getArguments();

            // check named and unnamed arguments are mixing used.
            boolean anyNamedArgs = callArgs.stream().anyMatch(arg -> arg.getName().isPresent());
            boolean allNamedArgs = callArgs.stream().allMatch(arg -> arg.getName().isPresent());
            if (anyNamedArgs && !allNamedArgs) {
                throw new SemanticException("Mixing named and positional arguments is not allowed, %s", stmt);
            }

            // record procedure argument names and their positions
            Map<String, NamedArgument> procedureArgNameToArgument = new HashMap<>();
            for (int index = 0; index < procedureArgs.size(); ++index) {
                procedureArgNameToArgument.put(procedureArgs.get(index).getName(), procedure.getArguments().get(index));
            }

            // convert call arguments to named arguments
            LinkedHashMap<String, ProcedureArgument> callNamedArgs = new LinkedHashMap<>();
            for (int index = 0; index < callArgs.size(); ++index) {
                ProcedureArgument procedureArgument = callArgs.get(index);
                if (procedureArgument.getName().isPresent()) {
                    String name = procedureArgument.getName().get();
                    NamedArgument argument = procedureArgNameToArgument.get(name);
                    if (argument == null) {
                        throw new SemanticException("Unknown argument name: " + name);
                    }
                    if (callNamedArgs.put(name, callArgs.get(index)) != null) {
                        throw new SemanticException("Duplicate argument name: " + name);
                    }
                } else if (index < procedureArgs.size()) {
                    callNamedArgs.put(procedureArgs.get(index).getName(), procedureArgument);
                } else {
                    throw new SemanticException("Too many arguments provided, expected at most %d, got %d",
                            procedureArgs.size(), callArgs.size());
                }
            }

            // check if all required arguments are provided
            procedure.getArguments().forEach(arg -> {
                if (arg.isRequired() && !callNamedArgs.containsKey(arg.getName())) {
                    throw new SemanticException("Missing required argument: " + arg.getName());
                }
            });

            // check arguments values
            Map<String, ConstantOperator> constantArgs = new HashMap<>();
            for (Map.Entry<String, ProcedureArgument> entry : callNamedArgs.entrySet()) {
                Expr callArgumentValue = entry.getValue().getValue();
                NamedArgument procedureArgument = procedureArgNameToArgument.get(entry.getKey());
                // check call argument is constant
                ScalarOperator result;
                try {
                    Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
                    ExpressionAnalyzer.analyzeExpression(callArgumentValue, new AnalyzeState(), scope, context);
                    ExpressionMapping expressionMapping = new ExpressionMapping(scope);
                    result = SqlToScalarOperatorTranslator.translate(callArgumentValue, expressionMapping,
                            new ColumnRefFactory());
                    if (result instanceof ConstantOperator resConstantOperator) {
                        // check if constant argument type is compatible with procedure argument type
                        if (resConstantOperator.castTo(procedureArgument.getType()).isEmpty()) {
                            throw new SemanticException("Argument '%s' has invalid type %s, expected %s",
                                    entry.getKey(), result.getType(), procedureArgument.getType());
                        }
                        constantArgs.put(entry.getKey(), resConstantOperator);
                    } else {
                        throw new SemanticException("Argument '%s' must be a constant, got %s",
                                entry.getKey(), result);
                    }
                } catch (Exception e) {
                    throw new SemanticException("Failed to resolve call procedure args: %s. msg: %s, " +
                            "expected const argument", callArgumentValue, e.getMessage());
                }
            }
            stmt.setAnalyzedArguments(constantArgs);
        }

        private QualifiedName normalizeQualifiedName(QualifiedName qualifiedName, ConnectContext context) {
            if (qualifiedName == null || qualifiedName.getParts() == null) {
                throw new SemanticException("Procedure name cannot be null");
            }
            if (qualifiedName.getParts().isEmpty()) {
                throw new SemanticException("Procedure name cannot be empty");
            }

            String catalogName;
            String dbName = DEFAULT_DB_NAME;
            String procedureName;
            if (qualifiedName.getParts().size() == 1) {
                catalogName = context.getCurrentCatalog();
                procedureName = qualifiedName.getParts().get(0);
            } else if (qualifiedName.getParts().size() == 2) {
                catalogName = context.getCurrentCatalog();
                dbName = qualifiedName.getParts().get(0);
                procedureName = qualifiedName.getParts().get(1);
            } else if (qualifiedName.getParts().size() == 3) {
                catalogName = qualifiedName.getParts().get(0);
                dbName = qualifiedName.getParts().get(1);
                procedureName = qualifiedName.getParts().get(2);
            } else {
                throw new SemanticException("Invalid procedure name format: " + qualifiedName);
            }
            return QualifiedName.of(List.of(catalogName, dbName, procedureName));
        }
    }
}
