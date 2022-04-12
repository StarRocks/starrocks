// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.RefreshType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.PartitionExpDesc;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;

import java.util.List;

// todo PrivilegeChecker add permission
public class MaterializedViewAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(statement, session);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, ConnectContext context) {
            // todo check exists in catalog
            RefreshSchemeDesc refreshSchemeDesc = statement.getRefreshSchemeDesc();
            if (refreshSchemeDesc.getType() != RefreshType.ASYNC) {
                throw new SemanticException("Materialized view refresh type only support async");
            }
            // check partition key in query select statement, check partition key in used by partition in base tables
            QueryStatement queryStatement = statement.getQueryStatement();
            //get table name and column name first
            PartitionExpDesc partitionExpDesc = (PartitionExpDesc) statement.getPartitionDesc();
            FunctionName functionName = partitionExpDesc.getFunctionName();
            if(functionName != null && !functionName.getFunction().equals("date_format")) {
                throw new SemanticException("Materialized view partition function name" + functionName.getFunction() + "is not support");
            }
            // analyze query statement
            new QueryAnalyzer(context).analyze(queryStatement);
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
                throw new SemanticException("Materialized view query statement only support select");
            }
            // todo check relationField.relationAlias in scope can filter not alias column
            SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());

            Relation relation = selectRelation.getRelation();
            if (!(relation instanceof TableRelation) && !(relation instanceof JoinRelation)) {
                throw new SemanticException("Materialized view query statement must select from table");
            }
            // get table name and column name in metadata, compare with partition exp desc
            // todo some expression parse to mv item
            List<Expr> outputExpression = selectRelation.getOutputExpression();
            // check partition

            // check queryStatement and refresh

            // check properties

            return null;
        }


    }


}
