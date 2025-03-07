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
package com.starrocks.sql.common;

import com.google.common.base.Strings;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.ParseNode;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.ResourceDesc;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;

import java.util.List;
import java.util.Map;

/**
 * Responsible for determining whether the corresponding statement
 * needs to encrypt sensitive information in the audit log
 */
public class AuditEncryptionChecker implements AstVisitor<Boolean, Void> {
    private static final AuditEncryptionChecker INSTANCE = new AuditEncryptionChecker();

    private AuditEncryptionChecker() {
    }

    public static AuditEncryptionChecker getInstance() {
        return INSTANCE;
    }

    public static boolean needEncrypt(StatementBase statement) {
        if (statement == null) {
            return false;
        }
        return getInstance().visit(statement);
    }

    @Override
    public Boolean visitNode(ParseNode node, Void context) {
        return false;
    }

    @Override
    public Boolean visitStatement(StatementBase statement, Void context) {
        return false;
    }

    @Override
    public Boolean visitQueryStatement(QueryStatement statement, Void context) {
        QueryRelation queryRelation = statement.getQueryRelation();
        return visit(queryRelation);
    }

    @Override
    public Boolean visitInsertStatement(InsertStmt statement, Void context) {
        boolean tableFunctionAsTargetTable = statement.useTableFunctionAsTargetTable();
        if (tableFunctionAsTargetTable) {
            return true;
        }

        QueryStatement queryStatement = statement.getQueryStatement();
        return queryStatement != null && visitQueryStatement(queryStatement, context);
    }

    @Override
    public Boolean visitSetStatement(SetStmt statement, Void context) {
        List<SetListItem> setListItems = statement.getSetListItems();

        for (SetListItem var : setListItems) {
            if (var instanceof SetPassVar) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visitCreateTableStatement(CreateTableStmt statement, Void context) {
        String engineName = statement.getEngineName();
        return !Strings.isNullOrEmpty(engineName) && !statement.isOlapEngine();
    }

    @Override
    public Boolean visitCreateStorageVolumeStatement(CreateStorageVolumeStmt statement, Void context) {
        Map<String, String> properties = statement.getProperties();

        if (properties.containsKey(CloudConfigurationConstants.AWS_S3_ACCESS_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AWS_S3_SECRET_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN)) {
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, Void context) {
        Map<String, String> properties = statement.getProperties();

        if (properties.containsKey(CloudConfigurationConstants.AWS_S3_ACCESS_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AWS_S3_SECRET_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN)) {
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, Void context) {
        return true;
    }

    @Override
    public Boolean visitCreateCatalogStatement(CreateCatalogStmt statement, Void context) {
        return true;
    }

    @Override
    public Boolean visitCreateResourceStatement(CreateResourceStmt statement, Void context) {
        return true;
    }

    @Override
    public Boolean visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, Void context) {
        return true;
    }

    @Override
    public Boolean visitLoadStatement(LoadStmt statement, Void context) {
        BrokerDesc brokerDesc = statement.getBrokerDesc();
        ResourceDesc resourceDesc = statement.getResourceDesc();

        if (brokerDesc != null || resourceDesc != null) {
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitExportStatement(ExportStmt statement, Void context) {
        BrokerDesc brokerDesc = statement.getBrokerDesc();
        return brokerDesc != null;
    }

    @Override
    public Boolean visitCreatePipeStatement(CreatePipeStmt statement, Void context) {
        return visitInsertStatement(statement.getInsertStmt(), context);
    }

    @Override
    public Boolean visitRelation(Relation relation, Void context) {
        return false;
    }

    @Override
    public Boolean visitSelect(SelectRelation relation, Void context) {
        return visit(relation.getRelation());
    }

    @Override
    public Boolean visitFileTableFunction(FileTableFunctionRelation relation, Void context) {
        return true;
    }

    @Override
    public Boolean visitSetOp(SetOperationRelation relation, Void context) {
        for (QueryRelation queryRelation : relation.getRelations()) {
            if (visit(queryRelation)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visitSubqueryRelation(SubqueryRelation relation, Void context) {
        QueryStatement queryStatement = relation.getQueryStatement();
        return visit(queryStatement);
    }
}
