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

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateTemporaryTableStmt extends CreateTableStmt {
    // the session's id associated with this temporary table
    private UUID sessionId = null;

    public CreateTemporaryTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           List<IndexDef> indexDefs,
                           String engineName,
                           String charsetName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> rollupAlterClauseList, List<String> sortKeys,
                           NodePosition pos) {
        super(ifNotExists, isExternal, tableName, columnDefinitions, indexDefs, engineName, charsetName, keysDesc,
                partitionDesc, distributionDesc, properties, extProperties, comment, rollupAlterClauseList, sortKeys, pos);
    }

    public CreateTemporaryTableStmt(boolean ifNotExists,
                                    boolean isExternal,
                                    TableName tableName,
                                    List<ColumnDef> columnDefinitions,
                                    List<IndexDef> indexDefs,
                                    String engineName,
                                    String charsetName,
                                    KeysDesc keysDesc,
                                    PartitionDesc partitionDesc,
                                    DistributionDesc distributionDesc,
                                    Map<String, String> properties,
                                    Map<String, String> extProperties,
                                    String comment, List<AlterClause> rollupAlterClauseList, List<String> sortKeys) {
        super(ifNotExists, isExternal, tableName, columnDefinitions, indexDefs, engineName, charsetName, keysDesc,
                partitionDesc, distributionDesc, properties, extProperties, comment, rollupAlterClauseList,
                sortKeys, NodePosition.ZERO);
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTemporaryTableStatement(this, context);
    }
}
