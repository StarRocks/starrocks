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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DeleteStmt.java

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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class DeleteStmt extends DmlStmt {
    private final TableName tblName;
    private final PartitionNames partitionNames;
    private final List<Relation> usingRelations;
    private final Expr wherePredicate;
    private final List<CTERelation> commonTableExpressions;

    // fields for new planer, primary key table
    private Table table;
    private QueryStatement queryStatement;

    // fields for old planer, non-primary key table
    private List<Predicate> deleteConditions;
    // Each deleteStmt corresponds to a DeleteJob.
    // The JobID is generated here for easy correlation when cancel Delete
    private long jobId = -1;

    public DeleteStmt(TableName tableName, PartitionNames partitionNames, Expr wherePredicate) {
        this(tableName, partitionNames, null, wherePredicate, null, NodePosition.ZERO);
    }

    public DeleteStmt(TableName tableName, PartitionNames partitionNames, List<Relation> usingRelations,
                      Expr wherePredicate, List<CTERelation> commonTableExpressions) {
        this(tableName, partitionNames, usingRelations, wherePredicate, commonTableExpressions, NodePosition.ZERO);
    }

    public DeleteStmt(TableName tableName, PartitionNames partitionNames, List<Relation> usingRelations,
                      Expr wherePredicate, List<CTERelation> commonTableExpressions, NodePosition pos) {
        super(pos);
        this.tblName = tableName;
        this.partitionNames = partitionNames;
        this.usingRelations = usingRelations;
        this.wherePredicate = wherePredicate;
        this.commonTableExpressions = commonTableExpressions;
        this.deleteConditions = Lists.newLinkedList();
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public TableName getTableName() {
        return tblName;
    }

    public Expr getWherePredicate() {
        return wherePredicate;
    }

    public List<CTERelation> getCommonTableExpressions() {
        return commonTableExpressions;
    }

    public List<String> getPartitionNamesList() {
        return partitionNames == null ? Lists.newArrayList() : partitionNames.getPartitionNames();
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public List<Relation> getUsingRelations() {
        return usingRelations;
    }

    public List<Predicate> getDeleteConditions() {
        return deleteConditions;
    }

    public void setDeleteConditions(List<Predicate> deleteConditions) {
        this.deleteConditions = deleteConditions;
    }

    public boolean shouldHandledByDeleteHandler() {
        // table must present if analyzed and is a delele for primary key table, so it is executed by new
        // planner&execution engine, otherwise(non-pk table) it is handled by DeleteHandler
        return table == null;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDeleteStatement(this, context);
    }
}
