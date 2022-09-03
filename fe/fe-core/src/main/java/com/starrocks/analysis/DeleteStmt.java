// This file is made available under Elastic License 2.0.
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

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.analysis.CompoundPredicate.Operator;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryStatement;

import java.util.List;

public class DeleteStmt extends DmlStmt {
    private final TableName tblName;
    private final PartitionNames partitionNames;
    private final Expr wherePredicate;

    // fields for new planer, primary key table
    private Table table;
    private QueryStatement queryStatement;

    // fields for old planer, non-primary key table
    private final List<Predicate> deleteConditions;
    // Each deleteStmt corresponds to a DeleteJob.
    // The JobID is generated here for easy correlation when cancel Delete
    private long jobId = -1;

    public DeleteStmt(TableName tableName, PartitionNames partitionNames, Expr wherePredicate) {
        this.tblName = tableName;
        this.partitionNames = partitionNames;
        this.wherePredicate = wherePredicate;
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

    public List<String> getPartitionNames() {
        return partitionNames == null ? Lists.newArrayList() : partitionNames.getPartitionNames();
    }

    public List<Predicate> getDeleteConditions() {
        return deleteConditions;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (tblName == null) {
            throw new AnalysisException("Table is not set");
        }

        tblName.analyze(analyzer);

        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support deleting temp partitions");
            }
        }

        if (wherePredicate == null) {
            throw new AnalysisException("Where clause is not set");
        }

        // analyze predicate
        analyzePredicate(wherePredicate);
    }

    private void analyzePredicate(Expr predicate) throws AnalysisException {
        if (predicate instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;
            Expr leftExpr = binaryPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of binary predicate should be column name");
            }
            Expr rightExpr = binaryPredicate.getChild(1);
            if (!(rightExpr instanceof LiteralExpr)) {
                throw new AnalysisException("Right expr of binary predicate should be value");
            }
            deleteConditions.add(binaryPredicate);
        } else if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            if (compoundPredicate.getOp() != Operator.AND) {
                throw new AnalysisException("Compound predicate's op should be AND");
            }

            analyzePredicate(compoundPredicate.getChild(0));
            analyzePredicate(compoundPredicate.getChild(1));
        } else if (predicate instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) predicate;
            Expr leftExpr = isNullPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of is_null predicate should be column name");
            }
            deleteConditions.add(isNullPredicate);
        } else if (predicate instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) predicate;
            Expr leftExpr = inPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of binary predicate should be column name");
            }
            int inElementNum = inPredicate.getInElementNum();
            int maxAllowedInElementNumOfDelete = Config.max_allowed_in_element_num_of_delete;
            if (inElementNum > maxAllowedInElementNumOfDelete) {
                throw new AnalysisException("Element num of predicate should not be more than " +
                        maxAllowedInElementNumOfDelete);
            }
            for (int i = 1; i <= inElementNum; i++) {
                Expr expr = inPredicate.getChild(i);
                if (!(expr instanceof LiteralExpr)) {
                    throw new AnalysisException("Child of in predicate should be value");
                }
            }
            deleteConditions.add(inPredicate);
        } else {
            throw new AnalysisException("Where clause only supports compound predicate, binary predicate, " +
                    "is_null predicate and in predicate");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tblName.toSql());
        if (partitionNames != null) {
            sb.append(" PARTITION (");
            sb.append(Joiner.on(", ").join(partitionNames.getPartitionNames()));
            sb.append(")");
        }
        sb.append(" WHERE ").append(wherePredicate.toSql());
        return sb.toString();
    }

    public boolean supportNewPlanner() {
        // table must present if analyzed by new analyzer
        return table != null;
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
