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

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.Arrays;
import java.util.List;

/*
    show all of task belong to job
    SHOW ROUTINE LOAD TASK FROM DB where expr;

    where expr: JobName=xxx
 */
public class ShowRoutineLoadTaskStmt extends ShowStmt {
    private static final List<String> SUPPORT_COLUMN = Arrays.asList("jobname");
    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("TaskId")
                    .add("TxnId")
                    .add("TxnStatus")
                    .add("JobId")
                    .add("CreateTime")
                    .add("LastScheduledTime")
                    .add("ExecuteStartTime")
                    .add("Timeout")
                    .add("BeId")
                    .add("DataSourceProperties")
                    .add("Message")
                    .build();

    private final Expr jobNameExpr;

    private String jobName;
    private String dbFullName;

    public ShowRoutineLoadTaskStmt(String dbName, Expr jobNameExpr) {
        this(dbName, jobNameExpr, NodePosition.ZERO);
    }

    public ShowRoutineLoadTaskStmt(String dbName, Expr jobNameExpr, NodePosition pos) {
        super(pos);
        this.dbFullName = dbName;
        this.jobNameExpr = jobNameExpr;
    }

    public String getJobName() {
        return jobName;
    }

    public String getDbFullName() {
        return dbFullName;
    }

    public void setDbFullName(String dbFullName) {
        this.dbFullName = dbFullName;
    }

    public void checkJobNameExpr() throws AnalysisException {
        if (jobNameExpr == null) {
            throw new AnalysisException("please designate a jobName in where expr such as JobName=\"ILoveStarRocks\"");
        }

        boolean valid = true;
        CHECK:
        {
            // check predicate
            if (!(jobNameExpr instanceof BinaryPredicate)) {
                valid = false;
                break CHECK;
            }
            BinaryPredicate binaryPredicate = (BinaryPredicate) jobNameExpr;
            if (binaryPredicate.getOp() != BinaryType.EQ) {
                valid = false;
                break CHECK;
            }

            // check child(0)
            if (!(binaryPredicate.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
            if (!SUPPORT_COLUMN.stream().anyMatch(entity -> entity.equals(slotRef.getColumnName().toLowerCase()))) {
                valid = false;
                break CHECK;
            }

            // check child(1)
            if (!(binaryPredicate.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break CHECK;
            }
            StringLiteral stringLiteral = (StringLiteral) binaryPredicate.getChild(1);
            jobName = stringLiteral.getValue();
        }

        if (!valid) {
            throw new AnalysisException(
                    "show routine load job only support one equal expr which is sames like JobName=\"ILoveStarRocks\"");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRoutineLoadTaskStatement(this, context);
    }
}
