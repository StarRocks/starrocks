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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.load.routineload.RoutineLoadFunctionalExprProvider;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/*
  Show routine load progress by routine load name

  syntax:
      SHOW [ALL] ROUTINE LOAD [database.][name]

      without ALL: only show job which is not final
      with ALL: show all of job include history job

      without name: show all of routine load job in database with different name
      with name: show all of job named ${name} in database

      without on db: show all of job in connection db
         if user does not choose db before, return error
      with on db: show all of job in ${db}

      example:
        show routine load named test in database1
        SHOW ROUTINE LOAD database1.test;

        show routine load in database1
        SHOW ROUTINE LOAD database1;

        show routine load in database1 include history
        use database1;
        SHOW ALL ROUTINE LOAD;

        show routine load in all of database
        please use show proc
 */
public class ShowRoutineLoadStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Id")
                    .add("Name")
                    .add("CreateTime")
                    .add("PauseTime")
                    .add("EndTime")
                    .add("DbName")
                    .add("TableName")
                    .add("State")
                    .add("DataSourceType")
                    .add("CurrentTaskNum")
                    .add("JobProperties")
                    .add("DataSourceProperties")
                    .add("CustomProperties")
                    .add("Statistic")
                    .add("Progress")
                    .add("ReasonOfStateChanged")
                    .add("ErrorLogUrls")
                    .add("TrackingSQL")
                    .add("OtherMsg")
                    .build();

    private final LabelName labelName;
    private boolean includeHistory = false;
    private RoutineLoadFunctionalExprProvider functionalExprProvider;
    private Expr whereClause;
    private List<OrderByElement> orderElements;
    private LimitElement limitElement;


    public ShowRoutineLoadStmt(LabelName labelName, boolean includeHistory) {
        this(labelName, includeHistory, null, null, null, NodePosition.ZERO);
    }

    public ShowRoutineLoadStmt(LabelName labelName, boolean includeHistory, Expr expr,
                               List<OrderByElement> orderElements, LimitElement limitElement) {
        this(labelName, includeHistory, expr, orderElements, limitElement, NodePosition.ZERO);
    }

    public ShowRoutineLoadStmt(LabelName labelName, boolean includeHistory, Expr expr,
                               List<OrderByElement> orderElements, LimitElement limitElement,
                               NodePosition pos) {
        super(pos);
        this.labelName = labelName;
        this.includeHistory = includeHistory;
        this.whereClause = expr;
        this.orderElements = orderElements;
        this.limitElement = limitElement;
    }

    public String getDbFullName() {
        return labelName.getDbName();
    }

    public String getName() {
        return labelName.getLabelName();
    }

    public boolean isIncludeHistory() {
        return includeHistory;
    }

    public RoutineLoadFunctionalExprProvider getFunctionalExprProvider(ConnectContext context) throws AnalysisException {
        if (null == functionalExprProvider) {
            functionalExprProvider = new RoutineLoadFunctionalExprProvider();
        }
        functionalExprProvider.analyze(context, whereClause, orderElements, limitElement);
        return functionalExprProvider;
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
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
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    public void setDb(String db) {
        this.labelName.setDbName(db);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRoutineLoadStatement(this, context);
    }
}
