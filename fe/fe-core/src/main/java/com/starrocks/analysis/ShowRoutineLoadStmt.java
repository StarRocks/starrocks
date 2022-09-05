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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.load.routineload.RoutineLoadFunctionalExprProvider;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;
import java.util.stream.Collectors;

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
                    .add("OtherMsg")
                    .build();

    private final LabelName labelName;
    private boolean includeHistory = false;
    private RoutineLoadFunctionalExprProvider functionalExprProvider;
    private Expr whereClause;
    private List<OrderByElement> orderElements;
    private LimitElement limitElement;

    public ShowRoutineLoadStmt(LabelName labelName, boolean includeHistory) {
        this.labelName = labelName;
        this.includeHistory = includeHistory;
    }

    public ShowRoutineLoadStmt(LabelName labelName, boolean includeHistory, Expr expr,
                               List<OrderByElement> orderElements, LimitElement limitElement) {
        this(labelName, includeHistory);
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

    @Deprecated
    public RoutineLoadFunctionalExprProvider getFunctionalExprProvider(){
        if (null == functionalExprProvider) {
            functionalExprProvider = new RoutineLoadFunctionalExprProvider();
        }
        return functionalExprProvider;
    }

    @Deprecated
    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkLabelName(analyzer);
        getFunctionalExprProvider().analyze(analyzer, whereClause, orderElements, limitElement);
    }

    @Deprecated
    private void checkLabelName(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(labelName.getDbName())) {
            String dbName = analyzer.getContext().getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            labelName.setDbName(dbName);
        }
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }


    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW");
        if (includeHistory) {
            sb.append(" ALL");
        }
        sb.append(" ROUTINE LOAD");
        if (!Strings.isNullOrEmpty(labelName.getLabelName())) {
            sb.append(" FOR ").append(labelName.toString());
        } else {
            sb.append(" FROM ").append(labelName.getDbName());
        }
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }
        if (orderElements != null) {
            sb.append(" ORDER BY ");
            String order = orderElements.stream().map(e -> e.toSql()).collect(Collectors.joining(","));
            sb.append(order);
        }
        if (limitElement !=null) {
            sb.append(limitElement.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
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

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
